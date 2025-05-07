import requests
import pandas as pd
import concurrent.futures
import threading
import itertools
import time
import logging
import os
from telegram import Bot

# --- Proxy helper functions ---

def fetch_proxies_from_url(url: str, default_scheme: str = "http") -> list:
    proxies = []
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        lines = response.text.strip().splitlines()
        for line in lines:
            proxy = line.strip()
            if not proxy:
                continue
            if "://" in proxy:
                proxies.append(proxy)
            else:
                proxies.append(f"{default_scheme}://{proxy}")
        logging.info("Fetched %d proxies from %s", len(proxies), url)
    except Exception as e:
        logging.error("Error fetching proxies from URL %s: %s", url, e)
    return proxies

def test_proxy(proxy: str) -> bool:
    test_url = "https://api.binance.com/api/v3/time"
    try:
        response = requests.get(test_url, proxies={"http": proxy, "https": proxy}, timeout=10)
        return 200 <= response.status_code < 300
    except Exception as e:
        logging.debug("Proxy %s failed connectivity test: %s", proxy, e)
        return False

def test_proxy_speed(proxy: str) -> float:
    test_url = "https://api.binance.com/api/v3/time"
    try:
        start_time = time.time()
        response = requests.get(test_url, proxies={"http": proxy, "https": proxy}, timeout=10)
        response.raise_for_status()
        end_time = time.time()
        return end_time - start_time
    except Exception as e:
        logging.warning("Proxy %s failed speed test: %s", proxy, e)
        return float("inf")

def test_proxies_concurrently(proxies: list, max_workers: int = 50, max_working: int = 20) -> list:
    working = []
    tested = 0
    dead = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(test_proxy, proxy): proxy for proxy in proxies}
        try:
            for future in concurrent.futures.as_completed(futures):
                tested += 1
                proxy = futures[future]
                if future.result():
                    working.append(proxy)
                    if len(working) % 2 == 0:
                        logging.info("Proxy check: Tested %d | Working: %d | Dead: %d", tested, len(working), tested - len(working))
                else:
                    dead += 1
                if len(working) >= max_working:
                    break
        finally:
            if len(working) >= max_working:
                for f in futures:
                    f.cancel()
    logging.info("Found %d working proxies (tested %d)", len(working), tested)
    return working[:max_working]

def rank_proxies_by_speed(proxies: list) -> list:
    ranked = []
    for proxy in proxies:
        speed = test_proxy_speed(proxy)
        ranked.append((proxy, speed))
    ranked.sort(key=lambda x: x[1])
    return ranked

# --- ProxyPool Class ---

class ProxyPool:
    def __init__(self, proxies: list = None, max_pool_size: int = 25, proxy_check_interval: int = 600, max_failures: int = 3):
        self.lock = threading.Lock()
        self.max_pool_size = max_pool_size
        self.proxy_check_interval = proxy_check_interval
        self.max_failures = max_failures

        self.proxies = proxies.copy() if proxies else []
        self.proxy_failures = {}  # proxy -> failure count
        self.failed_proxies = set()

        self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None

        self._stop_event = threading.Event()
        self.start_checker()
        self.start_fastest_proxy_checker()

    def get_next_proxy(self):
        with self.lock:
            if not self.proxies:
                logging.warning("Proxy pool empty when requesting next proxy.")
                return None
            for _ in range(len(self.proxies)):
                proxy = next(self.proxy_cycle)
                if proxy not in self.failed_proxies:
                    return proxy
            logging.warning("All proxies in pool are marked as failed.")
            return None

    def mark_proxy_failure(self, proxy):
        with self.lock:
            count = self.proxy_failures.get(proxy, 0) + 1
            self.proxy_failures[proxy] = count
            logging.warning(f"Proxy {proxy} failure count: {count}/{self.max_failures}")
            if count >= self.max_failures:
                self.failed_proxies.add(proxy)
                if proxy in self.proxies:
                    self.proxies.remove(proxy)
                    logging.warning(f"Proxy {proxy} removed from pool due to repeated failures.")
                self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None

    def reset_proxy_failures(self, proxy):
        with self.lock:
            if proxy in self.proxy_failures:
                self.proxy_failures[proxy] = 0
            if proxy in self.failed_proxies:
                self.failed_proxies.remove(proxy)
                if proxy not in self.proxies:
                    self.proxies.append(proxy)
                    self.proxy_cycle = itertools.cycle(self.proxies)

    def update_fastest_proxy(self):
        with self.lock:
            if not self.proxies:
                self.fastest_proxy = None
                logging.warning("No proxies available to update fastest proxy.")
                return
            ranked = rank_proxies_by_speed(self.proxies)
            if ranked:
                self.fastest_proxy = ranked[0][0]
                logging.info(f"Fastest proxy updated: {self.fastest_proxy}")
            else:
                self.fastest_proxy = None
                logging.warning("Could not determine fastest proxy.")

    def populate_to_max(self):
        with self.lock:
            needed = self.max_pool_size - len(self.proxies)
            if needed <= 0:
                return
        new_proxies = self.get_new_proxies(needed)
        with self.lock:
            self.proxies.extend(new_proxies)
            self.proxy_cycle = itertools.cycle(self.proxies)
            logging.info(f"Proxy pool populated to max size: {len(self.proxies)}/{self.max_pool_size}")

    def get_new_proxies(self, count: int) -> list:
        backup_url = "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt"
        new_proxies = fetch_proxies_from_url(backup_url)
        working = test_proxies_concurrently(new_proxies, max_working=count)
        return working

    def populate_from_url(self, url: str, default_scheme: str = "http"):
        new_proxies = fetch_proxies_from_url(url, default_scheme)
        working = test_proxies_concurrently(new_proxies, max_working=self.max_pool_size)
        with self.lock:
            self.proxies = working[:self.max_pool_size]
            self.proxy_failures.clear()
            self.failed_proxies.clear()
            self.proxy_cycle = itertools.cycle(self.proxies)
            logging.info(f"Proxy pool filled with {len(self.proxies)} proxies from URL.")

    def check_proxies(self):
        with self.lock:
            initial_count = len(self.proxies)
            alive_proxies = [p for p in self.proxies if test_proxy(p)]
            removed = initial_count - len(alive_proxies)
            self.proxies = alive_proxies
            self.proxy_failures = {p: self.proxy_failures.get(p, 0) for p in self.proxies}
            self.failed_proxies = set()
            self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
        if removed > 0:
            logging.info(f"Removed {removed} dead proxies during check.")
            self.populate_to_max()

    def fastest_proxy_checker_loop(self):
        while not self._stop_event.is_set():
            self.update_fastest_proxy()
            self._stop_event.wait(3600)  # sleep 1 hour or until stopped

    def proxy_checker_loop(self):
        while not self._stop_event.is_set():
            logging.info("Running proxy pool health check...")
            self.check_proxies()
            self._stop_event.wait(self.proxy_check_interval)

    def start_fastest_proxy_checker(self):
        thread = threading.Thread(target=self.fastest_proxy_checker_loop, daemon=True)
        thread.start()

    def start_checker(self):
        thread = threading.Thread(target=self.proxy_checker_loop, daemon=True)
        thread.start()

    def stop(self):
        self._stop_event.set()

    def has_proxies(self) -> bool:
        with self.lock:
            return bool(self.proxies)

# --- Binance Client with proxy support ---

class BinanceClient:
    def __init__(self, proxy_pool: ProxyPool):
        self.proxy_pool = proxy_pool

    def _get_proxy_dict(self):
        proxy = self.proxy_pool.get_next_proxy()
        if proxy is None:
            raise RuntimeError("No working proxies available")
        return {"http": proxy, "https": proxy}

    def get_spot_symbols(self):
        url = 'https://api.binance.com/api/v3/exchangeInfo'
        proxies = self._get_proxy_dict()
        resp = requests.get(url, proxies=proxies, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        # Spot symbols have 'SPOT' in 'permissions' list
        spot_symbols = [s['symbol'] for s in data['symbols'] 
                        if 'SPOT' in s.get('permissions', []) and s['status'] == 'TRADING']
        return spot_symbols

    def get_perp_symbols(self):
        url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        proxies = self._get_proxy_dict()
        resp = requests.get(url, proxies=proxies, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        perp_symbols = [s['symbol'] for s in data['symbols'] 
                        if s.get('contractType') == 'PERPETUAL' and s['status'] == 'TRADING']
        return perp_symbols

    def fetch_ohlcv(self, symbol, interval, limit=100):
        url = 'https://api.binance.com/api/v3/klines'
        params = {'symbol': symbol, 'interval': interval, 'limit': limit}
        proxies = self._get_proxy_dict()
        resp = requests.get(url, params=params, proxies=proxies, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data, columns=[
            'openTime','open','high','low','close','volume',
            'closeTime','quoteAssetVolume','numberOfTrades',
            'takerBuyBaseAssetVolume','takerBuyQuoteAssetVolume','ignore'
        ])
        df[['high','low','close']] = df[['high','low','close']].astype(float)
        return df

    def fetch_24h_changes(self):
        url = 'https://api.binance.com/api/v3/ticker/24hr'
        proxies = self._get_proxy_dict()
        resp = requests.get(url, proxies=proxies, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return {item['symbol']: float(item['priceChangePercent']) for item in data}

# --- EMA distance calculation ---

def calculate_pct_distance(df):
    df['EMA34'] = df['close'].ewm(span=34, adjust=False).mean()
    last = df.iloc[-1]
    return (last['close'] - last['EMA34']) / last['EMA34'] * 100

# --- Telegram Reporter ---

class TelegramReporter:
    def __init__(self, token, chat_id):
        self.bot = Bot(token=token)
        self.chat_id = chat_id

    def format_section(self, timeframe, position, df):
        header = f"*{timeframe} - {position} EMA34*"
        lines = [header]
        for _, row in df.iterrows():
            lines.append(f"`{row['Symbol']:10} {row['Distance (%)']:>7}%  {row['Daily Movement (%)']:>7}`")
        return "\n".join(lines)

    def send_report(self, message):
        self.bot.send_message(chat_id=self.chat_id, text=message, parse_mode='Markdown')

# --- Build top 40 above/below sections ---

def build_top_sections(df, daily_changes):
    df['daily'] = df['symbol'].map(daily_changes)
    df['Distance (%)'] = df['pct_distance'].map('{:.2f}'.format)
    df['Daily Movement (%)'] = df['daily'].map(lambda x: f"{x:.2f}%" if pd.notnull(x) else "N/A")

    above = df.sort_values('pct_distance', ascending=False).head(40)[['symbol', 'Distance (%)', 'Daily Movement (%)']]
    below = df.sort_values('pct_distance').head(40)[['symbol', 'Distance (%)', 'Daily Movement (%)']]

    above.columns = below.columns = ['Symbol', 'Distance (%)', 'Daily Movement (%)']
    return above, below

# --- Main scanning and reporting loop ---

def fetch_symbol_distance(client, symbol, tf):
    try:
        df = client.fetch_ohlcv(symbol, tf)
        pct_dist = calculate_pct_distance(df)
        return symbol, pct_dist
    except Exception as e:
        logging.debug(f"Failed fetching {symbol} {tf}: {e}")
        return None

def run_scan_and_report(binance_client, reporter, proxy_pool):
    perp_symbols = set(binance_client.get_perp_symbols())
    spot_symbols = set(binance_client.get_spot_symbols())
    symbols_to_process = list(spot_symbols - perp_symbols)
    logging.info(f"Processing {len(symbols_to_process)} spot symbols excluding perps")

    daily_changes = binance_client.fetch_24h_changes()

    for tf in ['5m', '15m', '30m', '1h', '4h', '1d', '1w']:
        logging.info(f"Scanning timeframe {tf}")
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_symbol_distance, binance_client, sym, tf): sym for sym in symbols_to_process}
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res is not None:
                    results.append(res)

        df = pd.DataFrame(results, columns=['symbol', 'pct_distance'])

        above, below = build_top_sections(df, daily_changes)

        msg_above = reporter.format_section(tf, "Above", above)
        msg_below = reporter.format_section(tf, "Below", below)

        full_msg = f"{msg_above}\n\n{msg_below}\n\n" + ("-"*30)
        reporter.send_report(full_msg)
        time.sleep(2)  # avoid Telegram flood limits

# --- Entrypoint ---

def main():
    logging.basicConfig(level=logging.INFO)

    # Load proxies from URL or local file
    proxy_url = os.getenv("PROXY_LIST_URL")
    if not proxy_url:
        logging.error("Set PROXY_LIST_URL environment variable with your proxy list URL")
        return

    proxies = fetch_proxies_from_url(proxy_url)
    proxy_pool = ProxyPool(proxies=proxies)

    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables")
        return

    binance_client = BinanceClient(proxy_pool)
    reporter = TelegramReporter(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

    run_scan_and_report(binance_client, reporter, proxy_pool)

if __name__ == "__main__":
    main()
