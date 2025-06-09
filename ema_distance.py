import requests
import pandas as pd
import concurrent.futures
import threading
import itertools
import time
import logging
import os
import asyncio
from telegram import Bot
import random
import tqdm
import re

# List of symbols to ignore in all scanning
IGNORED_SYMBOLS = {
    "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
    "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT",
    "WBTCUSDT"
}

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

def test_proxy(proxy: str, timeout=5) -> bool:
    test_url = "https://api.binance.com/api/v3/time"
    try:
        response = requests.get(test_url, proxies={"http": proxy, "https": proxy}, timeout=timeout)
        return 200 <= response.status_code < 300
    except requests.exceptions.Timeout:
        logging.debug(f"Proxy {proxy} timed out during connectivity test.")
        return False
    except requests.exceptions.ProxyError:
        logging.debug(f"Proxy {proxy} proxy error during connectivity test.")
        return False
    except Exception as e:
        logging.debug(f"Proxy {proxy} failed connectivity test: {e}")
        return False

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
                    if len(working) % 5 == 0:
                        logging.info(f"Proxy check: Tested {tested} | Working: {len(working)} | Dead: {tested - len(working)}")
                else:
                    dead += 1
                if len(working) >= max_working:
                    # Early stop when enough working proxies found
                    break
        finally:
            if len(working) >= max_working:
                for f in futures:
                    f.cancel()
    logging.info(f"Found {len(working)} working proxies (tested {tested})")
    return working[:max_working]

class ProxyPool:
    def __init__(self, max_pool_size: int = 25, proxy_check_interval: int = 600, max_failures: int = 3):
        self.lock = threading.Lock()
        self.max_pool_size = max_pool_size
        self.proxy_check_interval = proxy_check_interval
        self.max_failures = max_failures

        self.proxies = []
        self.proxy_failures = {}  # proxy -> failure count
        self.failed_proxies = set()

        self.proxy_cycle = None
        self.fastest_proxy = None

        self._stop_event = threading.Event()

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

            max_workers = min(50, len(self.proxies))
            fastest = None
            fastest_time = float('inf')

            def test_and_return(proxy):
                try:
                    start = time.time()
                    resp = requests.get("https://api.binance.com/api/v3/time",
                                        proxies={"http": proxy, "https": proxy},
                                        timeout=5)
                    resp.raise_for_status()
                    return proxy, time.time() - start
                except:
                    return proxy, float('inf')

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(test_and_return, p): p for p in self.proxies}
                for future in concurrent.futures.as_completed(futures):
                    proxy, speed = future.result()
                    if speed < fastest_time:
                        fastest = proxy
                        fastest_time = speed
                    if fastest_time < 0.5:  # early stop if very fast proxy found
                        break

            self.fastest_proxy = fastest
            if fastest:
                logging.info(f"Fastest proxy updated: {fastest} with speed {fastest_time:.2f}s")
            else:
                logging.warning("Could not determine fastest proxy.")

    def refresh_proxies(self):
        logging.info("Refreshing proxy pool because all proxies failed...")
        proxy_url = os.getenv("PROXY_LIST_URL")
        if proxy_url:
            self.populate_from_url(proxy_url)
        else:
            self.populate_to_max()
        # Optional delay to avoid hammering the proxy source
        time.sleep(5)

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
            self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
            logging.info(f"Proxy pool filled with {len(self.proxies)} proxies from URL.")

    def check_proxies(self):
        logging.info("Running concurrent proxy health check...")
        working = test_proxies_concurrently(self.proxies,
                                            max_workers=50,
                                            max_working=len(self.proxies))
        with self.lock:
            initial_count = len(self.proxies)
            self.proxies = working
            removed = initial_count - len(working)
            self.proxy_failures = {p: self.proxy_failures.get(p, 0) for p in self.proxies}
            self.failed_proxies = set()
            self.proxy_cycle = itertools.cycle(self.proxies) if self.proxies else None
        if removed > 0:
            logging.info(f"Removed {removed} dead proxies during check.")
            self.populate_to_max()

    def fastest_proxy_checker_loop(self):
        while not self._stop_event.is_set():
            self.update_fastest_proxy()
            self._stop_event.wait(3600)  # 1 hour

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
    def __init__(self, proxy_pool: ProxyPool, max_retries=5, retry_delay=2):
        self.proxy_pool = proxy_pool
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def _get_proxy_dict(self):
        proxy = self.proxy_pool.get_next_proxy()
        if proxy is None:
            raise RuntimeError("No working proxies available")
        return {"http": proxy, "https": proxy}

    def get_perp_symbols(self):
        logging.info("Fetching perpetual futures symbols (USDT pairs only)...")
        url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        for attempt in range(1, self.max_retries + 1):
            proxy = self.proxy_pool.get_next_proxy()
            if proxy is None:
                logging.error("No proxies available to fetch perp symbols")
                time.sleep(self.retry_delay)
                continue
            proxies = {"http": proxy, "https": proxy}
            try:
                resp = requests.get(url, proxies=proxies, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                symbols = [
                    s['symbol'] for s in data['symbols']
                    if s.get('contractType') == 'PERPETUAL' and
                       s['status'] == 'TRADING' and
                       s.get('quoteAsset') == 'USDT'
                ]
                if symbols:
                    logging.info(f"Fetched {len(symbols)} perp USDT symbols successfully.")
                    return symbols
                else:
                    logging.warning(f"Attempt {attempt}: No perp USDT symbols returned, retrying...")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt} failed with proxy {proxy}: {e}")
                self.proxy_pool.mark_proxy_failure(proxy)
            time.sleep(self.retry_delay * attempt + random.uniform(0, 1))
        logging.error("All retries failed to fetch perp symbols")
        return []

    def get_spot_symbols(self):
        logging.info("Fetching spot symbols (USDT pairs only)...")
        url = 'https://api.binance.com/api/v3/exchangeInfo'
        params = {'showPermissionSets': 'true'}
        for attempt in range(1, self.max_retries + 1):
            try:
                proxies = self._get_proxy_dict()
                resp = requests.get(url, params=params, proxies=proxies, timeout=10)
                logging.info(f"Spot symbols fetch attempt {attempt} status: {resp.status_code}")
                logging.debug(f"Spot symbols fetch attempt {attempt} response: {resp.text[:500]}")  # first 500 chars
                resp.raise_for_status()
                data = resp.json()

                spot_symbols = [
                    s['symbol']
                    for s in data['symbols']
                    if s['status'] == 'TRADING' and
                       s.get('quoteAsset') == 'USDT' and
                       any('SPOT' in perm for perm in itertools.chain.from_iterable(s.get('permissionSets', [])))
                ]

                if spot_symbols:
                    logging.info(f"Fetched {len(spot_symbols)} spot USDT symbols successfully.")
                    return spot_symbols
                else:
                    logging.warning(f"Attempt {attempt}: No spot USDT symbols returned, retrying...")
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed to fetch spot symbols: {e}")
            time.sleep(self.retry_delay * attempt + random.uniform(0, 1))
        logging.error("All retries failed to fetch spot symbols")
        return []

    def fetch_ohlcv(self, symbol, interval, limit=100, market="spot"):
        if market == "spot":
            url = 'https://api.binance.com/api/v3/klines'
        elif market == "perp":
            url = 'https://fapi.binance.com/fapi/v1/klines'
        else:
            raise ValueError(f"Unknown market type: {market}")

        params = {'symbol': symbol, 'interval': interval, 'limit': limit}
        attempt = 1
        max_proxy_refresh_attempts = 3
        proxy_refresh_attempts = 0
        while attempt <= self.max_retries:
            try:
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
            except RuntimeError as e:
                if "No working proxies available" in str(e):
                    proxy_refresh_attempts += 1
                    if proxy_refresh_attempts > max_proxy_refresh_attempts:
                        logging.error("Max proxy refresh attempts reached, aborting fetch_ohlcv")
                        raise
                    logging.warning("All proxies failed, refreshing proxy pool and retrying...")
                    self.proxy_pool.refresh_proxies()
                    continue  # Do not increment attempt, just retry
                else:
                    raise
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed fetching OHLCV for {symbol} ({market}) {interval}: {e}")
                self.proxy_pool.mark_proxy_failure(proxies.get('http'))
            time.sleep(self.retry_delay * attempt + random.uniform(0, 1))
            attempt += 1
        logging.error(f"All retries failed fetching OHLCV for {symbol} ({market}) {interval}")
        raise RuntimeError(f"Failed to fetch OHLCV for {symbol} ({market}) {interval}")

    def fetch_24h_changes(self):
        spot_url = 'https://api.binance.com/api/v3/ticker/24hr'
        futures_url = 'https://fapi.binance.com/fapi/v1/ticker/24hr'
        combined_changes = {}

        # Helper to fetch and parse data from a URL
        def fetch_changes(url):
            try:
                proxies = self._get_proxy_dict()
                resp = requests.get(url, proxies=proxies, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                return {item['symbol']: float(item['priceChangePercent']) for item in data}
            except Exception as e:
                logging.error(f"Failed to fetch 24h changes from {url}: {e}")
                return {}

        # Fetch spot changes
        spot_changes = fetch_changes(spot_url)
        combined_changes.update(spot_changes)

        # Fetch futures changes (overwrites spot if same symbol)
        futures_changes = fetch_changes(futures_url)
        combined_changes.update(futures_changes)

        return combined_changes


# --- EMA distance calculation ---

def calculate_pct_distance(df):
    df['EMA34'] = df['close'].ewm(span=34, adjust=False).mean()
    last = df.iloc[-1]
    return (last['close'] - last['EMA34']) / last['EMA34'] * 100


# --- Enhanced EMA analysis function with touches only ---

def calculate_enhanced_ema_analysis(df, touch_threshold=0.5, lookback_period=20, max_distance_below_ema=3):
    """
    Enhanced EMA analysis with relaxed trend filter (price can be slightly below EMA).
    """
    df = df.copy()
    df['EMA34'] = df['close'].ewm(span=34, adjust=False).mean()
    df['pct_distance'] = (df['close'] - df['EMA34']) / df['EMA34'] * 100

    recent_data = df.tail(lookback_period)

    touches = 0
    volume_spikes = 0

    avg_volume = recent_data['volume'].astype(float).mean()

    last_distance = recent_data.iloc[-1]['pct_distance']
    if last_distance < -max_distance_below_ema:
        return None  # Price too far below EMA, skip

    for i in range(1, len(recent_data)):
        current_dist = abs(recent_data.iloc[i]['pct_distance'])
        current_volume = float(recent_data.iloc[i]['volume'])

        if current_dist <= touch_threshold:
            touches += 1

        if current_volume > avg_volume * 1.5:
            volume_spikes += 1

    breakout_score = touches * 2 + volume_spikes * 2

    return {
        'touches': touches,
        'volume_spikes': volume_spikes,
        'breakout_score': breakout_score,
        'current_distance': last_distance
    }


# --- Multi-timeframe confluence check ---

# Function definition expects perp_symbols
def check_timeframe_confluence(symbol, binance_client, perp_symbols, timeframes=['4h', '1d']):
    confluence_score = 0
    for tf in timeframes:
        market = "perp" if symbol in perp_symbols else "spot"
        df = binance_client.fetch_ohlcv(symbol, tf, limit=50, market=market)
        analysis = calculate_enhanced_ema_analysis(df)
        if analysis and analysis['touches'] >= 2:
            confluence_score += 1
    return confluence_score

# --- MACD calculation ---

def calculate_macd(df):
    ema12 = df['close'].ewm(span=12).mean()
    ema26 = df['close'].ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    bullish = macd.iloc[-1] > signal.iloc[-1]
    return {'macd': macd.iloc[-1], 'signal': signal.iloc[-1], 'macd_bullish': bullish}


# --- Relative volume calculation ---

def calculate_relative_volume(df, lookback=20):
    recent_vol = df['volume'].astype(float).tail(lookback).mean()
    longer_vol = df['volume'].astype(float).tail(lookback * 3).mean()
    rel_vol = recent_vol / longer_vol if longer_vol > 0 else 1
    volume_surge = rel_vol > 1.5
    return {'relative_volume': rel_vol, 'volume_surge': volume_surge}


# --- Multiple EMA signals ---

def calculate_multiple_ema_signals(df):
    df['EMA21'] = df['close'].ewm(span=21).mean()
    df['EMA34'] = df['close'].ewm(span=34).mean()
    df['EMA55'] = df['close'].ewm(span=55).mean()
    last = df.iloc[-1]
    ema_alignment = (last['EMA21'] > last['EMA34'] > last['EMA55'])
    above_all_emas = (last['close'] > last['EMA21'] > last['EMA34'])
    return {'ema_alignment': ema_alignment, 'above_all_emas': above_all_emas}


# --- Consolidation detection ---

def detect_consolidation(df, lookback=20):
    recent = df.tail(lookback)
    price_range = recent['high'].max() - recent['low'].min()
    avg_price = recent['close'].mean()
    consolidation_ratio = price_range / avg_price if avg_price else 1
    is_consolidating = consolidation_ratio < 0.10
    breakout_potential = consolidation_ratio < 0.05
    return {'is_consolidating': is_consolidating, 'breakout_potential': breakout_potential}


# --- Comprehensive breakout scoring ---

def calculate_comprehensive_breakout_score(df, binance_client, symbol, perp_symbols):
    base = calculate_enhanced_ema_analysis(df)
    if base is None:
        return None

    macd_data = calculate_macd(df)
    vol_data = calculate_relative_volume(df)
    ema_data = calculate_multiple_ema_signals(df)
    consolidation_data = detect_consolidation(df)
    confluence = check_timeframe_confluence(symbol, binance_client, perp_symbols)  # Now passes perp_symbols

    score = base['breakout_score']
    if macd_data['macd_bullish']:
        score += 4
    if vol_data['volume_surge']:
        score += 5
    if ema_data['ema_alignment']:
        score += 3
    if consolidation_data['breakout_potential']:
        score += 4
    if confluence > 0:
        score += confluence * 2  # each timeframe confluence adds points

    confirmation_factors = sum([
        macd_data['macd_bullish'],
        vol_data['volume_surge'],
        ema_data['ema_alignment'],
        consolidation_data['breakout_potential'],
        confluence > 0
    ])

    return {
        **base,
        'comprehensive_score': score,
        'confirmation_factors': confirmation_factors
    }


# --- Telegram Reporter (async) ---

class TelegramReporter:
    def __init__(self, token, chat_id, channel_username):
        self.bot = Bot(token=token)
        self.chat_id = chat_id
        self.channel_username = channel_username

    def _escape_md_v2(self, text):
        escape_chars = r'_*[]()~`>#+-=|{}.!'
        return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

    def format_section(self, timeframe, position, df):
        header = f"*{self._escape_md_v2(timeframe)} • {self._escape_md_v2(position)} Line*"
        lines = [header, "```"]
        lines.append(f"{'Symbol':<12} {'Distance (%)':>12} {'Daily Move (%)':>14}")
        lines.append("-" * 40)
        for _, row in df.iterrows():
            symbol = str(row['Symbol'])
            dist = str(row['Distance (%)'])
            daily = str(row['Daily Movement (%)'])
            lines.append(f"{symbol:<12} {dist:>12} {daily:>14}")
        lines.append("```")
        return "\n".join(lines)

    def format_ema_touch_section(self, timeframe, df, daily_changes):
        if df.empty:
            return ""

        df_copy = df.copy()
        df_copy['daily'] = df_copy['symbol'].map(daily_changes)

        df_copy['Touches'] = df_copy['touches'].astype(str)
        df_copy['Volume Spikes'] = df_copy['volume_spikes'].astype(str)
        df_copy['Breakout Score'] = df_copy['comprehensive_score'].map('{:.1f}'.format)
        df_copy['Distance (%)'] = df_copy['current_distance'].map('{:.2f}'.format)
        df_copy['Confirmations'] = df_copy['confirmation_factors'].astype(str)
        df_copy['Daily Move (%)'] = df_copy['daily'].map(lambda x: f"{x:.2f}%" if pd.notnull(x) else "N/A")

        display_df = df_copy[['symbol', 'Touches', 'Volume Spikes', 'Breakout Score', 'Confirmations', 'Distance (%)', 'Daily Move (%)']].copy()
        display_df.columns = ['Symbol', 'Touches', 'Volume Spikes', 'Breakout Score', 'Confirmations', 'Distance (%)', 'Daily Move (%)']

        header = f"*{self._escape_md_v2(timeframe)} • Most Probable To Break Structure*"
        lines = [header, "```"]
        row_format = "{:<12} {:>7} {:>9} {:>7} {:>13} {:>9} {:>10}"
        header_row = row_format.format('Symbol', 'Touches', 'VolSpikes', 'Score', 'Confirmations', 'Dist(%)', 'Daily')
        lines.append(header_row)
        lines.append("-" * len(header_row))

        for _, row in display_df.iterrows():
            data_row = row_format.format(
                row['Symbol'],
                row['Touches'],
                row['Volume Spikes'],
                row['Breakout Score'],
                row['Confirmations'],
                row['Distance (%)'],
                row['Daily Move (%)']
            )
            lines.append(data_row)

        lines.append("```")
        return "\n".join(lines)

    async def send_report(self, message):
        await self.bot.send_message(
            chat_id=self.chat_id,
            text=message,
            parse_mode='MarkdownV2'
        )
        await self.bot.send_message(
            chat_id=self.channel_username,
            text=message,
            parse_mode='MarkdownV2'
        )


# --- Build top 40 above/below sections ---

def build_top_sections(df, daily_changes):
    df['daily'] = df['symbol'].map(daily_changes)
    df['Distance (%)'] = df['pct_distance'].map('{:.2f}'.format)
    df['Daily Movement (%)'] = df['daily'].map(lambda x: f"{x:.2f}%" if pd.notnull(x) else "N/A")

    above = df.sort_values('pct_distance', ascending=False).head(40)[['symbol', 'Distance (%)', 'Daily Movement (%)']]
    below = df.sort_values('pct_distance').head(40)[['symbol', 'Distance (%)', 'Daily Movement (%)']]

    above.columns = below.columns = ['Symbol', 'Distance (%)', 'Daily Movement (%)']
    return above, below


# --- Async main scanning and reporting loop ---

async def run_scan_and_report(binance_client, reporter, proxy_pool):
    EMA_TOUCH_THRESHOLD = float(os.getenv("EMA_TOUCH_THRESHOLD", "0.5"))
    EMA_LOOKBACK_PERIOD = int(os.getenv("EMA_LOOKBACK_PERIOD", "20"))
    MIN_TOUCHES_ALERT = int(os.getenv("MIN_TOUCHES_ALERT", "3"))
    MAX_DISTANCE_BELOW_EMA = float(os.getenv("MAX_DISTANCE_BELOW_EMA", "3"))

    perp_symbols = set(binance_client.get_perp_symbols())
    if not perp_symbols:
        logging.error("No perp symbols fetched, aborting scan.")
        return

    spot_symbols = set(binance_client.get_spot_symbols())
    if not spot_symbols:
        logging.error("No spot symbols fetched, aborting scan.")
        return

    symbols_to_process = list(perp_symbols.union(spot_symbols - perp_symbols))
    logging.info(f"Processing {len(symbols_to_process)} symbols (perps + spot-only)")

    # Remove ignored symbols
    symbols_to_process = [s for s in symbols_to_process if s not in IGNORED_SYMBOLS]

    daily_changes = binance_client.fetch_24h_changes()

    ema_touch_timeframes = {'1h', '4h', '1d', '1w'}
    ema_touch_reports = {}

    for tf in ['5m', '15m', '30m', '1h', '4h', '1d', '1w']:
        logging.info(f"Scanning timeframe {tf}")
        results = []
        ema_touch_results = []

        # Use ThreadPoolExecutor for blocking IO calls to Binance API
        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
            futures = {}
            for sym in symbols_to_process:
                market = "perp" if sym in perp_symbols else "spot"
                futures[executor.submit(binance_client.fetch_ohlcv, sym, tf, limit=200, market=market)] = sym

            for future in tqdm.tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc=f"Fetching OHLCV {tf}"
            ):
                sym = futures[future]
                try:
                    df = future.result()

                    pct_dist = calculate_pct_distance(df)
                    results.append((sym, pct_dist))

                    if tf in ema_touch_timeframes:
                        # Pass perp_symbols to scoring function
                        analysis = calculate_comprehensive_breakout_score(df, binance_client, sym, perp_symbols)
                        if analysis is None:
                            continue

                        ema_touch_results.append((
                            sym,
                            analysis['touches'],
                            analysis['volume_spikes'],
                            analysis['comprehensive_score'],
                            analysis['current_distance'],
                            analysis['confirmation_factors']
                        ))

                except Exception as e:
                    logging.warning(f"Failed fetching {sym} {tf}: {e}")

        if not results:
            logging.warning(f"No OHLCV data fetched for timeframe {tf}, skipping Telegram message.")
            continue

        df = pd.DataFrame(results, columns=['symbol', 'pct_distance'])
        above, below = build_top_sections(df, daily_changes)

        msg_parts = []

        if not above.empty:
            msg_parts.append(reporter.format_section(tf, "Above", above))

        if not below.empty:
            msg_parts.append(reporter.format_section(tf, "Below", below))

        if msg_parts:
            full_msg = "\n\n".join(msg_parts)
            try:
                await reporter.send_report(full_msg)
                logging.info(f"Sent Telegram report for timeframe {tf}")
            except Exception as e:
                logging.error(f"Failed to send Telegram message: {e}")

        if tf in ema_touch_timeframes and ema_touch_results:
            ema_touch_reports[tf] = pd.DataFrame(ema_touch_results, columns=[
                'symbol', 'touches', 'volume_spikes', 'comprehensive_score', 'current_distance', 'confirmation_factors'
            ])

        await asyncio.sleep(2)

    for tf, ema_df in ema_touch_reports.items():
        top_touchers = ema_df[ema_df['touches'] >= MIN_TOUCHES_ALERT].sort_values(
            ['comprehensive_score', 'touches'], ascending=[False, False]
        ).head(20)

        if top_touchers.empty:
            logging.info(f"No EMA enhanced signals for timeframe {tf}, skipping EMA enhanced report.")
            continue

        msg = reporter.format_ema_touch_section(tf, top_touchers, daily_changes)
        try:
            await reporter.send_report(msg)
            logging.info(f"Sent EMA enhanced Telegram report for timeframe {tf}")
        except Exception as e:
            logging.error(f"Failed to send EMA enhanced Telegram message: {e}")
        await asyncio.sleep(2)

# --- Async Entrypoint ---

async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    proxy_url = os.getenv("PROXY_LIST_URL")
    if not proxy_url:
        logging.error("Set PROXY_LIST_URL environment variable with your proxy list URL")
        return

    proxy_pool = ProxyPool(max_pool_size=25)
    proxy_pool.populate_from_url(proxy_url)
    proxy_pool.start_checker()
    proxy_pool.start_fastest_proxy_checker()

    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    TELEGRAM_CHANNEL_USERNAME = os.getenv("TELEGRAM_CHANNEL_USERNAME")

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not TELEGRAM_CHANNEL_USERNAME:
        logging.error("Set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, and TELEGRAM_CHANNEL_USERNAME environment variables")
        return

    binance_client = BinanceClient(proxy_pool)

    reporter = TelegramReporter(
        token=TELEGRAM_TOKEN,
        chat_id=int(TELEGRAM_CHAT_ID),
        channel_username=TELEGRAM_CHANNEL_USERNAME
    )

    await run_scan_and_report(binance_client, reporter, proxy_pool)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
