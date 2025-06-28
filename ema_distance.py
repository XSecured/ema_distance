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
    "WBTCUSDT", "YFIUSDT", "BNBUSDT", "XMRUSDT", "SANTOSUSDT",
    "PROMUSDT", "ACMUSDT", "CITYUSDT", "JUVUSDT", "PSGUSDT",
    "WINUSDT"
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
    df['EMA13'] = df['close'].ewm(span=13).mean()
    df['EMA21'] = df['close'].ewm(span=21).mean()
    df['EMA34'] = df['close'].ewm(span=34).mean()
    last = df.iloc[-1]
    ema_alignment = (last['EMA13'] > last['EMA21'] > last['EMA34'])
    above_all_emas = (last['close'] > last['EMA13'] > last['EMA21'])
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


# --- EMA Direction Change Detection ---

def detect_ema_direction_change(df, ema_period=34, lookback_trend=5, reversal_candles=2, pump_threshold=0.5):
    """
    Detect EMA34 direction changes - momentum shift detection (BULLISH ONLY)
    
    Parameters:
    - lookback_trend: How many candles to check for previous trend (default 5)
    - reversal_candles: Consecutive candles needed for reversal confirmation (default 2) 
    - pump_threshold: Percentage pump threshold required as alternative to consecutive candles (default 0.5%)
    """
    df = df.copy()
    df['EMA34'] = df['close'].ewm(span=ema_period, adjust=False).mean()
    
    # Calculate EMA percentage changes
    df['ema_pct_change'] = df['EMA34'].pct_change() * 100
    
    recent_data = df.tail(lookback_trend + reversal_candles)
    
    if len(recent_data) < lookback_trend + reversal_candles:
        return {
            'ema_direction_change': False,
            'change_type': None,
            'momentum_strength': 0,
            'bearish_to_bullish': False,
            'ema_pump_pct': 0
        }
    
    # Split into trend period and reversal period
    trend_period = recent_data.iloc[:-reversal_candles]
    reversal_period = recent_data.iloc[-reversal_candles:]
    
    # BEARISH TO BULLISH DETECTION ONLY
    # Was EMA trending down?
    downward_candles = sum(1 for change in trend_period['ema_pct_change'] if change < -0.01)  # More than -0.01%
    was_bearish = downward_candles >= lookback_trend * 0.6  # 60% of candles were bearish
    
    # Is EMA now trending up?
    upward_candles = sum(1 for change in reversal_period['ema_pct_change'] if change > 0.01)  # More than 0.01%
    consecutive_bullish = upward_candles == reversal_candles
    
    # Check for EMA pump
    ema_start = reversal_period['EMA34'].iloc[0]
    ema_end = reversal_period['EMA34'].iloc[-1]
    ema_pump_pct = ((ema_end - ema_start) / ema_start) * 100 if ema_start > 0 else 0
    has_ema_pump = ema_pump_pct >= pump_threshold
    
    # EITHER condition must be met: consecutive bullish candles OR pump threshold
    bearish_to_bullish = was_bearish and (consecutive_bullish or has_ema_pump)
    
    momentum_strength = 0
    change_type = None
    
    if bearish_to_bullish:
        change_type = 'bearish_to_bullish'
        momentum_strength = max(ema_pump_pct, upward_candles * 0.5)  # Stronger signal = higher score
    
    return {
        'ema_direction_change': bearish_to_bullish,
        'change_type': change_type,
        'bearish_to_bullish': bearish_to_bullish,
        'momentum_strength': momentum_strength,
        'ema_pump_pct': ema_pump_pct
    }


# --- EMA distance calculation (original function preserved for compatibility) ---

def calculate_pct_distance(df):
    df['EMA34'] = df['close'].ewm(span=34, adjust=False).mean()
    last = df.iloc[-1]
    return (last['close'] - last['EMA34']) / last['EMA34'] * 100

# --- Add this new function for unfiltered EMA distance calculation ---

def calculate_simple_ema_distance(df):
    """
    Simple EMA distance calculation without any filtering - for traditional above/below reports.
    """
    df = df.copy()
    df['EMA34'] = df['close'].ewm(span=34, adjust=False).mean()
    df['pct_distance'] = (df['close'] - df['EMA34']) / df['EMA34'] * 100
    last_distance = df.iloc[-1]['pct_distance']
    
    return {
        'symbol': None,  # Will be added later
        'pct_distance': last_distance
    }

# --- Enhanced EMA analysis function with relative volume approach ---

def calculate_enhanced_ema_analysis(df, min_distance_above=0.1, lookback_period=20, max_distance_above=5.0):
    """
    Enhanced EMA analysis based on being ABOVE the EMA34 line.
    Instead of counting touches, we now focus on:
    - How far above the EMA34 the price is
    - How consistently it stays above
    - Combined with other technical indicators
    """
    df = df.copy()
    df['EMA34'] = df['close'].ewm(span=34, adjust=False).mean()
    df['pct_distance'] = (df['close'] - df['EMA34']) / df['EMA34'] * 100

    recent_data = df.tail(lookback_period)
    
    # Check if price is currently above EMA34
    last_distance = recent_data.iloc[-1]['pct_distance']
    if last_distance < min_distance_above:  # Must be at least min_distance_above% above EMA
        return None
    
    # Don't include symbols that are too far above (potential exhaustion)
    if last_distance > max_distance_above:
        return None
    
    # Count how many candles in lookback period were above EMA34
    candles_above = sum(1 for i in range(len(recent_data)) 
                       if recent_data.iloc[i]['pct_distance'] > 0)
    consistency_ratio = candles_above / len(recent_data)
    
    # Calculate the average distance above EMA when price was above
    distances_above = [d for d in recent_data['pct_distance'] if d > 0]
    avg_distance_above = sum(distances_above) / len(distances_above) if distances_above else 0
    
    # Check if price recently crossed above EMA34 (bullish momentum)
    recent_cross = False
    if len(recent_data) >= 3:
        # Was below or near EMA 3 candles ago, now clearly above
        if recent_data.iloc[-3]['pct_distance'] < 0.5 and last_distance > min_distance_above:
            recent_cross = True

    # Calculate all technical indicators
    macd_data = calculate_macd(df)
    rel_vol_data = calculate_relative_volume(df, lookback_period)
    ema_signals = calculate_multiple_ema_signals(df)
    consolidation_data = detect_consolidation(df, lookback_period)
    direction_change_data = detect_ema_direction_change(df)

    # New scoring system based on being above EMA34
    breakout_score = (
        (consistency_ratio * 10) +  # How consistently above EMA34 (0-10 points)
        (min(avg_distance_above, 3) * 2) +  # Average distance above (capped at 6 points)
        (5 if recent_cross else 0) +  # Recent cross above EMA34
        (5 if rel_vol_data['volume_surge'] else 0) +  # Volume surge confirmation
        (3 if macd_data['macd_bullish'] else 0) +  # MACD momentum
        (2 if ema_signals['ema_alignment'] else 0) +  # EMA alignment
        (1 if ema_signals['above_all_emas'] else 0) +  # Price position
        (3 if consolidation_data['breakout_potential'] else 
         1 if consolidation_data['is_consolidating'] else 0) +  # Consolidation breakout
        (8 if direction_change_data['bearish_to_bullish'] else 0)  # Momentum shift
    )

    return {
        'symbol': None,  # Will be added later
        'breakout_score': breakout_score,
        'current_distance': last_distance,
        'pct_distance': last_distance,
        'consistency_above': consistency_ratio,
        'avg_distance_above': avg_distance_above,
        'recent_cross': recent_cross,
        'candles_above': candles_above,
        'macd_bullish': macd_data['macd_bullish'],
        'relative_volume': rel_vol_data['relative_volume'],
        'volume_surge': rel_vol_data['volume_surge'],
        'ema_alignment': ema_signals['ema_alignment'],
        'above_all_emas': ema_signals['above_all_emas'],
        'is_consolidating': consolidation_data['is_consolidating'],
        'breakout_potential': consolidation_data['breakout_potential'],
        'ema_direction_change': direction_change_data['ema_direction_change'],
        'bearish_to_bullish': direction_change_data['bearish_to_bullish'],
        'momentum_strength': direction_change_data['momentum_strength'],
        'ema_pump_pct': direction_change_data.get('ema_pump_pct', 0)
    }

# --- Telegram Reporter (async) ---

class TelegramReporter:
    def __init__(self, token, chat_id, channel_username):
        """
        :param token: Telegram bot token
        :param chat_id: Your personal chat ID (int)
        :param channel_username: Channel username string starting with '@'
        """
        self.bot = Bot(token=token)
        self.chat_id = chat_id
        self.channel_username = channel_username

    def _escape_md_v2(self, text):
        escape_chars = r'_*[]()~`>#+-=|{}.!'
        return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

    def format_section(self, timeframe, position, df):
        # Escape only the header (outside code block)
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

    def format_enhanced_ema_section(self, timeframe: str, df: pd.DataFrame,
                                    daily_changes: dict) -> str:
        """Pretty printing for the *above-EMA* breakout list."""
    
        if df.empty:
            return ""
    
        # ---------- Build printable dataframe ----------
        df_copy = df.copy()
        df_copy["daily"] = df_copy["symbol"].map(daily_changes)
    
        # numeric / text conversions ------------------------------------------------
        df_copy["Score"]    = df_copy["breakout_score"].map("{:.1f}".format)
        df_copy["Dist%"]    = df_copy["current_distance"].map("{:.1f}".format)
        df_copy["Cons%"]    = df_copy["consistency_above"].map(lambda x: f"{x*100:.0f}")
        df_copy["Cross"]    = df_copy["recent_cross"].map(lambda x: "✓" if x else "")
        df_copy["Vol"]      = df_copy["relative_volume"].map("{:.1f}".format)
        df_copy["Momentum"] = df_copy.apply(
            lambda r: "🚀" if r["bearish_to_bullish"] else "", axis=1
        )
        df_copy["Daily"] = df_copy["daily"].map(
            lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A"
        )
    
        # columns that were missing -------------------------------------------------
        # MACD: show an arrow ↑ / ↓ depending on bullish signal
        df_copy["MACD"] = df_copy["macd_bullish"].map(lambda x: "↑" if x else "↓")
    
        # EMA alignment flag
        df_copy["EMA"] = df_copy["ema_alignment"].map(lambda x: "✓" if x else "")
    
        # Consolidation / breakout hint
        #  ⚡ = breakout potential, □ = still consolidating, '' = none
        def _consol(row):
            if row["breakout_potential"]:
                return "⚡"
            if row["is_consolidating"]:
                return "□"
            return ""
        df_copy["Con"] = df_copy.apply(_consol, axis=1)
    
        # ---------- Assemble markdown text block ----------
        header  = f"*{self._escape_md_v2(timeframe)} • Enhanced Breakout Analysis*"
        lines = [header, "```"]
    
        # Header row – widths chosen for neat alignment
        lines.append(
            f"{'Symbol':<12}{'Score':>6}{'Dist%':>6}{'Cons%':>6}"
            f"{'Cross':>6}{'MACD':>6}{'Vol':>6}{'EMA':>5}"
            f"{'Mom':>4}{'Con':>4}{'Daily':>9}"
        )
        lines.append("-" * 71)
    
        # Data rows – same order as header
        for _, row in df_copy.iterrows():
            lines.append(
                f"{row['symbol']:<12}{row['Score']:>6}{row['Dist%']:>6}{row['Cons%']:>6}"
                f"{row['Cross']:>6}{row['MACD']:>6}{row['Vol']:>6}{row['EMA']:>5}"
                f"{row['Momentum']:>4}{row['Con']:>4}{row['Daily']:>9}"
            )
    
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

async def run_scan_and_report(binance_client, reporter, proxy_pool):
    # Update environment variables for the new approach
    MIN_DISTANCE_ABOVE_EMA = float(os.getenv("MIN_DISTANCE_ABOVE_EMA", "0.1"))  # Must be at least 0.1% above
    MAX_DISTANCE_ABOVE_EMA = float(os.getenv("MAX_DISTANCE_ABOVE_EMA", "5.0"))  # Not more than 5% above
    EMA_LOOKBACK_PERIOD = int(os.getenv("EMA_LOOKBACK_PERIOD", "20"))
    MIN_BREAKOUT_SCORE = float(os.getenv("MIN_BREAKOUT_SCORE", "10"))  # Minimum score to report
    EMA_TREND_LOOKBACK = int(os.getenv("EMA_TREND_LOOKBACK", "5"))
    EMA_REVERSAL_CANDLES = int(os.getenv("EMA_REVERSAL_CANDLES", "2"))
    EMA_PUMP_THRESHOLD = float(os.getenv("EMA_PUMP_THRESHOLD", "0.5"))  # Default to 0.5%

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

    ema_touch_timeframes = {'15m','1h', '4h', '1d', '1w'}

    for tf in ['5m', '15m', '1h', '4h', '1d', '1w']:
        logging.info(f"Scanning timeframe {tf}")
        ema_above_results = []
        traditional_results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
            futures = {}
            for sym in symbols_to_process:
                if sym in perp_symbols:
                    futures[executor.submit(binance_client.fetch_ohlcv, sym, tf, limit=200, market="perp")] = sym
                else:
                    futures[executor.submit(binance_client.fetch_ohlcv, sym, tf, limit=200, market="spot")] = sym

            for future in tqdm.tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc=f"Fetching OHLCV {tf}"
            ):
                sym = futures[future]
                try:
                    df = future.result()

                    # Enhanced analysis for symbols above EMA34
                    analysis = calculate_enhanced_ema_analysis(
                        df,
                        min_distance_above=MIN_DISTANCE_ABOVE_EMA,
                        lookback_period=EMA_LOOKBACK_PERIOD,
                        max_distance_above=MAX_DISTANCE_ABOVE_EMA
                    )
                    if analysis is not None:
                        analysis['symbol'] = sym
                        ema_above_results.append(analysis)

                    # Simple distance calculation for traditional reports
                    simple_analysis = calculate_simple_ema_distance(df)
                    simple_analysis['symbol'] = sym
                    traditional_results.append(simple_analysis)

                except Exception as e:
                    logging.warning(f"Failed fetching {sym} {tf}: {e}")
                    
        # Send traditional above/below EMA reports using UNFILTERED data
        if traditional_results:
            traditional_df = pd.DataFrame(traditional_results)
            above, below = build_top_sections(traditional_df, daily_changes)

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

        # Send enhanced breakout analysis for selected timeframes using FILTERED data
        if tf in ema_touch_timeframes and ema_above_results:
            df = pd.DataFrame(ema_above_results)
            top_above_ema = df[df['breakout_score'] >= MIN_BREAKOUT_SCORE].sort_values(
                'breakout_score', ascending=False
            ).head(20)

            if top_above_ema.empty:
                logging.info(f"No above-EMA signals for timeframe {tf}, skipping enhanced report.")
            else:
                msg = reporter.format_enhanced_ema_section(tf, top_above_ema, daily_changes)
                try:
                    await reporter.send_report(msg)
                    logging.info(f"Sent above-EMA enhanced report for timeframe {tf}")
                except Exception as e:
                    logging.error(f"Failed to send above-EMA enhanced message: {e}")
                await asyncio.sleep(2)

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
