import asyncio
import itertools
import logging
import os
import random
import re
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import numpy as np
import pandas as pd
from tqdm.asyncio import tqdm

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


# =============================================================================
# CONSTANTS
# =============================================================================
IGNORED_SYMBOLS = {
    "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
    "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT",
    "WBTCUSDT", "YFIUSDT", "BNBUSDT", "XMRUSDT", "SANTOSUSDT",
    "PROMUSDT", "ACMUSDT", "CITYUSDT", "JUVUSDT", "PSGUSDT",
    "WINUSDT"
}

ENHANCED_TIMEFRAMES = {"15m", "1h", "4h", "1d", "1w"}
ALL_TIMEFRAMES = ["5m", "15m", "1h", "4h", "1d", "1w"]

BACKUP_PROXY_URL = (
    "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt"
)


# =============================================================================
# CONFIGURATION
# =============================================================================
class Config:
    """Centralized configuration loaded from environment variables."""

    def __init__(self) -> None:
        # Telegram
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.telegram_channel_username = os.getenv("TELEGRAM_CHANNEL_USERNAME", "")

        # Proxies
        self.proxy_list_url = os.getenv("PROXY_LIST_URL", "")
        self.backup_proxy_url = os.getenv("BACKUP_PROXY_URL", BACKUP_PROXY_URL)

        # OHLC
        self.show_d_plus = os.getenv("SHOW_D_PLUS", "True").lower() == "true"
        self.show_d_minus = os.getenv("SHOW_D_MINUS", "True").lower() == "false"
        self.show_m_plus = os.getenv("SHOW_M_PLUS", "True").lower() == "true"
        self.show_m_minus = os.getenv("SHOW_M_MINUS", "True").lower() == "false"
        self.ohlc_lookback = int(os.getenv("OHLC_LOOKBACK", "60"))
        self.ohlc_alert_threshold = float(os.getenv("OHLC_ALERT_THRESHOLD", "2.0"))

        # EMA Analysis
        self.min_distance_above_ema = float(os.getenv("MIN_DISTANCE_ABOVE_EMA", "0.1"))
        self.max_distance_above_ema = float(os.getenv("MAX_DISTANCE_ABOVE_EMA", "5.0"))
        self.ema_lookback_period = int(os.getenv("EMA_LOOKBACK_PERIOD", "20"))
        self.min_breakout_score = float(os.getenv("MIN_BREAKOUT_SCORE", "10"))
        self.ema_trend_lookback = int(os.getenv("EMA_TREND_LOOKBACK", "5"))
        self.ema_reversal_candles = int(os.getenv("EMA_REVERSAL_CANDLES", "2"))
        self.ema_pump_threshold = float(os.getenv("EMA_PUMP_THRESHOLD", "0.5"))

        # Pool / Concurrency
        self.proxy_max_pool = int(os.getenv("PROXY_MAX_POOL", "25"))
        self.proxy_min_pool = int(os.getenv("PROXY_MIN_POOL", "15"))
        self.proxy_validation_concurrency = int(os.getenv("PROXY_VAL_CONCURRENCY", "50"))
        self.proxy_refresh_interval = float(os.getenv("PROXY_REFRESH_INTERVAL", "180"))
        self.fetch_concurrency = int(os.getenv("FETCH_CONCURRENCY", "50"))
        self.calc_workers = int(os.getenv("CALC_WORKERS", "8"))

        self.validate()

    def validate(self) -> None:
        required = [
            ("TELEGRAM_BOT_TOKEN", self.telegram_bot_token),
            ("TELEGRAM_CHAT_ID", self.telegram_chat_id),
            ("TELEGRAM_CHANNEL_USERNAME", self.telegram_channel_username),
            ("PROXY_LIST_URL", self.proxy_list_url),
        ]
        missing = [name for name, val in required if not val]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


# =============================================================================
# LOGGING
# =============================================================================
def setup_logging() -> None:
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# =============================================================================
# PROXY INFRASTRUCTURE
# =============================================================================
class ProxyState(Enum):
    ACTIVE = "active"
    COOLING = "cooling"
    BANNED = "banned"


@dataclass
class ProxyStats:
    successes: int = 0
    failures: int = 0
    consecutive_failures: int = 0
    total_latency_ms: float = 0.0
    last_used: float = field(default_factory=time.time)
    last_success: float = 0.0
    last_failure: float = 0.0
    state: ProxyState = ProxyState.ACTIVE
    cooldown_until: float = 0.0

    @property
    def total_uses(self) -> int:
        return self.successes + self.failures

    @property
    def success_rate(self) -> float:
        if self.total_uses == 0:
            return 0.8
        return self.successes / self.total_uses

    @property
    def avg_latency_ms(self) -> float:
        if self.successes == 0:
            return 9999.0
        return self.total_latency_ms / self.successes

    def compute_score(self) -> float:
        if self.state != ProxyState.ACTIVE:
            return 0.0
        score = self.success_rate
        score *= 0.5 ** self.consecutive_failures
        if self.avg_latency_ms < 9999:
            latency_factor = max(0.1, 1.0 - (self.avg_latency_ms / 5000))
            score *= 0.6 + 0.4 * latency_factor
        return max(0.001, min(1.0, score))


class RobustProxyPool:
    """Production-grade async proxy pool with backup sources, health checks,
    fastest-proxy tracking, and automatic recovery."""

    def __init__(
        self,
        max_pool_size: int = 25,
        min_pool_size: int = 15,
        max_consecutive_failures: int = 3,
        cooldown_seconds: float = 90.0,
        ban_after_uses: int = 8,
        ban_below_rate: float = 0.25,
        validation_concurrency: int = 50,
        refresh_interval: float = 180.0,
    ):
        self.max_pool_size = max_pool_size
        self.min_pool_size = min_pool_size
        self.max_consecutive_failures = max_consecutive_failures
        self.cooldown_seconds = cooldown_seconds
        self.ban_after_uses = ban_after_uses
        self.ban_below_rate = ban_below_rate
        self.validation_concurrency = validation_concurrency
        self.refresh_interval = refresh_interval

        self._proxies: Dict[str, ProxyStats] = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._source_url: Optional[str] = None
        self._backup_url: Optional[str] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._fastest_proxy: Optional[str] = None
        self._stop_event = asyncio.Event()

    # ------------------------------------------------------------------
    async def initialize(
        self,
        session: aiohttp.ClientSession,
        source_url: str,
        backup_url: Optional[str] = None,
    ) -> None:
        self._session = session
        self._source_url = source_url
        self._backup_url = backup_url

        await self._populate_pool(primary=True)
        if self._active_count() < self.min_pool_size and self._backup_url:
            logging.warning("Primary proxy source insufficient (%d/%d), trying backup...",
                            self._active_count(), self.min_pool_size)
            await self._populate_pool(primary=False)

        if self._active_count() == 0:
            raise RuntimeError("No working proxies available from primary or backup sources.")

        self._refresh_task = asyncio.create_task(self._refresh_loop())
        logging.info("Proxy pool initialized with %d active proxies.", self._active_count())

    # ------------------------------------------------------------------
    def _active_count(self) -> int:
        return sum(1 for s in self._proxies.values() if s.state == ProxyState.ACTIVE)

    # ------------------------------------------------------------------
    async def _refresh_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.refresh_interval)
            except asyncio.TimeoutError:
                pass
            if self._stop_event.is_set():
                break
            try:
                await self._populate_pool(primary=True)
                if self._active_count() < self.min_pool_size and self._backup_url:
                    await self._populate_pool(primary=False)
            except Exception as exc:
                logging.error("Proxy refresh error: %s", exc)

    # ------------------------------------------------------------------
    async def _populate_pool(self, primary: bool = True) -> None:
        url = self._source_url if primary else self._backup_url
        if not url:
            return
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with self._session.get(url, timeout=timeout) as resp:
                if resp.status != 200:
                    logging.warning("Proxy source %s returned %d", url, resp.status)
                    return
                text = await resp.text()
        except Exception as exc:
            logging.warning("Failed to fetch proxy list from %s: %s", url, exc)
            return

        raw = {line.strip() for line in text.splitlines() if line.strip() and "." in line}
        new_proxies = {p if "://" in p else f"http://{p}" for p in raw}
        to_validate = list(new_proxies - set(self._proxies.keys()))
        if not to_validate:
            return

        sem = asyncio.Semaphore(self.validation_concurrency)

        async def _validate(proxy: str) -> Tuple[str, bool, float]:
            async with sem:
                start = time.time()
                try:
                    async with self._session.get(
                        "https://api.binance.com/api/v3/time",
                        proxy=proxy,
                        timeout=aiohttp.ClientTimeout(total=8, connect=5),
                    ) as r:
                        if r.status == 200:
                            return proxy, True, (time.time() - start) * 1000
                except Exception:
                    pass
                return proxy, False, 0.0

        tasks = [asyncio.create_task(_validate(p)) for p in to_validate]
        for coro in asyncio.as_completed(tasks):
            proxy, ok, lat = await coro
            if ok:
                async with self._lock:
                    if self._active_count() < self.max_pool_size:
                        self._proxies[proxy] = ProxyStats(
                            successes=1, total_latency_ms=lat, last_success=time.time()
                        )

    # ------------------------------------------------------------------
    async def get_proxy(self) -> Optional[str]:
        now = time.time()
        async with self._lock:
            active: List[Tuple[str, float]] = []
            for p, s in self._proxies.items():
                if s.state == ProxyState.COOLING and now > s.cooldown_until:
                    s.state = ProxyState.ACTIVE
                    s.consecutive_failures = 0
                if s.state == ProxyState.ACTIVE:
                    active.append((p, s.compute_score()))

            if not active:
                return None

            total = sum(score for _, score in active)
            if total <= 0:
                return active[-1][0]

            r = random.random() * total
            cumulative = 0.0
            for p, score in active:
                cumulative += score
                if cumulative >= r:
                    self._proxies[p].last_used = now
                    return p
            return active[-1][0]

    # ------------------------------------------------------------------
    async def report_success(self, proxy: str, latency: float) -> None:
        async with self._lock:
            if proxy in self._proxies:
                s = self._proxies[proxy]
                s.successes += 1
                s.consecutive_failures = 0
                s.total_latency_ms += latency
                s.last_success = time.time()

    async def report_failure(self, proxy: str) -> None:
        async with self._lock:
            if proxy in self._proxies:
                s = self._proxies[proxy]
                s.failures += 1
                s.consecutive_failures += 1
                s.last_failure = time.time()
                if s.consecutive_failures >= self.max_consecutive_failures:
                    s.state = ProxyState.COOLING
                    s.cooldown_until = time.time() + self.cooldown_seconds
                if s.total_uses >= self.ban_after_uses and s.success_rate < self.ban_below_rate:
                    s.state = ProxyState.BANNED

    # ------------------------------------------------------------------
    async def shutdown(self) -> None:
        self._stop_event.set()
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass


# =============================================================================
# BINANCE SCANNER
# =============================================================================
class BinanceScanner:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        proxy_pool: RobustProxyPool,
        max_retries: int = 5,
        concurrency: int = 50,
    ):
        self.session = session
        self.proxies = proxy_pool
        self.max_retries = max_retries
        self.sem = asyncio.Semaphore(concurrency)
        self._request_count = 0
        self._error_count = 0

    async def _request(self, url: str, params: Optional[dict] = None) -> Any:
        for attempt in range(1, self.max_retries + 1):
            proxy = await self.proxies.get_proxy()
            if not proxy:
                logging.error("No active proxies available for request")
                await asyncio.sleep(1)
                continue

            start = time.time()
            try:
                async with self.sem:
                    async with self.session.get(
                        url,
                        params=params,
                        proxy=proxy,
                        timeout=aiohttp.ClientTimeout(total=10, connect=5),
                    ) as resp:
                        latency = (time.time() - start) * 1000
                        if resp.status == 200:
                            await self.proxies.report_success(proxy, latency)
                            self._request_count += 1
                            return await resp.json()
                        if resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", 2))
                            logging.warning("Rate limited (429), sleeping %ds", retry_after)
                            await asyncio.sleep(retry_after)
                        else:
                            logging.debug("HTTP %d from %s", resp.status, url)
            except aiohttp.ClientError as exc:
                logging.debug("Request exception (attempt %d): %s", attempt, exc)
            except asyncio.TimeoutError:
                logging.debug("Timeout on attempt %d", attempt)
            except Exception as exc:
                logging.debug("Unexpected error (attempt %d): %s", attempt, exc)

            await self.proxies.report_failure(proxy)
            self._error_count += 1
            backoff = self._backoff(attempt)
            await asyncio.sleep(backoff)

        logging.error("All %d retries failed for %s", self.max_retries, url)
        return None

    @staticmethod
    def _backoff(attempt: int) -> float:
        return min(2 ** attempt + random.uniform(0, 1), 30)

    async def get_all_symbols(self) -> Tuple[Set[str], Set[str]]:
        f_info = await self._request("https://fapi.binance.com/fapi/v1/exchangeInfo")
        s_info = await self._request("https://api.binance.com/api/v3/exchangeInfo")

        perps: Set[str] = set()
        if f_info:
            perps = {
                s["symbol"]
                for s in f_info.get("symbols", [])
                if s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING"
                and s.get("quoteAsset") == "USDT"
            }

        spots: Set[str] = set()
        if s_info:
            spots = {
                s["symbol"]
                for s in s_info.get("symbols", [])
                if s.get("status") == "TRADING"
                and s.get("quoteAsset") == "USDT"
                and any(
                    "SPOT" in perm
                    for perm in itertools.chain.from_iterable(s.get("permissionSets", []))
                )
            }

        logging.info("Fetched %d perp and %d spot USDT symbols", len(perps), len(spots))
        return perps, spots

    async def fetch_24h_changes(self) -> Dict[str, float]:
        s_data = await self._request("https://api.binance.com/api/v3/ticker/24hr")
        f_data = await self._request("https://fapi.binance.com/fapi/v1/ticker/24hr")
        res: Dict[str, float] = {}
        if s_data:
            res.update({i["symbol"]: float(i["priceChangePercent"]) for i in s_data})
        if f_data:
            res.update({i["symbol"]: float(i["priceChangePercent"]) for i in f_data})
        return res

    async def fetch_ohlcv(
        self, symbol: str, interval: str, market: str, limit: int
    ) -> Optional[pd.DataFrame]:
        base = (
            "https://fapi.binance.com/fapi/v1/klines"
            if market == "perp"
            else "https://api.binance.com/api/v3/klines"
        )
        data = await self._request(base, {"symbol": symbol, "interval": interval, "limit": limit})
        if not data:
            return None
        df = pd.DataFrame(
            data,
            columns=[
                "ot", "open", "high", "low", "close", "volume",
                "ct", "qav", "nt", "tbb", "tbq", "i",
            ],
        )
        cols = ["open", "high", "low", "close", "volume"]
        df[cols] = df[cols].apply(pd.to_numeric, errors="coerce")
        return df


# =============================================================================
# CALCULATION ENGINE  (CPU-bound work wrapped in thread pool)
# =============================================================================
class CalculationEngine:
    def __init__(self, max_workers: int = 8):
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="calc")

    async def simple_ema(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, _calc_simple_ema_distance, df)

    async def enhanced_ema(self, df: pd.DataFrame, cfg: Config) -> Optional[Dict[str, Any]]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            _calc_enhanced_ema_analysis,
            df,
            cfg.min_distance_above_ema,
            cfg.ema_lookback_period,
            cfg.max_distance_above_ema,
            cfg.ema_trend_lookback,
            cfg.ema_reversal_candles,
            cfg.ema_pump_threshold,
        )

    async def ohlc(self, df: pd.DataFrame, lookback: int) -> Optional[Dict[str, Any]]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, _calc_ohlc_projections, df, lookback)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True)


# ------------------------------------------------------------------
# Pure calculation functions (run inside thread pool)
# ------------------------------------------------------------------
def _calc_simple_ema_distance(df: pd.DataFrame) -> Dict[str, Any]:
    df = df.copy()
    df["EMA34"] = df["close"].ewm(span=34, adjust=False).mean()
    df["pct_distance"] = (df["close"] - df["EMA34"]) / df["EMA34"] * 100
    return {"symbol": None, "pct_distance": df.iloc[-1]["pct_distance"]}


def _calc_enhanced_ema_analysis(
    df: pd.DataFrame,
    min_distance_above: float,
    lookback_period: int,
    max_distance_above: float,
    trend_lookback: int,
    reversal_candles: int,
    pump_threshold: float,
) -> Optional[Dict[str, Any]]:
    df = df.copy()
    df["EMA34"] = df["close"].ewm(span=34, adjust=False).mean()
    df["pct_distance"] = (df["close"] - df["EMA34"]) / df["EMA34"] * 100
    recent_data = df.tail(lookback_period)
    last_distance = recent_data.iloc[-1]["pct_distance"]

    if last_distance < min_distance_above or last_distance > max_distance_above:
        return None

    candles_above = sum(1 for i in range(len(recent_data)) if recent_data.iloc[i]["pct_distance"] > 0)
    consistency_ratio = candles_above / len(recent_data)
    distances_above = [d for d in recent_data["pct_distance"] if d > 0]
    avg_distance_above = sum(distances_above) / len(distances_above) if distances_above else 0

    recent_cross = False
    if len(recent_data) >= 3:
        if recent_data.iloc[-3]["pct_distance"] < 0.5 and last_distance > min_distance_above:
            recent_cross = True

    macd_data = _calc_macd(df)
    rel_vol_data = _calc_relative_volume(df, lookback_period)
    ema_signals = _calc_multiple_ema_signals(df)
    consolidation_data = _detect_consolidation(df, lookback_period)
    direction_change_data = _detect_ema_direction_change(
        df, 34, trend_lookback, reversal_candles, pump_threshold
    )

    breakout_score = (
        (consistency_ratio * 10)
        + (min(avg_distance_above, 3) * 2)
        + (5 if recent_cross else 0)
        + (5 if rel_vol_data["volume_surge"] else 0)
        + (3 if macd_data["macd_bullish"] else 0)
        + (2 if ema_signals["ema_alignment"] else 0)
        + (1 if ema_signals["above_all_emas"] else 0)
        + (
            3 if consolidation_data["breakout_potential"] else
            1 if consolidation_data["is_consolidating"] else 0
        )
        + (8 if direction_change_data["bearish_to_bullish"] else 0)
    )

    return {
        "symbol": None,
        "breakout_score": breakout_score,
        "current_distance": last_distance,
        "pct_distance": last_distance,
        "consistency_above": consistency_ratio,
        "avg_distance_above": avg_distance_above,
        "recent_cross": recent_cross,
        "candles_above": candles_above,
        "macd_bullish": macd_data["macd_bullish"],
        "relative_volume": rel_vol_data["relative_volume"],
        "volume_surge": rel_vol_data["volume_surge"],
        "ema_alignment": ema_signals["ema_alignment"],
        "above_all_emas": ema_signals["above_all_emas"],
        "is_consolidating": consolidation_data["is_consolidating"],
        "breakout_potential": consolidation_data["breakout_potential"],
        "ema_direction_change": direction_change_data["ema_direction_change"],
        "bearish_to_bullish": direction_change_data["bearish_to_bullish"],
        "momentum_strength": direction_change_data["momentum_strength"],
        "ema_pump_pct": direction_change_data.get("ema_pump_pct", 0),
    }


def _calc_macd(df: pd.DataFrame) -> Dict[str, Any]:
    ema12 = df["close"].ewm(span=12).mean()
    ema26 = df["close"].ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    return {
        "macd": macd.iloc[-1],
        "signal": signal.iloc[-1],
        "macd_bullish": macd.iloc[-1] > signal.iloc[-1],
    }


def _calc_relative_volume(df: pd.DataFrame, lookback: int = 20) -> Dict[str, Any]:
    recent_vol = df["volume"].astype(float).tail(lookback).mean()
    longer_vol = df["volume"].astype(float).tail(lookback * 3).mean()
    rel_vol = recent_vol / longer_vol if longer_vol > 0 else 1.0
    return {"relative_volume": rel_vol, "volume_surge": rel_vol > 1.5}


def _calc_multiple_ema_signals(df: pd.DataFrame) -> Dict[str, Any]:
    df["EMA13"] = df["close"].ewm(span=13).mean()
    df["EMA21"] = df["close"].ewm(span=21).mean()
    df["EMA34"] = df["close"].ewm(span=34).mean()
    last = df.iloc[-1]
    return {
        "ema_alignment": last["EMA13"] > last["EMA21"] > last["EMA34"],
        "above_all_emas": last["close"] > last["EMA13"] > last["EMA21"],
    }


def _detect_consolidation(df: pd.DataFrame, lookback: int = 20) -> Dict[str, Any]:
    recent = df.tail(lookback)
    price_range = recent["high"].max() - recent["low"].min()
    avg_price = recent["close"].mean()
    ratio = price_range / avg_price if avg_price else 1.0
    return {"is_consolidating": ratio < 0.10, "breakout_potential": ratio < 0.05}


def _detect_ema_direction_change(
    df: pd.DataFrame,
    ema_period: int = 34,
    lookback_trend: int = 5,
    reversal_candles: int = 2,
    pump_threshold: float = 0.5,
) -> Dict[str, Any]:
    df = df.copy()
    df["EMA34"] = df["close"].ewm(span=ema_period, adjust=False).mean()
    df["ema_pct_change"] = df["EMA34"].pct_change() * 100
    recent_data = df.tail(lookback_trend + reversal_candles)

    if len(recent_data) < lookback_trend + reversal_candles:
        return {
            "ema_direction_change": False,
            "change_type": None,
            "momentum_strength": 0,
            "bearish_to_bullish": False,
            "ema_pump_pct": 0,
        }

    trend_period = recent_data.iloc[:-reversal_candles]
    reversal_period = recent_data.iloc[-reversal_candles:]

    downward_candles = sum(1 for c in trend_period["ema_pct_change"] if c < -0.01)
    was_bearish = downward_candles >= lookback_trend * 0.6
    upward_candles = sum(1 for c in reversal_period["ema_pct_change"] if c > 0.01)
    consecutive_bullish = upward_candles == reversal_candles

    ema_start = reversal_period["EMA34"].iloc[0]
    ema_end = reversal_period["EMA34"].iloc[-1]
    ema_pump_pct = ((ema_end - ema_start) / ema_start) * 100 if ema_start > 0 else 0
    has_ema_pump = ema_pump_pct >= pump_threshold
    bearish_to_bullish = was_bearish and (consecutive_bullish or has_ema_pump)

    momentum_strength = 0
    change_type = None
    if bearish_to_bullish:
        change_type = "bearish_to_bullish"
        momentum_strength = max(ema_pump_pct, upward_candles * 0.5)

    return {
        "ema_direction_change": bearish_to_bullish,
        "change_type": change_type,
        "bearish_to_bullish": bearish_to_bullish,
        "momentum_strength": momentum_strength,
        "ema_pump_pct": ema_pump_pct,
    }


def _calc_ohlc_projections(df: pd.DataFrame, lookback: int = 60) -> Optional[Dict[str, Any]]:
    df = df.copy()
    df["is_bull"] = df["close"] > df["open"]
    df["manip_wick"] = np.where(df["is_bull"], df["open"] - df["low"], df["high"] - df["open"])
    df["dist_dist"] = np.where(df["is_bull"], df["high"] - df["open"], df["open"] - df["low"])
    if len(df) < lookback + 1:
        return None
    recent_closed = df.iloc[-(lookback + 1) : -1]
    avg_manip = recent_closed["manip_wick"].mean()
    avg_dist = recent_closed["dist_dist"].mean()
    current_open = df.iloc[-1]["open"]
    current_close = df.iloc[-1]["close"]
    return {
        "d_plus": current_open + avg_dist + avg_manip,
        "d_minus": current_open - avg_dist - avg_manip,
        "m_minus": current_open + avg_manip,
        "m_plus": current_open - avg_manip,
        "current_close": current_close,
    }


def build_top_sections(df: pd.DataFrame, daily_changes: Dict[str, float]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    df["daily"] = df["symbol"].map(daily_changes)
    df["Distance (%)"] = df["pct_distance"].map("{:.2f}".format)
    df["Daily Movement (%)"] = df["daily"].map(lambda x: f"{x:.2f}%" if pd.notnull(x) else "N/A")
    above = df.sort_values("pct_distance", ascending=False).head(40)[
        ["symbol", "Distance (%)", "Daily Movement (%)"]
    ]
    below = df.sort_values("pct_distance").head(40)[
        ["symbol", "Distance (%)", "Daily Movement (%)"]
    ]
    above.columns = ["Symbol", "Distance (%)", "Daily Movement (%)"]
    below.columns = ["Symbol", "Distance (%)", "Daily Movement (%)"]
    return above, below


# =============================================================================
# TELEGRAM REPORTER
# =============================================================================
class Reporter:
    def __init__(
        self,
        token: str,
        chat_id: str,
        channel: str,
        session: aiohttp.ClientSession,
    ):
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.chat_id = chat_id
        self.channel = channel
        self.session = session

    @staticmethod
    def esc(t: Any) -> str:
        return re.sub(r"([_**\[\]()~`>#+\-=|{}.!])", r"\\\1", str(t))

    async def send(self, msg: str) -> None:
        for target in [self.chat_id, self.channel]:
            if not target:
                continue
            try:
                payload = {"chat_id": target, "text": msg, "parse_mode": "MarkdownV2"}
                async with self.session.post(self.url, json=payload) as r:
                    if r.status == 429:
                        retry_after = int(r.headers.get("Retry-After", 5))
                        await asyncio.sleep(retry_after)
            except Exception as exc:
                logging.debug("Telegram send error: %s", exc)

    def format_section(self, timeframe: str, position: str, df: pd.DataFrame) -> str:
        header = f"*{self.esc(timeframe)} • {self.esc(position)} Line*"
        lines = [header, "```"]
        lines.append(f"{'Symbol':<12} {'Distance (%)':>12} {'Daily Move (%)':>14}")
        lines.append("-" * 40)
        for _, row in df.iterrows():
            lines.append(
                f"{row['Symbol']:<12} {row['Distance (%)']:>12} {row['Daily Movement (%)']:>14}"
            )
        lines.append("```")
        return "\n".join(lines)

    def format_enhanced_ema_section(
        self, timeframe: str, df: pd.DataFrame, daily_changes: Dict[str, float]
    ) -> str:
        if df.empty:
            return ""
        df_copy = df.copy()
        df_copy["daily"] = df_copy["symbol"].map(daily_changes)
        df_copy["Score"] = df_copy["breakout_score"].map("{:.1f}".format)
        df_copy["Dist%"] = df_copy["current_distance"].map("{:.1f}".format)
        df_copy["Cons%"] = df_copy["consistency_above"].map(lambda x: f"{x*100:.0f}")
        df_copy["Cross"] = df_copy["recent_cross"].map(lambda x: "✓" if x else "")
        df_copy["Vol"] = df_copy["relative_volume"].map("{:.1f}".format)
        df_copy["Momentum"] = df_copy.apply(
            lambda r: "🚀" if r["bearish_to_bullish"] else "", axis=1
        )
        df_copy["Daily"] = df_copy["daily"].map(
            lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A"
        )
        df_copy["MACD"] = df_copy["macd_bullish"].map(lambda x: "↑" if x else "↓")
        df_copy["EMA"] = df_copy["ema_alignment"].map(lambda x: "✓" if x else "")

        def _consol(row: pd.Series) -> str:
            if row["breakout_potential"]:
                return "⚡"
            if row["is_consolidating"]:
                return "□"
            return ""

        df_copy["Con"] = df_copy.apply(_consol, axis=1)

        header = f"*{self.esc(timeframe)} • Enhanced Breakout Analysis*"
        lines = [header, "```"]
        lines.append(
            f"{'Symbol':<12}{'Score':>6}{'Dist%':>6}{'Cons%':>6}"
            f"{'Cross':>6}{'MACD':>6}{'Vol':>6}{'EMA':>5}"
            f"{'Mom':>4}{'Con':>4}{'Daily':>9}"
        )
        lines.append("-" * 71)
        for _, row in df_copy.iterrows():
            lines.append(
                f"{row['symbol']:<12}{row['Score']:>6}{row['Dist%']:>6}{row['Cons%']:>6}"
                f"{row['Cross']:>6}{row['MACD']:>6}{row['Vol']:>6}{row['EMA']:>5}"
                f"{row['Momentum']:>4}{row['Con']:>4}{row['Daily']:>9}"
            )
        lines.append("```")
        return "\n".join(lines)

    def format_ohlc_section(self, timeframe: str, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        header = f"{self.esc(timeframe)} OHLC Projections Alerts"
        lines = [header, "```"]
        lines.append(f"{'Symbol':<12} {'Level':<8} {'Dist %':>8}")
        lines.append("-" * 30)
        for _, row in df.iterrows():
            lines.append(f"{row['symbol']:<12} {row['level']:<8} {row['pct_dist']:>8.2f}")
        lines.append("```")
        return "\n".join(lines)


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================
_shutdown_event = asyncio.Event()


def _signal_handler(sig: int) -> None:
    logging.warning("Received signal %d, initiating graceful shutdown...", sig)
    _shutdown_event.set()


async def run() -> None:
    setup_logging()
    cfg = Config()

    connector = aiohttp.TCPConnector(
        limit=200,
        limit_per_host=50,
        ttl_dns_cache=300,
        use_dns_cache=True,
        force_close=False,
        enable_cleanup_closed=True,
    )
    timeout = aiohttp.ClientTimeout(total=30, connect=10)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        proxies = RobustProxyPool(
            max_pool_size=cfg.proxy_max_pool,
            min_pool_size=cfg.proxy_min_pool,
            validation_concurrency=cfg.proxy_validation_concurrency,
            refresh_interval=cfg.proxy_refresh_interval,
        )
        await proxies.initialize(session, cfg.proxy_list_url, cfg.backup_proxy_url)

        scanner = BinanceScanner(session, proxies, concurrency=cfg.fetch_concurrency)
        reporter = Reporter(
            cfg.telegram_bot_token,
            cfg.telegram_chat_id,
            cfg.telegram_channel_username,
            session,
        )
        engine = CalculationEngine(max_workers=cfg.calc_workers)

        try:
            perps, spots = await scanner.get_all_symbols()
        except Exception as exc:
            logging.error("Failed to fetch symbols: %s", exc)
            await proxies.shutdown()
            return

        all_syms = sorted(list((perps | spots) - IGNORED_SYMBOLS))
        logging.info("Total symbols to scan after filtering: %d", len(all_syms))

        try:
            daily = await scanner.fetch_24h_changes()
        except Exception as exc:
            logging.error("Failed to fetch 24h changes: %s", exc)
            daily = {}

        # ------------------------------------------------------------------
        # Timeframe scan loop
        # ------------------------------------------------------------------
        for tf in ALL_TIMEFRAMES:
            if _shutdown_event.is_set():
                logging.info("Shutdown requested, stopping scan loop.")
                break

            logging.info("Scanning timeframe %s", tf)
            enhanced_results: List[Dict[str, Any]] = []
            traditional_results: List[Dict[str, Any]] = []

            async def _process_symbol(sym: str) -> None:
                if _shutdown_event.is_set():
                    return
                market = "perp" if sym in perps else "spot"
                try:
                    df = await scanner.fetch_ohlcv(sym, tf, market, 200)
                    if df is None or df.empty:
                        return
                except Exception as exc:
                    logging.debug("Fetch failed for %s %s: %s", sym, tf, exc)
                    return

                # Traditional (unfiltered) EMA distance
                try:
                    simple = await engine.simple_ema(df)
                    if simple:
                        simple["symbol"] = sym
                        traditional_results.append(simple)
                except Exception as exc:
                    logging.debug("Simple EMA calc failed for %s: %s", sym, exc)

                # Enhanced breakout analysis
                try:
                    enhanced = await engine.enhanced_ema(df, cfg)
                    if enhanced:
                        enhanced["symbol"] = sym
                        enhanced_results.append(enhanced)
                except Exception as exc:
                    logging.debug("Enhanced EMA calc failed for %s: %s", sym, exc)

            tasks = [asyncio.create_task(_process_symbol(s)) for s in all_syms]
            for coro in tqdm.as_completed(tasks, desc=f"Scanning {tf}", total=len(tasks)):
                try:
                    await coro
                except Exception:
                    pass

            logging.info(
                "%s complete | Traditional: %d | Enhanced: %d",
                tf, len(traditional_results), len(enhanced_results),
            )

            # ---- Traditional Above/Below Reports ----
            if traditional_results:
                trad_df = pd.DataFrame(traditional_results)
                above, below = build_top_sections(trad_df, daily)
                parts = []
                if not above.empty:
                    parts.append(reporter.format_section(tf, "Above", above))
                if not below.empty:
                    parts.append(reporter.format_section(tf, "Below", below))
                if parts:
                    try:
                        await reporter.send("\n\n".join(parts))
                        logging.info("Sent traditional EMA report for %s", tf)
                    except Exception as exc:
                        logging.error("Failed to send traditional report: %s", exc)
                await asyncio.sleep(1)

            # ---- Enhanced Breakout Report ----
            if tf in ENHANCED_TIMEFRAMES and enhanced_results:
                enh_df = pd.DataFrame(enhanced_results)
                top = enh_df[enh_df["breakout_score"] >= cfg.min_breakout_score].sort_values(
                    "breakout_score", ascending=False
                ).head(20)
                if not top.empty:
                    msg = reporter.format_enhanced_ema_section(tf, top, daily)
                    try:
                        await reporter.send(msg)
                        logging.info("Sent enhanced breakout report for %s", tf)
                    except Exception as exc:
                        logging.error("Failed to send enhanced report: %s", exc)
                else:
                    logging.info("No enhanced signals above threshold for %s", tf)
                await asyncio.sleep(1)

            await asyncio.sleep(1)

        # ------------------------------------------------------------------
        # OHLC Projections
        # ------------------------------------------------------------------
        for tf in cfg.ohlc_timeframes:
            if _shutdown_event.is_set():
                break
            logging.info("Scanning OHLC projections for %s", tf)
            ohlc_results: List[Dict[str, Any]] = []

            async def _process_ohlc(sym: str) -> None:
                market = "perp" if sym in perps else "spot"
                try:
                    df = await scanner.fetch_ohlcv(
                        sym, tf, market, cfg.ohlc_lookback + 10
                    )
                    if df is None or df.empty:
                        return
                    projections = await engine.ohlc(df, cfg.ohlc_lookback)
                    if not projections:
                        return
                    close = projections["current_close"]
                    levels = []
                    if cfg.show_d_plus:
                        levels.append(("D+", projections["d_plus"]))
                    if cfg.show_d_minus:
                        levels.append(("D-", projections["d_minus"]))
                    if cfg.show_m_minus:
                        levels.append(("M-", projections["m_minus"]))
                    if cfg.show_m_plus:
                        levels.append(("M+", projections["m_plus"]))
                    for name, value in levels:
                        if value > 0:
                            pct_dist = abs((close - value) / value * 100)
                            if pct_dist <= cfg.ohlc_alert_threshold:
                                ohlc_results.append({
                                    "symbol": sym,
                                    "level": name,
                                    "pct_dist": pct_dist,
                                })
                except Exception as exc:
                    logging.debug("OHLC calc failed for %s %s: %s", sym, tf, exc)

            tasks = [asyncio.create_task(_process_ohlc(s)) for s in all_syms]
            for coro in tqdm.as_completed(tasks, desc=f"OHLC {tf}", total=len(tasks)):
                try:
                    await coro
                except Exception:
                    pass

            if ohlc_results:
                results_df = pd.DataFrame(ohlc_results).sort_values("pct_dist").head(40)
                msg = reporter.format_ohlc_section(tf, results_df)
                try:
                    await reporter.send(msg)
                    logging.info("Sent OHLC projection report for %s", tf)
                except Exception as exc:
                    logging.error("Failed to send OHLC report: %s", exc)
            else:
                logging.info("No OHLC alerts for %s", tf)
            await asyncio.sleep(1)

        # ------------------------------------------------------------------
        # Cleanup
        # ------------------------------------------------------------------
        engine.shutdown()
        await proxies.shutdown()
        logging.info("Scan complete. Requests: %d | Errors: %d", scanner._request_count, scanner._error_count)


async def main() -> None:
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: _signal_handler(s))
    try:
        await run()
    except Exception as exc:
        logging.critical("Fatal error in main: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
