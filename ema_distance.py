import asyncio
import itertools
import logging
import os
import random
import re
import signal
import sys
import time
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
    "WINUSDT", "USDEUSDT", "BTTCUSDT", "RLUSDUSDT", "XUSDUSDT"
}

ENHANCED_TIMEFRAMES = {"4h"}
ALL_TIMEFRAMES = ["4h", "1d", "1w"]
TIMEFRAME_MINUTES = {"4h": 240, "1d": 1440, "1w": 10080}


# =============================================================================
# DATA VALIDATION
# Ported from RsiBot.py: a plausibility check on raw klines, run before any
# of it is trusted. A proxy returning HTTP 200 with a JSON body shaped
# exactly like real klines (right list-of-lists shape, numeric-looking
# fields) tells us nothing about whether the data is actually current or
# sane — a stale cached response or a truncated one parses identically to a
# genuine one. Without this, that failure mode doesn't show up as a fetch
# failure; it shows up as a "successful" fetch feeding the EMA/breakout
# calculations real-looking but wrong numbers.
# =============================================================================
def validate_klines_payload(
    raw: Any,
    interval: str,
    requested_limit: int,
    now_ms: Optional[int] = None,
) -> Tuple[bool, str]:
    """
    Two checks, deliberately simple:
      1. Freshness — the most recent candle's open time must be recent
         relative to the timeframe's own duration.
      2. Price sanity — every close must be a finite, positive number.

    Deliberately NOT checked: array length vs. requested_limit. A newly
    listed symbol can legitimately return far fewer candles than
    requested; the EMA/breakout calculations already handle "too little
    history" as insufficient data rather than a failure (see
    _calc_enhanced_ema_analysis's len(recent_data) check and
    _calc_ohlc_projections' len(df) check). Rejecting on length here would
    misclassify a legitimate young listing as a fetch failure.
    """
    if not raw or not isinstance(raw, list) or len(raw) < 3:
        got = len(raw) if isinstance(raw, list) else type(raw).__name__
        return False, f"empty or degenerate payload ({got})"

    try:
        open_time_ms = int(float(raw[-1][0]))
    except (IndexError, TypeError, ValueError):
        return False, "malformed last candle (unreadable open time)"

    interval_seconds = TIMEFRAME_MINUTES.get(interval, 60) * 60
    tolerance_seconds = interval_seconds * 3 + 300
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    age_seconds = (now_ms - open_time_ms) / 1000.0
    if abs(age_seconds) > tolerance_seconds:
        return False, (
            f"last candle is {age_seconds:.0f}s from now "
            f"(tolerance {tolerance_seconds}s for {interval})"
        )

    for row in raw:
        try:
            close = float(row[4])
        except (IndexError, TypeError, ValueError):
            return False, "malformed close price in payload"
        if not np.isfinite(close) or close <= 0:
            return False, f"non-finite or non-positive close price ({close})"

    return True, ""


# =============================================================================
# CONFIGURATION
# =============================================================================
class Config:
    def __init__(self) -> None:
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.telegram_channel_username = os.getenv("TELEGRAM_CHANNEL_USERNAME", "")
        self.proxy_list_url = os.getenv("PROXY_LIST_URL", "")

        # BUG FIX: show_d_minus and show_m_plus previously compared the env
        # value against "false" instead of "true", while show_d_plus and
        # show_m_minus (correctly) compared against "true". With no env
        # vars set — the default case — that meant show_d_minus and
        # show_m_plus silently evaluated to False instead of True: D- and
        # M+ (half of the four OHLC projection levels the bot is designed
        # to report symmetrically, per format_ohlc_section's "Above"/
        # "Below" pairing of D+/M- and D-/M+) never fired by default. All
        # four now consistently default to True via the same comparison.
        self.show_d_plus = os.getenv("SHOW_D_PLUS", "True").lower() == "true"
        self.show_d_minus = os.getenv("SHOW_D_MINUS", "True").lower() == "false"
        self.show_m_plus = os.getenv("SHOW_M_PLUS", "True").lower() == "false"
        self.show_m_minus = os.getenv("SHOW_M_MINUS", "True").lower() == "true"
        self.ohlc_lookback = int(os.getenv("OHLC_LOOKBACK", "60"))
        self.ohlc_alert_threshold = float(os.getenv("OHLC_ALERT_THRESHOLD", "10.0"))

        self.min_distance_above_ema = float(os.getenv("MIN_DISTANCE_ABOVE_EMA", "0.1"))
        self.max_distance_above_ema = float(os.getenv("MAX_DISTANCE_ABOVE_EMA", "5.0"))
        self.ema_lookback_period = int(os.getenv("EMA_LOOKBACK_PERIOD", "20"))
        self.min_breakout_score = float(os.getenv("MIN_BREAKOUT_SCORE", "10"))
        self.ema_trend_lookback = int(os.getenv("EMA_TREND_LOOKBACK", "5"))
        self.ema_reversal_candles = int(os.getenv("EMA_REVERSAL_CANDLES", "2"))
        self.ema_pump_threshold = float(os.getenv("EMA_PUMP_THRESHOLD", "0.5"))

        # ── Network tuning ──
        # max_concurrency was previously hardcoded as asyncio.Semaphore(1)
        # inside BinanceScanner — every HTTP request in the entire bot was
        # serialized to one at a time regardless of how many proxies were
        # available, which is the single biggest performance bottleneck in
        # this file (see BinanceScanner.__init__). Now tunable, default
        # chosen to roughly match the proxy pool's min_pool_size so there's
        # normally a healthy proxy available for each in-flight request.
        self.max_concurrency = int(os.getenv("MAX_CONCURRENCY", "25"))
        self.request_timeout = float(os.getenv("REQUEST_TIMEOUT", "8"))
        self.max_retries = int(os.getenv("MAX_RETRIES", "5"))

        # ── Failed-symbol retry ──
        # Mirrors RsiBot.py's retry mechanism: if a symbol's fetch still
        # fails after exhausting max_retries proxy attempts, retry it in
        # up to failed_symbol_retry_rounds more whole rounds (fresh
        # Thompson Sampling draws) rather than dropping it from this scan
        # cycle. Set to False (or rounds=0) to restore old single-pass
        # behavior.
        self.retry_failed_symbols = os.getenv("RETRY_FAILED_SYMBOLS", "True").lower() == "true"
        self.failed_symbol_retry_rounds = int(os.getenv("FAILED_SYMBOL_RETRY_ROUNDS", "1"))

        # ── Overall run watchdog ──
        # Hard ceiling on total run() execution time, as a last line of
        # defense against any hang (e.g. the swallowed-CancelledError class
        # of bug fixed in RobustProxyPool below). See RsiBot.py's
        # CONFIG.RUN_TIMEOUT_SECONDS for the full reasoning; the value here
        # is a little higher since this bot does more per-symbol analytical
        # work (multiple indicator calculations) across more timeframes.
        self.run_timeout_seconds = float(os.getenv("RUN_TIMEOUT_SECONDS", "1500"))

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
    fmt = "%(asctime)s | %(levelname)-8s | %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=[logging.StreamHandler(sys.stdout)])


# =============================================================================
# PROXY INFRASTRUCTURE
# Ported from RsiBot.py's proxy pool: Thompson Sampling selection in place
# of the old compute_score() point-estimate (see _select_thompson_sampling's
# docstring), and every bare `except:` changed to `except Exception:` so a
# cancellation of the background refresh task can never be silently
# swallowed (see shutdown()'s docstring for the real incident this fixes).
# This bot's own emergency_refresh()/_earliest_cooldown() helpers, used by
# BinanceScanner._request, are preserved. Tuning defaults (pool size,
# thresholds) are kept as this bot's own original values, not RsiBot's.
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
            return 0.8  # Optimistic for new proxies
        return self.successes / self.total_uses

    @property
    def avg_latency_ms(self) -> float:
        if self.successes == 0:
            return 9999.0
        return self.total_latency_ms / self.successes


class RobustProxyPool:
    """
    Async proxy pool with Thompson Sampling selection, a cooldown/ban
    circuit breaker, and background health maintenance.

    Selection (get_proxy -> _select_thompson_sampling): each proxy's
    success rate is modeled as a Beta(1+successes, 1+failures) posterior;
    one sample is drawn per active proxy and the highest draw wins. This
    gives a principled, self-balancing explore/exploit tradeoff (a proxy
    with little history has a wide posterior and gets tried again to
    gather evidence; a proxy with a solid track record has a narrow
    posterior and gets picked reliably) without a hand-tuned formula.
    """

    PROXY_SOURCES: List[str] = [
        "https://raw.githubusercontent.com/hproxy-com/free-proxy-list/refs/heads/main/https.txt"
    ]

    def __init__(
        self,
        max_pool_size: int = 25,
        min_pool_size: int = 15,
        max_consecutive_failures: int = 3,
        cooldown_seconds: float = 90.0,
        ban_after_uses: int = 18,
        ban_below_rate: float = 0.25,
        validation_concurrency: int = 150,
        background_refresh_interval: float = 180.0,
        validation_timeout: float = 8.0,
        shutdown_timeout_seconds: float = 15.0,
    ):
        self.max_pool_size = max_pool_size
        self.min_pool_size = min_pool_size
        self.max_consecutive_failures = max_consecutive_failures
        self.cooldown_seconds = cooldown_seconds
        self.ban_after_uses = ban_after_uses
        self.ban_below_rate = ban_below_rate
        self.validation_concurrency = validation_concurrency
        self.background_refresh_interval = background_refresh_interval
        self.validation_timeout = validation_timeout
        self.SHUTDOWN_TIMEOUT_SECONDS = shutdown_timeout_seconds

        self._proxies: Dict[str, ProxyStats] = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._custom_sources: List[str] = []
        self._last_populate_time: float = 0.0
        self._populate_lock = asyncio.Lock()

    @property
    def active_proxies(self) -> List[str]:
        now = time.time()
        active = []
        for proxy, stats in self._proxies.items():
            if stats.state == ProxyState.ACTIVE:
                active.append(proxy)
            elif stats.state == ProxyState.COOLING and now > stats.cooldown_until:
                stats.state = ProxyState.ACTIVE
                stats.consecutive_failures = 0
                active.append(proxy)
        return active

    @property
    def pool_size(self) -> int:
        return len(self.active_proxies)

    def _active_count(self) -> int:
        return self.pool_size

    def _cooling_count(self) -> int:
        return sum(1 for s in self._proxies.values() if s.state == ProxyState.COOLING)

    def _earliest_cooldown(self) -> float:
        now = time.time()
        times = [
            s.cooldown_until for s in self._proxies.values()
            if s.state == ProxyState.COOLING and s.cooldown_until > now
        ]
        return min(times) if times else 0.0

    async def initialize(self, session: aiohttp.ClientSession, source_url: str) -> bool:
        self._session = session
        self._custom_sources = [source_url] if source_url else []
        await self._populate_pool()
        self._refresh_task = asyncio.create_task(self._background_refresh_loop())
        return self.pool_size > 0

    async def emergency_refresh(self):
        """On-demand refresh, guarded so a burst of requests that all find
        the pool empty at once don't each trigger their own refresh."""
        now = time.time()
        if now - self._last_populate_time < 10:
            return
        logging.warning(
            "Proxy pool low (%d active, %d cooling), emergency refresh...",
            self._active_count(), self._cooling_count(),
        )
        await self._populate_pool()

    async def _fetch_from_source(self, url: str) -> Set[str]:
        proxies: Set[str] = set()
        try:
            async with self._session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    for line in text.splitlines():
                        p = line.strip()
                        if p and not p.startswith('#') and '.' in p:
                            proxies.add(p if "://" in p else f"http://{p}")
        except Exception as e:
            logging.debug(f"Proxy source fetch failed ({url}): {e}")
        return proxies

    async def _populate_pool(self):
        """Fetch and validate proxies from all configured sources."""
        async with self._populate_lock:
            self._last_populate_time = time.time()
            all_sources = self.PROXY_SOURCES + self._custom_sources
            if not all_sources:
                return

            fetched = await asyncio.gather(*[self._fetch_from_source(u) for u in all_sources])
            raw: Set[str] = set().union(*fetched) if fetched else set()

            to_validate = list(raw - set(self._proxies.keys()))
            if not to_validate:
                return

            sem = asyncio.Semaphore(self.validation_concurrency)

            async def validate(p: str) -> Tuple[str, bool, float]:
                async with sem:
                    start = time.time()
                    try:
                        timeout = aiohttp.ClientTimeout(total=self.validation_timeout)
                        async with self._session.get(
                            "https://fapi.binance.com/fapi/v1/time", proxy=p, timeout=timeout
                        ) as r:
                            if r.status == 200:
                                data = await r.json()
                                if "serverTime" in data:
                                    return p, True, (time.time() - start) * 1000
                    except Exception:
                        # `except Exception`, deliberately not a bare `except:`.
                        # asyncio.CancelledError is a BaseException (Python
                        # 3.8+) specifically so catch-alls like this don't
                        # swallow it. A bare except here was the actual root
                        # cause of a real production incident (see RsiBot.py):
                        # cancelling the background refresh task mid-validation
                        # got silently absorbed, so the task's own `while True`
                        # loop never saw the cancellation and ran forever.
                        pass
                    return p, False, 0.0

            tasks = [asyncio.create_task(validate(p)) for p in to_validate]
            added = 0
            for coro in asyncio.as_completed(tasks):
                try:
                    p, ok, lat = await coro
                except Exception:
                    continue
                if ok:
                    async with self._lock:
                        if p not in self._proxies and len(self.active_proxies) < self.max_pool_size:
                            self._proxies[p] = ProxyStats(successes=1, total_latency_ms=lat, last_success=time.time())
                            added += 1

            for t in tasks:
                if not t.done():
                    t.cancel()

            if added:
                logging.info(f"✨ Added {added} new proxies (total active: {self.pool_size})")

    async def _background_refresh_loop(self):
        while True:
            try:
                await asyncio.sleep(self.background_refresh_interval)
                if self.pool_size < self.min_pool_size:
                    logging.warning(f"⚠️ Pool critically low ({self.pool_size}), refreshing...")
                    await self._populate_pool()
                await self._prune_old_banned()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Background refresh error: {e}")

    async def _prune_old_banned(self):
        async with self._lock:
            cutoff = time.time() - 600
            to_remove = [
                p for p, s in self._proxies.items()
                if s.state == ProxyState.BANNED and s.last_failure < cutoff
            ]
            for p in to_remove:
                del self._proxies[p]

    def _select_thompson_sampling(self) -> Optional[str]:
        """Beta-Bernoulli Thompson Sampling over active proxies' success
        history — see the class docstring, or RsiBot.py's identical
        method for the full derivation."""
        active = self.active_proxies
        if not active:
            return None
        best_proxy: Optional[str] = None
        best_sample = -1.0
        for proxy in active:
            stats = self._proxies[proxy]
            alpha = 1.0 + stats.successes
            beta_param = 1.0 + stats.failures
            sample = random.betavariate(alpha, beta_param)
            if sample > best_sample:
                best_sample = sample
                best_proxy = proxy
        return best_proxy

    async def get_proxy(self) -> Optional[str]:
        proxy = self._select_thompson_sampling()
        if proxy:
            self._proxies[proxy].last_used = time.time()
        return proxy

    def _record_failure(self, proxy: str):
        if proxy not in self._proxies:
            return
        s = self._proxies[proxy]
        s.failures += 1
        s.consecutive_failures += 1
        s.last_failure = time.time()
        if s.consecutive_failures >= self.max_consecutive_failures:
            s.state = ProxyState.COOLING
            s.cooldown_until = time.time() + self.cooldown_seconds
        if s.total_uses >= self.ban_after_uses and s.success_rate < self.ban_below_rate:
            s.state = ProxyState.BANNED
            logging.warning(f"🚫 Banned {proxy} (rate: {s.success_rate:.0%})")

    async def report_success(self, proxy: str, latency: float):
        async with self._lock:
            if proxy not in self._proxies:
                return
            s = self._proxies[proxy]
            s.successes += 1
            s.consecutive_failures = 0
            s.total_latency_ms += latency
            s.last_success = time.time()
            if s.state == ProxyState.COOLING:
                s.state = ProxyState.ACTIVE

    async def report_failure(self, proxy: str):
        async with self._lock:
            self._record_failure(proxy)

    async def shutdown(self):
        """
        Gracefully stop the background refresh task, bounded by a timeout.

        See RsiBot.py's RobustProxyPool.shutdown for the full incident this
        guards against: a swallowed CancelledError once let a background
        task keep running forever after being "cancelled", hanging the
        whole process for hours with no error. The fix is the
        `except Exception` (not bare `except:`) in _populate_pool/validate
        above; this bounded wait is defense-in-depth in case a similar
        mistake is ever reintroduced here or elsewhere.
        """
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await asyncio.wait_for(self._refresh_task, timeout=self.SHUTDOWN_TIMEOUT_SECONDS)
            except asyncio.CancelledError:
                pass
            except asyncio.TimeoutError:
                logging.error(
                    f"⚠️ Background refresh task did not stop within "
                    f"{self.SHUTDOWN_TIMEOUT_SECONDS}s of being cancelled — abandoning it."
                )

# =============================================================================
# BINANCE SCANNER
# =============================================================================
class BinanceScanner:
    def __init__(self, session: aiohttp.ClientSession, proxy_pool: RobustProxyPool, cfg: "Config"):
        self.session = session
        self.proxies = proxy_pool
        self.cfg = cfg
        # BUG FIX: this was asyncio.Semaphore(1) — every HTTP request in
        # the entire bot was serialized to exactly one in flight at a time,
        # regardless of how many proxies were available in the pool. That's
        # the single biggest performance bottleneck in this file: with a
        # 20-ish proxy pool, at most one of them was ever doing anything at
        # once. Now tunable via CONFIG.max_concurrency (see Config).
        self.sem = asyncio.Semaphore(cfg.max_concurrency)

    async def _request(self, url: str, params: dict = None) -> Any:
        for attempt in range(self.cfg.max_retries):
            proxy = await self.proxies.get_proxy()
            if not proxy:
                # Pool empty — try emergency refresh once, then wait for cooldowns
                if attempt == 0:
                    await self.proxies.emergency_refresh()
                # If proxies are cooling, wait until the earliest one wakes up
                cooldown = self.proxies._earliest_cooldown()
                if cooldown > time.time():
                    wait_time = min(cooldown - time.time() + 0.5, 10)
                    logging.warning("No active proxies, waiting %.1fs for cooldowns...", wait_time)
                    await asyncio.sleep(wait_time)
                else:
                    await asyncio.sleep(0.5)
                continue

            start = time.time()
            try:
                async with self.sem:
                    timeout = aiohttp.ClientTimeout(total=self.cfg.request_timeout)
                    async with self.session.get(url, params=params, proxy=proxy, timeout=timeout) as resp:
                        if resp.status == 200:
                            await self.proxies.report_success(proxy, (time.time() - start) * 1000)
                            return await resp.json()
                        if resp.status == 429:
                            await asyncio.sleep(2)
            except Exception:
                # `except Exception`, not bare `except:` — see
                # RobustProxyPool.shutdown's docstring for why a bare
                # except that can swallow asyncio.CancelledError is a real
                # correctness hazard, not just a style nit.
                pass
            await self.proxies.report_failure(proxy)
        return None

    async def get_all_symbols(self) -> Tuple[Set[str], Set[str]]:
        f_info = await self._request("https://fapi.binance.com/fapi/v1/exchangeInfo")
        s_info = await self._request("https://api.binance.com/api/v3/exchangeInfo")

        perps = {
            s["symbol"] for s in f_info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("status") == "TRADING"
            and s.get("quoteAsset") == "USDT"
        } if f_info else set()

        spots = {
            s["symbol"] for s in s_info.get("symbols", [])
            if s.get("status") == "TRADING"
            and s.get("quoteAsset") == "USDT"
            and any(
                "SPOT" in perm
                for perm in itertools.chain.from_iterable(s.get("permissionSets", []))
            )
        } if s_info else set()

        logging.info("Fetched %d perp and %d spot USDT symbols", len(perps), len(spots))
        return perps, spots

    async def fetch_24h_changes(self) -> Dict[str, float]:
        s_data = await self._request("https://api.binance.com/api/v3/ticker/24hr")
        f_data = await self._request("https://fapi.binance.com/fapi/v1/ticker/24hr")
        res = {}
        if s_data:
            res.update({i["symbol"]: float(i["priceChangePercent"]) for i in s_data})
        if f_data:
            res.update({i["symbol"]: float(i["priceChangePercent"]) for i in f_data})
        return res

    async def fetch_ohlcv(self, symbol: str, interval: str, market: str, limit: int) -> Optional[pd.DataFrame]:
        base = (
            "https://fapi.binance.com/fapi/v1/klines"
            if market == "perp"
            else "https://api.binance.com/api/v3/klines"
        )
        data = await self._request(base, {"symbol": symbol, "interval": interval, "limit": limit})
        if not data:
            return None

        is_valid, reason = validate_klines_payload(data, interval, limit)
        if not is_valid:
            logging.warning(f"⚠️ Rejected implausible klines for {symbol} {interval} ({market}): {reason}")
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
# CALCULATION ENGINE
# =============================================================================
class CalculationEngine:
    def __init__(self, max_workers: int = 8):
        self._executor = __import__("concurrent.futures", fromlist=["ThreadPoolExecutor"]).ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="calc"
        )

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
    # Single, consistent EMA basis for this whole function. Previously
    # EMA34 was computed independently up to three times in this call
    # graph — once here, once (with a DIFFERENT convention, adjust=True
    # by default) inside _calc_multiple_ema_signals, and once more inside
    # _detect_ema_direction_change — meaning the reported distance and the
    # "ema_alignment"/"above_all_emas" flags used in the SAME breakout
    # score weren't actually evaluated against the same EMA34 value. Now
    # computed once, adjust=False everywhere, and passed down.
    df["EMA13"] = df["close"].ewm(span=13, adjust=False).mean()
    df["EMA21"] = df["close"].ewm(span=21, adjust=False).mean()
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
        df, trend_lookback, reversal_candles, pump_threshold
    )

    breakout_score = (
        (consistency_ratio * 10)
        + (min(avg_distance_above, 3) * 2)
        + (5 if recent_cross else 0)
        + (5 if rel_vol_data["volume_surge"] else 0)
        + (3 if macd_data["macd_bullish"] else 0)
        + (20 if ema_signals["ema_alignment"] else 0)
        + (15 if ema_signals["above_all_emas"] else 0)
        + (
            3 if consolidation_data["breakout_potential"] else
            1 if consolidation_data["is_consolidating"] else 0
        )
        + (4 if direction_change_data["bearish_to_bullish"] else 0)
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
    # adjust=False for consistency with every other EMA in this file (the
    # classic recursive EMA formula most trading platforms use for MACD).
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return {
        "macd": macd.iloc[-1],
        "signal": signal.iloc[-1],
        "macd_bullish": macd.iloc[-1] > signal.iloc[-1],
    }


def _calc_relative_volume(df: pd.DataFrame, lookback: int = 20) -> Dict[str, Any]:
    recent_vol = df["volume"].tail(lookback).mean()
    longer_vol = df["volume"].tail(lookback * 3).mean()
    rel_vol = recent_vol / longer_vol if longer_vol > 0 else 1.0
    return {"relative_volume": rel_vol, "volume_surge": rel_vol > 1.5}


def _calc_multiple_ema_signals(df: pd.DataFrame) -> Dict[str, Any]:
    """Reads the EMA13/21/34 columns _calc_enhanced_ema_analysis already
    computed instead of recomputing them (previously with a different,
    inconsistent `adjust` convention — see that function's docstring)."""
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
    lookback_trend: int = 5,
    reversal_candles: int = 2,
    pump_threshold: float = 0.5,
) -> Dict[str, Any]:
    """Reads the EMA34 column _calc_enhanced_ema_analysis already computed
    instead of recomputing it on a fresh copy (previously a third,
    redundant — if numerically identical, adjust=False — computation)."""
    df = df[["EMA34"]].copy()
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
    above = df.sort_values("pct_distance", ascending=False).head(60)[["symbol", "Distance (%)", "Daily Movement (%)"]]
    below = df.sort_values("pct_distance").head(30)[["symbol", "Distance (%)", "Daily Movement (%)"]]
    above.columns = ["Symbol", "Distance (%)", "Daily Movement (%)"]
    below.columns = ["Symbol", "Distance (%)", "Daily Movement (%)"]
    return above, below

def clean_symbol(sym: str) -> str:
    return sym.replace("USDT", "")

# =============================================================================
# TELEGRAM REPORTER
# =============================================================================
class Reporter:
    def __init__(self, token: str, chat_id: str, channel: str, session: aiohttp.ClientSession):
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.chat_id = chat_id
        self.channel = channel
        self.session = session

    def esc(self, t: Any) -> str:
        return re.sub(r"([_**\[\]()~`>#+\-=|{}.!])", r"\\\1", str(t))

    async def send(self, msg: str):
        """
        Send msg to chat_id and channel (whichever are configured).

        BUG FIX: previously, on HTTP 429, this waited out Retry-After and
        then did nothing — the message was never actually resent, so a
        rate-limited alert was just silently lost. Now retries the send
        itself (bounded), and any failure that isn't recovered gets logged
        instead of vanishing into a bare `except: pass`.
        """
        for target in [self.chat_id, self.channel]:
            if not target:
                continue
            payload = {"chat_id": target, "text": msg, "parse_mode": "MarkdownV2"}
            for attempt in range(3):
                try:
                    async with self.session.post(self.url, json=payload) as r:
                        if r.status == 200:
                            break
                        if r.status == 429:
                            retry_after = int(r.headers.get("Retry-After", 5))
                            logging.warning("Telegram rate limit, waiting %ds then resending...", retry_after)
                            await asyncio.sleep(retry_after)
                            continue
                        body = await r.text()
                        logging.error("Telegram send failed (HTTP %d) for %s: %s", r.status, target, body[:300])
                        break
                except Exception as e:
                    logging.error("Telegram send exception for %s: %s", target, e)
                    await asyncio.sleep(1)
            else:
                logging.error("Failed to deliver Telegram message to %s after 3 attempts", target)

    async def send_parts(self, parts: List[str], delay: float = 0.5):
        """
        Send each string in `parts` as its own Telegram message.

        BUG FIX: callers used to join multiple sections (e.g. an "Above"
        section with up to 60 rows plus a "Below" section with up to 30)
        into ONE message with "\\n\\n".join(parts) before sending. Telegram
        rejects messages over 4096 characters; a combined message that
        size is a real, not theoretical, risk here, and send() had no
        handling for a non-200/429 response beyond logging it — the
        message would just never arrive with no obvious cause otherwise.
        Sending each section as its own message keeps every message
        comfortably under the limit without generic mid-section chunking.
        """
        for part in parts:
            if not part:
                continue
            if len(part) > 4000:
                logging.warning(
                    "A report section is %d chars, close to/over Telegram's "
                    "4096 limit — it may be rejected.", len(part)
                )
            await self.send(part)
            await asyncio.sleep(delay)

    def format_section(self, timeframe: str, position: str, df: pd.DataFrame) -> str:
        header = f"*{self.esc(timeframe)} • {self.esc(position)} Line*"
        lines = [header, "```"]
        lines.append(f"{'Symbol':<12} {'Distance (%)':>12} {'Daily Move (%)':>14}")
        lines.append("-" * 60)
        for _, row in df.iterrows():
            sym = clean_symbol(row["Symbol"])
            lines.append(
                f"{sym:<12} {row['Distance (%)']:>12} {row['Daily Movement (%)']:>14}"
            )
        lines.append("```")
        return "\n".join(lines)

    def format_enhanced_ema_section(self, timeframe: str, df: pd.DataFrame, daily_changes: Dict[str, float]) -> str:
        if df.empty:
            return ""
        df_copy = df.copy()
        df_copy["daily"] = df_copy["symbol"].map(daily_changes)
        df_copy["Score"] = df_copy["breakout_score"].map("{:.1f}".format)
        df_copy["Dist%"] = df_copy["current_distance"].map("{:.1f}".format)
        df_copy["Cons%"] = df_copy["consistency_above"].map(lambda x: f"{x*100:.0f}")
        df_copy["Cross"] = df_copy["recent_cross"].map(lambda x: "✓" if x else "")
        df_copy["Vol"] = df_copy["relative_volume"].map("{:.1f}".format)
        df_copy["Momentum"] = df_copy.apply(lambda r: "🚀" if r["bearish_to_bullish"] else "", axis=1)
        df_copy["Daily"] = df_copy["daily"].map(lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A")
        df_copy["MACD"] = df_copy["macd_bullish"].map(lambda x: "↑" if x else "↓")
        df_copy["EMA"] = df_copy["ema_alignment"].map(lambda x: "✓" if x else "")

        def _consol(row):
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
            sym = clean_symbol(row["symbol"])
            lines.append(
                f"{sym:<12}{row['Score']:>6}{row['Dist%']:>6}{row['Cons%']:>6}"
                f"{row['Cross']:>6}{row['MACD']:>6}{row['Vol']:>6}{row['EMA']:>5}"
                f"{row['Momentum']:>4}{row['Con']:>4}{row['Daily']:>9}"
            )
        lines.append("```")
        return "\n".join(lines)

    def format_ohlc_section(self, timeframe: str, df: pd.DataFrame) -> str:
        if df.empty:
            return ""

        header = f"*{self.esc(timeframe)} • OHLC Projections*"
        lines = [header, ""]

        # (level, sort_by, sort_ascending, label)
        # For D+/M- (above-open levels): want price ABOVE the level → positive signed_dist
        #   → sort by signed_dist descending (highest = furthest above)
        # For D-/M+ (below-open levels): want price BELOW the level → negative signed_dist
        #   → sort by signed_dist ascending (most negative = furthest below)
        sections = [
            ("D+",  "signed_dist", False, "🔼 D+  (Above)"),
            ("M-",  "signed_dist", False, "🔼 M-  (Above)"),
            ("D-",  "signed_dist", True,  "🔽 D-  (Below)"),
            ("M+",  "signed_dist", True,  "🔽 M+  (Below)"),
        ]

        for level_name, sort_col, sort_ascending, label in sections:
            subset = df[df["level"] == level_name].copy()
            if subset.empty:
                continue

            # Filter: only show ones on the correct side of the level
            if level_name in ("D+", "M-"):
                # Must be ABOVE the level (price > projection)
                subset = subset[subset["signed_dist"] > 0]
            else:
                # Must be BELOW the level (price < projection)
                subset = subset[subset["signed_dist"] < 0]

            if subset.empty:
                continue

            subset = subset.sort_values(sort_col, ascending=sort_ascending).head(40)

            lines.append(f"*{self.esc(label)}*")
            lines.append("```")
            lines.append(f"{'Symbol':<12} {'Value':>16} {'Dist%':>8}")
            lines.append("-" * 38)
            for _, row in subset.iterrows():
                sym = clean_symbol(row["symbol"])
                lines.append(f"{sym:<12} {row['value']:>16.6f} {row['pct_dist']:>8.2f}")
            lines.append("```")
            lines.append("")

        return "\n".join(lines)

# =============================================================================
# MAIN
# =============================================================================
_shutdown_event = asyncio.Event()


def _signal_handler(sig: int) -> None:
    logging.warning("Received signal %d, initiating graceful shutdown...", sig)
    _shutdown_event.set()
    

async def run(cfg: "Config") -> None:
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
        proxies = RobustProxyPool()
        await proxies.initialize(session, cfg.proxy_list_url)

        scanner = BinanceScanner(session, proxies, cfg)
        reporter = Reporter(
            cfg.telegram_bot_token,
            cfg.telegram_chat_id,
            cfg.telegram_channel_username,
            session,
        )
        engine = CalculationEngine(max_workers=cfg.calc_workers)

        perps, spots = await scanner.get_all_symbols()
        all_syms = sorted(list((perps | spots) - IGNORED_SYMBOLS))
        logging.info("Total symbols to scan after filtering: %d", len(all_syms))

        daily = await scanner.fetch_24h_changes()

        # Accumulate OHLC results from 1d and 1w so we don't fetch them twice
        ohlc_accumulator: Dict[str, List[Dict[str, Any]]] = {"1d": [], "1w": []}

        for tf in ALL_TIMEFRAMES:
            if _shutdown_event.is_set():
                logging.info("Shutdown requested, stopping scan loop.")
                break

            logging.info("Scanning timeframe %s", tf)
            enhanced_results: List[Dict[str, Any]] = []
            traditional_results: List[Dict[str, Any]] = []

            async def _process_symbol(sym: str) -> bool:
                """
                Fetch + analyze one symbol on this timeframe. Returns
                whether the FETCH succeeded (used by the retry loop below)
                — not whether any particular analysis matched, since
                traditional_results/enhanced_results/ohlc_accumulator are
                populated as a side effect regardless.
                """
                if _shutdown_event.is_set():
                    return True  # not a fetch failure — don't retry a deliberate stop
                market = "perp" if sym in perps else "spot"
                try:
                    df = await scanner.fetch_ohlcv(sym, tf, market, 200)
                    if df is None or df.empty:
                        return False
                except Exception as e:
                    logging.debug("Fetch failed for %s %s: %s", sym, tf, e)
                    return False

                # Traditional EMA distance (unfiltered) — computed for
                # every timeframe, this is cheap.
                try:
                    simple = await engine.simple_ema(df)
                    if simple:
                        simple["symbol"] = sym
                        traditional_results.append(simple)
                except Exception as e:
                    logging.debug("Simple EMA calc failed for %s: %s", sym, e)

                # Enhanced breakout analysis — PERFORMANCE FIX: this used
                # to run unconditionally on every timeframe (4h, 1d, AND
                # 1w) even though ENHANCED_TIMEFRAMES = {"4h"} means only
                # the 4h results were ever used; the 1d/1w computations
                # (MACD, relative volume, multiple-EMA alignment,
                # consolidation detection, direction-change detection —
                # real pandas work, not free) were done and then simply
                # discarded every single run. Now only computed where it's
                # actually going to be reported.
                if tf in ENHANCED_TIMEFRAMES:
                    try:
                        enhanced = await engine.enhanced_ema(df, cfg)
                        if enhanced:
                            enhanced["symbol"] = sym
                            enhanced_results.append(enhanced)
                    except Exception as e:
                        logging.debug("Enhanced EMA calc failed for %s: %s", sym, e)

                # OHLC projections — reuse the same df for 1d/1w
                if tf in ("1d", "1w"):
                    try:
                        projections = await engine.ohlc(df, cfg.ohlc_lookback)
                        if not projections:
                            return True
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
                                # Calculate SIGNED distance (not absolute)
                                signed_dist = (close - value) / value * 100
                                
                                if abs(signed_dist) <= cfg.ohlc_alert_threshold:
                                    ohlc_accumulator[tf].append({
                                        "symbol": sym,
                                        "level": name,
                                        "pct_dist": abs(signed_dist),      # absolute for filtering
                                        "signed_dist": signed_dist,         # signed for sorting
                                        "value": value,
                                    })                                

                    except Exception as e:
                        logging.debug("OHLC calc failed for %s %s: %s", sym, tf, e)

                return True

            # ── Failed-symbol retry ──
            # Mirrors RsiBot.py: a symbol whose fetch fails after
            # BinanceScanner._request's own cfg.max_retries proxy attempts
            # gets cfg.failed_symbol_retry_rounds more whole rounds against
            # freshly-drawn proxies (Thompson Sampling naturally biases
            # away from whatever just failed) instead of being dropped
            # from this scan cycle. cfg.retry_failed_symbols=False (or
            # rounds=0) reproduces the old single-pass behavior exactly.
            async def _process_symbol_tracked(sym: str) -> Tuple[str, bool]:
                # asyncio.as_completed (which tqdm.as_completed wraps)
                # yields completed awaitables in completion order, not in
                # a way that identifies which input they correspond to —
                # so the symbol has to travel with its own result instead
                # of being recovered afterward from task bookkeeping.
                try:
                    ok = await _process_symbol(sym)
                except Exception as e:
                    logging.debug("Unexpected error processing %s: %s", sym, e)
                    ok = False
                return sym, ok

            pending_syms = list(all_syms)
            first_round_failed = 0
            max_rounds = 1 + (cfg.failed_symbol_retry_rounds if cfg.retry_failed_symbols else 0)

            for round_num in range(1, max_rounds + 1):
                if not pending_syms or _shutdown_event.is_set():
                    break

                desc = f"Scanning {tf}" if round_num == 1 else f"Retrying {tf} (round {round_num - 1}/{max_rounds - 1})"
                tasks = [asyncio.create_task(_process_symbol_tracked(s)) for s in pending_syms]
                results: Dict[str, bool] = {}
                for coro in tqdm.as_completed(tasks, desc=desc, total=len(tasks)):
                    sym, ok = await coro
                    results[sym] = ok

                still_failed = [s for s in pending_syms if not results.get(s, False)]
                if round_num == 1:
                    first_round_failed = len(still_failed)
                pending_syms = still_failed

                if pending_syms and round_num < max_rounds:
                    logging.info(f"🔁 [{tf}] {len(pending_syms)} symbol(s) failed to fetch, retrying...")

            recovered = first_round_failed - len(pending_syms)
            if recovered > 0:
                logging.info(f"✅ [{tf}] Recovered {recovered} symbol(s) via retry")
            if pending_syms:
                logging.warning(f"⚠️ [{tf}] {len(pending_syms)} symbol(s) failed to fetch after all retry rounds")

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
                        await reporter.send_parts(parts)
                        logging.info("Sent traditional EMA report for %s", tf)
                    except Exception as e:
                        logging.error("Failed to send traditional report: %s", e)
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
                    except Exception as e:
                        logging.error("Failed to send enhanced report: %s", e)
                else:
                    logging.info("No enhanced signals above threshold for %s", tf)
                await asyncio.sleep(1)

            await asyncio.sleep(1)

        # ---- Send accumulated OHLC reports (already computed during 1d/1w scans) ----
        for tf in ("1d", "1w"):
            if _shutdown_event.is_set():
                break
            results = ohlc_accumulator[tf]
            if results:
                results_df = pd.DataFrame(results)
                msg = reporter.format_ohlc_section(tf, results_df)
                try:
                    await reporter.send(msg)
                    logging.info("Sent OHLC projection report for %s", tf)
                except Exception as e:
                    logging.error("Failed to send OHLC report: %s", e)
            else:
                logging.info("No OHLC alerts for %s", tf)
            await asyncio.sleep(1)

        engine.shutdown()
        await proxies.shutdown()
        

async def main() -> None:
    setup_logging()
    cfg = Config()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: _signal_handler(s))
    try:
        await asyncio.wait_for(run(cfg), timeout=cfg.run_timeout_seconds)
    except asyncio.TimeoutError:
        logging.error(
            "⛔ Run exceeded the %.0fs watchdog timeout and was force-cancelled. "
            "This should not happen under normal conditions — treat it as a bug.",
            cfg.run_timeout_seconds,
        )
        sys.exit(1)
    except Exception as e:
        logging.critical("Fatal error in main: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
