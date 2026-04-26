import asyncio
import json
import logging
import os
import random
import math
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Set, Optional, Tuple, Any

import aiohttp
import numpy as np
import pandas as pd
from tqdm.asyncio import tqdm

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


IGNORED_SYMBOLS = {
    "USDPUSDT", "USD1USDT", "TUSDUSDT", "AEURUSDT", "USDCUSDT",
    "ZKJUSDT", "FDUSDUSDT", "XUSDUSDT", "EURUSDT", "EURIUSDT",
    "WBTCUSDT", "YFIUSDT", "BNBUSDT", "XMRUSDT", "SANTOSUSDT",
    "PROMUSDT", "ACMUSDT", "CITYUSDT", "JUVUSDT", "PSGUSDT",
    "WINUSDT"
}

SHOW_D_PLUS = os.getenv("SHOW_D_PLUS", "True") == "True"
SHOW_D_MINUS = os.getenv("SHOW_D_MINUS", "True") == "False"
SHOW_M_PLUS = os.getenv("SHOW_M_PLUS", "True") == "True"
SHOW_M_MINUS = os.getenv("SHOW_M_MINUS", "True") == "False"
OHLC_TIMEFRAMES = {'1d', '1w'}
OHLC_LOOKBACK = int(os.getenv("OHLC_LOOKBACK", "60"))
OHLC_ALERT_THRESHOLD = float(os.getenv("OHLC_ALERT_THRESHOLD", "2.0"))


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
        score *= (0.5 ** self.consecutive_failures)
        if self.avg_latency_ms < 9999:
            latency_factor = max(0.1, 1.0 - (self.avg_latency_ms / 5000))
            score *= (0.6 + 0.4 * latency_factor)
        return max(0.001, min(1.0, score))


class RobustProxyPool:
    def __init__(
        self,
        max_pool_size: int = 25,
        min_pool_size: int = 15,
        max_consecutive_failures: int = 3,
        cooldown_seconds: float = 90.0,
        ban_after_uses: int = 8,
        ban_below_rate: float = 0.25,
        validation_concurrency: int = 50,
        refresh_interval: float = 180.0
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
        self._refresh_task: Optional[asyncio.Task] = None
        self._source_url: Optional[str] = None

    async def initialize(self, session: aiohttp.ClientSession, source_url: str):
        self._session = session
        self._source_url = source_url
        await self._populate_pool()
        self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def _refresh_loop(self):
        while True:
            await asyncio.sleep(self.refresh_interval)
            await self._populate_pool()

    async def _populate_pool(self):
        try:
            async with self._session.get(self._source_url, timeout=10) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    raw = {line.strip() for line in text.splitlines() if line.strip() and '.' in line}
                    new_proxies = {p if "://" in p else f"http://{p}" for p in raw}

                    to_validate = list(new_proxies - set(self._proxies.keys()))
                    if not to_validate:
                        return

                    sem = asyncio.Semaphore(self.validation_concurrency)

                    async def validate(p):
                        async with sem:
                            start = time.time()
                            try:
                                async with self._session.get(
                                    "https://fapi.binance.com/fapi/v1/time",
                                    proxy=p,
                                    timeout=5
                                ) as r:
                                    if r.status == 200:
                                        return p, True, (time.time() - start) * 1000
                            except:
                                pass
                            return p, False, 0

                    tasks = [validate(p) for p in to_validate]
                    for coro in asyncio.as_completed(tasks):
                        p, ok, lat = await coro
                        if ok:
                            async with self._lock:
                                active_count = len([
                                    pr for pr, s in self._proxies.items()
                                    if s.state == ProxyState.ACTIVE
                                ])
                                if active_count < self.max_pool_size:
                                    self._proxies[p] = ProxyStats(
                                        successes=1,
                                        total_latency_ms=lat
                                    )
        except Exception as e:
            logging.error(f"Proxy refresh error: {e}")

    async def get_proxy(self) -> Optional[str]:
        now = time.time()
        async with self._lock:
            active = []
            for p, s in self._proxies.items():
                if s.state == ProxyState.COOLING and now > s.cooldown_until:
                    s.state = ProxyState.ACTIVE
                    s.consecutive_failures = 0
                if s.state == ProxyState.ACTIVE:
                    active.append((p, s.compute_score()))

            if not active:
                return None

            total = sum(score for _, score in active)
            r = random.random() * total
            curr = 0
            for p, score in active:
                curr += score
                if curr >= r:
                    self._proxies[p].last_used = now
                    return p
            return active[-1][0]

    async def report_success(self, proxy: str, latency: float):
        async with self._lock:
            if proxy in self._proxies:
                s = self._proxies[proxy]
                s.successes += 1
                s.consecutive_failures = 0
                s.total_latency_ms += latency
                s.last_success = time.time()

    async def report_failure(self, proxy: str):
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

    async def shutdown(self):
        if self._refresh_task:
            self._refresh_task.cancel()


class BinanceScanner:
    def __init__(self, session: aiohttp.ClientSession, proxy_pool: RobustProxyPool):
        self.session = session
        self.proxies = proxy_pool
        self.sem = asyncio.Semaphore(50)

    async def _request(self, url: str, params: dict = None) -> Any:
        for _ in range(5):
            proxy = await self.proxies.get_proxy()
            if not proxy:
                await asyncio.sleep(0.5)
                continue
            start = time.time()
            try:
                async with self.session.get(url, params=params, proxy=proxy, timeout=8) as resp:
                    if resp.status == 200:
                        await self.proxies.report_success(
                            proxy,
                            (time.time() - start) * 1000
                        )
                        return await resp.json()
                    if resp.status == 429:
                        await asyncio.sleep(2)
            except:
                pass
            await self.proxies.report_failure(proxy)
        return None

    async def get_all_symbols(self) -> Tuple[Set[str], Set[str]]:
        f_info = await self._request("https://fapi.binance.com/fapi/v1/exchangeInfo")
        s_info = await self._request("https://api.binance.com/api/v3/exchangeInfo")

        perps = {
            s['symbol'] for s in f_info.get('symbols', [])
            if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING'
        } if f_info else set()

        spots = {
            s['symbol'] for s in s_info.get('symbols', [])
            if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING'
        } if s_info else set()

        return perps, spots

    async def fetch_24h_changes(self) -> Dict[str, float]:
        s_data = await self._request("https://api.binance.com/api/v3/ticker/24hr")
        f_data = await self._request("https://fapi.binance.com/fapi/v1/ticker/24hr")
        res = {}
        if s_data:
            res.update({i['symbol']: float(i['priceChangePercent']) for i in s_data})
        if f_data:
            res.update({i['symbol']: float(i['priceChangePercent']) for i in f_data})
        return res

    async def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        market: str,
        limit: int
    ) -> Optional[pd.DataFrame]:
        base = (
            "https://fapi.binance.com/fapi/v1/klines"
            if market == "perp"
            else "https://api.binance.com/api/v3/klines"
        )
        data = await self._request(base, {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        })
        if not data:
            return None
        df = pd.DataFrame(data, columns=[
            'ot', 'open', 'high', 'low', 'close', 'vol',
            'ct', 'qav', 'nt', 'tbb', 'tbq', 'i'
        ])
        cols = ['open', 'high', 'low', 'close', 'vol']
        df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
        return df


def calculate_metrics(df: pd.DataFrame, symbol: str, tf: str) -> Dict:
    last = df.iloc[-1]
    close = last['close']
    ema34 = df['close'].ewm(span=34, adjust=False).mean()
    curr_ema = ema34.iloc[-1]
    pct_dist = ((close - curr_ema) / curr_ema) * 100

    ema12 = df['close'].ewm(span=12).mean()
    ema26 = df['close'].ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()

    ema13 = df['close'].ewm(span=13).mean().iloc[-1]
    ema21 = df['close'].ewm(span=21).mean().iloc[-1]

    vol_recent = df['vol'].tail(20).mean()
    vol_long = df['vol'].tail(60).mean()
    rel_vol = vol_recent / vol_long if vol_long > 0 else 1

    cons_range = df['high'].tail(20).max() - df['low'].tail(20).min()
    cons_ratio = cons_range / df['close'].tail(20).mean()

    ema_pct_change = ema34.pct_change() * 100
    was_bearish = (ema_pct_change.iloc[-7:-2] < -0.01).sum() >= 3
    is_bullish = (ema_pct_change.iloc[-2:] > 0.01).all()
    bear_to_bull = bool(was_bearish and is_bullish)

    score = (
        (min((df['close'] > ema34).tail(20).sum() / 20 * 10, 10)) +
        (5 if rel_vol > 1.5 else 0) +
        (3 if macd.iloc[-1] > signal.iloc[-1] else 0) +
        (2 if ema13 > ema21 > curr_ema else 0) +
        (8 if bear_to_bull else 0) +
        (3 if cons_ratio < 0.05 else 0)
    )

    res = {
        'symbol': symbol,
        'pct_distance': pct_dist,
        'breakout_score': score,
        'rel_vol': rel_vol,
        'macd_bullish': macd.iloc[-1] > signal.iloc[-1],
        'ema_align': ema13 > ema21 > curr_ema,
        'bear_to_bull': bear_to_bull,
        'cons_potential': cons_ratio < 0.05,
        'ohlc': None
    }

    if tf in OHLC_TIMEFRAMES and len(df) >= OHLC_LOOKBACK + 1:
        hist = df.iloc[-(OHLC_LOOKBACK + 1):-1].copy()
        hist['is_bull'] = hist['close'] > hist['open']
        hist['m_wick'] = np.where(
            hist['is_bull'],
            hist['open'] - hist['low'],
            hist['high'] - hist['open']
        )
        hist['d_dist'] = np.where(
            hist['is_bull'],
            hist['high'] - hist['open'],
            hist['open'] - hist['low']
        )

        avg_m = hist['m_wick'].mean()
        avg_d = hist['d_dist'].mean()
        op = last['open']

        projs = {
            'D+': op + avg_d + avg_m,
            'D-': op - avg_d - avg_m,
            'M-': op + avg_m,
            'M+': op - avg_m
        }
        alerts = []
        for n, v in projs.items():
            if (n == 'D+' and not SHOW_D_PLUS) or \
               (n == 'D-' and not SHOW_D_MINUS) or \
               (n == 'M+' and not SHOW_M_PLUS) or \
               (n == 'M-' and not SHOW_M_MINUS):
                continue
            d = abs((close - v) / v * 100)
            if d <= OHLC_ALERT_THRESHOLD:
                alerts.append({'level': n, 'dist': d})
        res['ohlc'] = alerts

    return res


class Reporter:
    def __init__(
        self,
        token: str,
        chat_id: str,
        channel: str,
        session: aiohttp.ClientSession
    ):
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.chat_id = chat_id
        self.channel = channel
        self.session = session

    def esc(self, t: str) -> str:
        return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', str(t))

    async def send(self, msg: str):
        for target in [self.chat_id, self.channel]:
            if not target:
                continue
            try:
                payload = {
                    "chat_id": target,
                    "text": msg,
                    "parse_mode": "MarkdownV2"
                }
                async with self.session.post(self.url, json=payload) as r:
                    if r.status == 429:
                        await asyncio.sleep(int(r.headers.get("Retry-After", 5)))
            except:
                pass

    def format_ema(self, tf: str, results: List[Dict], daily: Dict) -> str:
        df = pd.DataFrame(results)
        df['dly'] = df['symbol'].map(daily)

        def _fmt_sec(title, data):
            out = [f"{self.esc(tf)} • {self.esc(title)}", ""]
            out.append(f"{'Symbol':<12} {'Dist%':>8} {'Daily%':>10}")
            out.append("-" * 32)
            for _, r in data.iterrows():
                dly = f"{r['dly']:.2f}%" if pd.notnull(r['dly']) else "N/A"
                out.append(
                    f"{r['symbol']:<12} {r['pct_distance']:>8.2f} {dly:>10}"
                )
            out.append("")
            return "\n".join(out)

        above = df.sort_values('pct_distance', ascending=False).head(20)
        below = df.sort_values('pct_distance').head(20)
        return _fmt_sec("Above EMA", above) + "\n\n" + _fmt_sec("Below EMA", below)

    def format_enhanced(self, tf: str, results: List[Dict], daily: Dict) -> str:
        top = [r for r in results if r['breakout_score'] >= 10]
        if not top:
            return ""
        top = sorted(top, key=lambda x: x['breakout_score'], reverse=True)[:15]

        out = [f"{self.esc(tf)} • Enhanced Breakout", ""]
        out.append(
            f"{'Symbol':<10} {'Scr':>4} {'Dst%':>5} {'Vol':>4} {'M':>1} {'E':>1} {'B':>1}"
        )
        out.append("-" * 35)
        for r in top:
            m = "↑" if r['macd_bullish'] else " "
            e = "✓" if r['ema_align'] else " "
            b = "🚀" if r['bear_to_bull'] else " "
            out.append(
                f"{r['symbol']:<10} {r['breakout_score']:>4.1f} "
                f"{r['pct_distance']:>5.1f} {r['rel_vol']:>4.1f} "
                f"{m:>1} {e:>1} {b:>1}"
            )
        out.append("")
        return "\n".join(out)

    def format_ohlc(self, tf: str, results: List[Dict]) -> str:
        alerts = []
        for r in results:
            if r['ohlc']:
                for a in r['ohlc']:
                    alerts.append({'s': r['symbol'], 'l': a['level'], 'd': a['dist']})
        if not alerts:
            return ""

        alerts = sorted(alerts, key=lambda x: x['d'])[:30]

        out = [f"{self.esc(tf)} • OHLC Projections", ""]
        out.append(f"{'Symbol':<12} {'Lvl':<5} {'Dist%':>8}")
        out.append("-" * 27)
        for a in alerts:
            out.append(f"{a['s']:<12} {a['l']:<5} {a['d']:>8.2f}")
        out.append("")
        return "\n".join(out)


async def run():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=0)
    ) as session:
        proxies = RobustProxyPool()
        await proxies.initialize(session, os.getenv("PROXY_LIST_URL"))

        scanner = BinanceScanner(session, proxies)
        reporter = Reporter(
            os.getenv("TELEGRAM_BOT_TOKEN"),
            os.getenv("TELEGRAM_CHAT_ID"),
            os.getenv("TELEGRAM_CHANNEL_USERNAME"),
            session
        )

        perps, spots = await scanner.get_all_symbols()
        all_syms = [s for s in list(perps | spots) if s not in IGNORED_SYMBOLS]
        daily = await scanner.fetch_24h_changes()

        for tf in ['15m', '1h', '4h', '1d', '1w']:
            logging.info(f"Scanning {tf}...")

            async def task(s):
                async with scanner.sem:
                    market = "perp" if s in perps else "spot"
                    df = await scanner.fetch_ohlcv(s, tf, market, 200)
                    if df is not None and not df.empty:
                        return calculate_metrics(df, s, tf)
                    return None

            results = await tqdm.gather(*[task(s) for s in all_syms], desc=tf)
            results = [r for r in results if r]

            if results:
                ema_msg = reporter.format_ema(tf, results, daily)
                await reporter.send(ema_msg)

                enh_msg = reporter.format_enhanced(tf, results, daily)
                if enh_msg:
                    await reporter.send(enh_msg)

                ohlc_msg = reporter.format_ohlc(tf, results)
                if ohlc_msg:
                    await reporter.send(ohlc_msg)

        await proxies.shutdown()


if __name__ == "__main__":
    asyncio.run(run())
