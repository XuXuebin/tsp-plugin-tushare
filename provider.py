"""Tushare Pro 数据源 provider: 日K / 除权因子 / 财务数据。

单位与口径 (实测 2026-11, 不按字段名推断):
  - daily.vol 已是**手** (与宿主契约一致, 原样透传)
  - daily.amount 是**千元** → ×1000 才是元 (自证: 600519.SH 20260910 amount=2428698.784
    → ×1000 = 2,428,698,784 元 ≈ 18900.22 手 × 100 × 1285.13 元 ✓)
  - adj_factor 是**累积因子**; 宿主契约要的是**单事件比值**(非累积), 累积链由
    pipeline._apply_adj_factor 自行 cum_prod → 这里必须转成相邻因子比值, 且只保留
    事件日 (比值≠1), 否则前复权会整体算错 (这是最容易悄悄出错的一处)
  - 代码格式 600519.SH 与宿主一致, 原样透传

未声明 minute / depth5 / realtime: Tushare 的分钟限频 1~2 次/天、五档无此接口、
rt_k 需单独开通权限 → 声明了反而会把路由带进死路。
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, datetime, timedelta
from typing import Any

import polars as pl

from .client import TushareClient, TushareError

logger = logging.getLogger(__name__)

_DATASETS = ("daily", "adj_factor", "financial")

API_KEY_ENV = "TUSHARE_TOKEN"
SECRETS_FIELD = "tushare_api_key"

_CALL_INTERVAL = 0.35          # 单接口节流 (Tushare 按接口计频, 保守些)
_FIN_HISTORY_PERIODS = 8       # 历史模式下每 symbol 保留的期数 (对齐 fuyao)
_CACHE_MAX_DAYS = 320           # 按日缓存的最大天数 (须 ≥ 窗口天数, 否则每批重拉 → 触发限频)
_DAILY_FIELDS = "ts_code,trade_date,open,high,low,close,vol,amount"
_ADJ_FIELDS = "ts_code,trade_date,adj_factor"
_BASIC_FIELDS = "ts_code,trade_date,total_share,float_share"   # daily_basic: 股本单位=万股
_SHARES_DAYS = 120          # 历史股本回溯的交易日数 (约半年; 突发调用数要压住 tushare 限频)

# 财务表 → (上游接口, 字段列表, 上游列→canonical 列)
_FIN_TABLES: dict[str, tuple[str, str, dict[str, str]]] = {
    "income": ("income",
               "ts_code,ann_date,end_date,total_revenue,operate_cost,sell_exp,admin_exp,"
               "rd_exp,operate_profit,finance_exp,total_profit,income_tax,n_income,"
               "n_income_attr_p,basic_eps",
               {"total_revenue": "revenue", "operate_cost": "operating_cost",
                "sell_exp": "selling_expense", "admin_exp": "admin_expense",
                "rd_exp": "rd_expense", "operate_profit": "operating_profit",
                "finance_exp": "financial_expense", "total_profit": "total_profit",
                "income_tax": "income_tax", "n_income": "net_income",
                "n_income_attr_p": "net_income_attributable", "basic_eps": "basic_eps"}),
    "balance_sheet": ("balancesheet",
                      "ts_code,ann_date,end_date,total_assets,total_cur_assets,"
                      "total_non_cur_assets,money_cap,accounts_receiv,total_liab,"
                      "total_hldr_eqy_exc_min_int",
                      {"total_assets": "total_assets", "total_cur_assets": "total_current_assets",
                       "total_non_cur_assets": "total_non_current_assets",
                       "money_cap": "cash_and_equivalents",
                       "accounts_receiv": "accounts_receivable",
                       "total_liab": "total_liabilities",
                       "total_hldr_eqy_exc_min_int": "total_equity"}),
    "cash_flow": ("cashflow",
                  "ts_code,ann_date,end_date,n_cashflow_act,n_cashflow_inv_act,"
                  "n_cash_flows_fnc_act,c_fr_sale_sg,c_pay_acq_const_fiolta,n_incr_cash_cash_equ",
                  {"n_cashflow_act": "net_operating_cash_flow",
                   "n_cashflow_inv_act": "net_investing_cash_flow",
                   "n_cash_flows_fnc_act": "net_financing_cash_flow",
                   "c_pay_acq_const_fiolta": "capex",
                   "n_incr_cash_cash_equ": "net_cash_change"}),
    "metrics": ("fina_indicator",
                "ts_code,ann_date,end_date,roe,roa,grossprofit_margin,netprofit_margin,"
                "debt_to_assets,or_yoy,netprofit_yoy,ocf_to_or,inv_turnover_rate",
                {"roe": "roe", "roa": "roa", "grossprofit_margin": "gross_margin",
                 "netprofit_margin": "net_margin", "debt_to_assets": "debt_to_asset_ratio",
                 "or_yoy": "revenue_yoy", "netprofit_yoy": "net_income_yoy",
                 "ocf_to_or": "operating_cash_to_revenue",
                 "inv_turnover_rate": "inventory_turnover"}),
}


def _raw_key() -> str:
    """读取 Key 原文: secrets.json (设置页写入) 优先, 其次容器环境变量。"""
    try:
        from app import secrets_store
        return secrets_store.get_env_backed_secret(SECRETS_FIELD, API_KEY_ENV) or ""
    except Exception:  # noqa: BLE001 - 独立加载时没有宿主模块
        import os
        return os.getenv(API_KEY_ENV, "")


def _parse_tokens(api_key: str | None) -> list[str]:
    """Key 框支持多种写法 → token 列表 (保持填写顺序, 轮询即按此顺序)。

    - 单 token:  ``<your-token>``
    - 分隔:      ``token1,token2`` / ``token1;token2`` / 换行分隔
    - JSON 数组: ``["token1","token2"]``
    - JSON 对象: ``{"tokens": ["t1","t2"]}``
    """
    text = (api_key or "").strip()
    if not text:
        return []
    if text[0] in "[{":
        try:
            data = json.loads(text)
        except ValueError:
            data = None
        if isinstance(data, list):
            out = [str(x).strip() for x in data if str(x).strip()]
            if out:
                return out
        elif isinstance(data, dict):
            for k in ("tokens", "keys", "token"):
                v = data.get(k)
                if isinstance(v, list):
                    out = [str(x).strip() for x in v if str(x).strip()]
                    if out:
                        return out
                if isinstance(v, str) and v.strip():
                    text = v.strip()
                    break
    parts = [x.strip() for x in re.split(r"[,;\n\r]+", text) if x.strip()]
    if parts:
        return parts
    return [text] if text else []


def _tokens() -> list[str]:
    """全部 token (按填写顺序轮询)。单 token 时长度为 1。"""
    return _parse_tokens(_raw_key())


def availability() -> tuple[bool, str]:
    """设置页自检: 有 Key 才算可用。"""
    toks = _tokens()
    if toks:
        return True, f"ok ({len(toks)} 个 token)"
    return False, f"缺少 Token(可在下方输入框直接填写, 或配置环境变量 {API_KEY_ENV})"


def probe_api_key(api_key: str) -> tuple[bool, str]:
    """用候选 Key 实探一次(先探后存)。支持多 token —— 逐个验证并报告可用数。"""
    tokens = _parse_tokens(api_key)
    if not tokens:
        return False, "Key 为空"
    client = None
    valid = 0
    details: list[str] = []
    try:
        for i, tok in enumerate(tokens, 1):
            try:
                client = TushareClient(tok, timeout=12.0)
                rows = client.call("daily", {"ts_code": "600519.SH",
                                             "start_date": "20260101", "end_date": "20260102"},
                                   _DAILY_FIELDS)
                valid += 1
                details.append(f"#{i} 可用({len(rows)}行)")
            except TushareError as exc:
                details.append(f"#{i} 失败:{str(exc)[:70]}")
            finally:
                if client is not None:
                    client.close()
                    client = None
    except Exception as exc:  # noqa: BLE001
        details.append(f"{type(exc).__name__}: {exc}")
    if valid == 0:
        return False, "全部 token 不可用: " + "; ".join(details)
    if valid == len(tokens):
        return True, f"ok ({valid} 个 token 全部可用, 插件将自动轮询摊平限频)"
    return True, f"部分可用 ({valid}/{len(tokens)}), 不可用的调用时自动跳过; " + "; ".join(details)


class _CnfreeConfig:  # 兼容命名: 实际是 tushare 的 config shim
    name: str = "tushare"
    display_name: str = "Tushare Pro"
    datasets: dict = {}
    path: None = None
    builtin: bool = True


def _date_str(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.strftime("%Y%m%d")


class TushareProvider:
    name = "tushare"
    builtin = True

    def __init__(self) -> None:
        from dataclasses import dataclass, field

        @dataclass
        class _Cfg:
            name: str = "tushare"
            display_name: str = "Tushare Pro"
            datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
            path: None = None
            builtin: bool = True

        self.config = _Cfg()
        # 多 token 轮询池: Key 框可填多个 token (JSON 数组 / 逗号 / 换行分隔),
        # 每次调用轮换一个 → 把 tushare 的按 token 限频线性摊平;
        # 触发限频/积分不足时立即切换下一个 token 重试同一接口。
        self._tokens: list[str] = _tokens()
        self._token_idx = 0
        self._clients: dict[str, TushareClient] = {}
        # 按 trade_date 的批量缓存: 全市场日K只需"每个交易日 1 次调用",
        # 宿主的 59 个批次共享同一窗口 → 缓存命中后总调用数 ≈ 窗口交易日数
        self._daily_cache: dict[str, pl.DataFrame] = {}
        self._adj_cache: dict[str, pl.DataFrame] = {}
        self._basic_cache: dict[str, pl.DataFrame] = {}
        self._cal_cache: list[str] | None = None
        self._cal_key: tuple[str, str] | None = None
        self._last_warnings: list[str] = []      # 最近取数告警 (供试拉接口回显诊断)

    # ------------------------------------------------------------------ 内部

    def _client_for(self, token: str) -> TushareClient:
        client = self._clients.get(token)
        if client is None:
            client = TushareClient(token)
            self._clients[token] = client
        return client

    def _next_token(self) -> str:
        """轮询取下一个 token (round-robin)。"""
        toks = self._tokens or _tokens()
        if not toks:
            raise TushareError("未配置 Tushare Token")
        token = toks[self._token_idx % len(toks)]
        self._token_idx = (self._token_idx + 1) % len(toks)
        return token

    def _call(self, api_name: str, params: dict[str, Any], fields: str,
              retries: int = 1) -> list[dict[str, Any]]:
        """调用接口 —— 多 token 时按调用轮询, 限频/积分不足自动切换下一个重试。

        尝试次数 = max(retries+1, token 数) —— 多 token 时保证每个都被轮到一次。
        """
        toks = self._tokens or _tokens()
        attempts = max(retries + 1, len(toks))
        last: Exception | None = None
        for attempt in range(attempts):
            token = self._next_token()
            try:
                rows = self._client_for(token).call(api_name, params, fields)
                time.sleep(_CALL_INTERVAL)
                return rows
            except TushareError as exc:
                last = exc
                msg = str(exc)
                rate_limited = any(k in msg for k in ("每分钟", "每小时", "每天", "限频", "频率", "积分", "权限"))
                if rate_limited and len(toks) > 1:
                    # 该 token 限频/无权限 → 立即换下一个 token 重试同一接口
                    logger.warning("tushare token 轮换: %s", msg[:120])
                    continue
                if "限频" in msg or "频率" in msg:
                    # 单 token 也要退避: tushare 限频窗口是分钟级, 3s 不够
                    time.sleep(15.0 * (attempt + 1))
                    continue
                break
        raise last if last else TushareError(f"{api_name} 调用失败")


    # ------------------------------------------------------------ 批量模式

    def _calendar(self, start: datetime, end: datetime) -> list[str] | None:
        """交易日历 (YYYYMMDD 列表)。trade_cal 无权限时返回 None → 退回日历日。"""
        key = (start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
        if self._cal_cache is not None and self._cal_key == key:
            return self._cal_cache
        try:
            rows = self._call("trade_cal", {"exchange": "SSE", "start_date": key[0],
                                            "end_date": key[1]}, "cal_date,is_open")
        except TushareError:
            rows = []
        days = [str(r.get("cal_date")) for r in rows
                if str(r.get("is_open")) in ("1", "1.0") and r.get("cal_date")]
        self._cal_cache = sorted(days) if days else None
        self._cal_key = key
        return self._cal_cache

    def _date_window(self, start_time: datetime | None, end_time: datetime | None) -> list[str]:
        """请求窗口内的交易日 (YYYYMMDD)。无日历权限时用日历日 (非交易日返回空, 安全)。"""
        end = end_time or datetime.now()
        start = start_time or (end - timedelta(days=365))
        cal = self._calendar(start, end)
        if cal:
            lo, hi = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
            today = datetime.now().strftime("%Y%m%d")
            hi = min(hi, today)          # 交易所日历含未来交易日 → 钳到今天
            return [d for d in cal if lo <= d <= hi]
        out, cur = [], start.date()
        while cur <= end.date():
            out.append(cur.strftime("%Y%m%d"))
            cur += timedelta(days=1)
        return out

    def _cached(self, cache: dict[str, pl.DataFrame], day: str, api: str,
                fields: str, price_scale: float = 1.0) -> pl.DataFrame:
        """按 trade_date 取全市场数据并缓存 (非交易日返回空表)。

        取数失败(限频/网络)时**返回空表而不是抛异常** —— 抛异常会让宿主把整批 100 只丢掉
        (实测: 深市批次全灭、沪市批次成功, 就是首批拉满窗口触发限频导致的)。
        丢一天 < 丢一批。
        """
        if day in cache:
            return cache[day]
        try:
            rows = self._call(api, {"trade_date": day}, fields)
        except TushareError as exc:
            msg = f"{api} trade_date={day} 取数失败: {exc}"
            logger.warning("tushare %s trade_date=%s 取数失败, 跳过该日: %s", api, day, exc)
            self._last_warnings.append(msg)
            self._last_warnings = self._last_warnings[-20:]
            return pl.DataFrame()      # 失败**不写缓存**: 空帧一旦缓存, 后续同步永远取不到这一天
        if not rows:
            frame = pl.DataFrame()
        else:
            frame = pl.DataFrame(rows)
            keep = [c for c in ("ts_code", "trade_date", "open", "high", "low", "close",
                                "vol", "amount", "adj_factor", "total_share", "float_share")
                    if c in frame.columns]
            frame = frame.select(keep)
            for c in keep:
                if c not in ("ts_code", "trade_date"):
                    frame = frame.with_columns(pl.col(c).cast(pl.Float64, strict=False))
            if "amount" in frame.columns and price_scale != 1.0:
                frame = frame.with_columns((pl.col("amount") * price_scale).alias("amount"))
            # daily_basic 的 total_share/float_share 单位是万股 → ×10000 折股
            for c in ("total_share", "float_share"):
                if c in frame.columns:
                    frame = frame.with_columns((pl.col(c) * 10000.0).alias(c))
        cache[day] = frame
        if len(cache) > _CACHE_MAX_DAYS:
            for old in sorted(cache)[: len(cache) - _CACHE_MAX_DAYS]:
                cache.pop(old, None)
        return frame

    def _daily_by_dates(self, symbols: list[str], days: list[str]) -> list[dict[str, Any]]:
        """按交易日批量取 (每次调用拿全市场, 再筛目标标的)。"""
        want = set(symbols)
        rows: list[dict[str, Any]] = []
        for day in days:
            frame = self._cached(self._daily_cache, day, "daily", _DAILY_FIELDS, price_scale=1000.0)
            if frame.is_empty():
                continue
            sub = frame.filter(pl.col("ts_code").is_in(list(want)))
            if sub.is_empty():
                continue
            for r in sub.to_dicts():
                rows.append({
                    "symbol": r.get("ts_code"),
                    "date": str(r.get("trade_date") or "")[:10].replace("-", ""),
                    "open": r.get("open"), "high": r.get("high"),
                    "low": r.get("low"), "close": r.get("close"),
                    "volume": r.get("vol"),
                    "amount": r.get("amount"),   # 已在取数时 ×1000 (千元→元)
                })
        return rows

    def _adj_by_dates(self, symbols: list[str], days: list[str]) -> list[dict[str, Any]]:
        """按交易日批量取除权因子 (全市场 → 筛标的 → 转单事件比值)。"""
        want = set(symbols)
        series: dict[str, list[tuple[str, float]]] = {}
        for day in days:
            frame = self._cached(self._adj_cache, day, "adj_factor", _ADJ_FIELDS)
            if frame.is_empty():
                continue
            sub = frame.filter(pl.col("ts_code").is_in(list(want)))
            for r in sub.to_dicts():
                f = r.get("adj_factor")
                if f is None:
                    continue
                series.setdefault(r.get("ts_code"), []).append(
                    (str(r.get("trade_date") or "")[:10].replace("-", ""), float(f)))
        out: list[dict[str, Any]] = []
        for sym, items in series.items():
            prev: float | None = None
            for day, factor in sorted(items):
                if prev is not None and prev > 0 and abs(factor / prev - 1.0) > 1e-9:
                    out.append({"symbol": sym, "trade_date": day, "ex_factor": factor / prev})
                prev = factor
        return out

    # ------------------------------------------------------------------ 维表

    def get_instruments(self, asset_type: str = "stock") -> list[dict]:
        """标的维表 (宿主 instrument_sync 会用它重写 data/instruments/instruments.parquet)。

        为什么必须有: 维表**跟随日K数据源** —— 当日K源不是 tickflow 且该源没有
        get_instruments 时, 宿主会回退 tickflow 直连, 而 **Free 档的标的池只有 2,469 只**
        → 每跑一次调度, 维表就从 5,893 只被改写成 2,469 只 (实测: job c8fccbbae1
        "个股维表同步完成,2469 只标的")。

        本实现用 tushare stock_basic (全市场 5,5xx 只, 含北交所) + daily_basic 的
        总/流通股本 (顺带把维表的股本字段填上, 免得 enriched 的换手率/市值为空)。
        返回形状对齐宿主 _flatten_instruments 期望的 Instrument 字典。
        """
        if asset_type != "stock":
            return []
        rows = self._call("stock_basic", {"list_status": "L"},
                          "ts_code,symbol,name,area,market,exchange,list_date")
        # 股本: 最近一个有数据的交易日的 daily_basic (全市场, 1 次调用)
        share_map: dict[str, tuple[float | None, float | None]] = {}
        day = self._latest_basic_day()
        if day:
            frame = self._cached(self._basic_cache, day, "daily_basic", _BASIC_FIELDS)
            if not frame.is_empty():
                for r in frame.to_dicts():
                    share_map[r.get("ts_code")] = (r.get("total_share"), r.get("float_share"))
        out: list[dict] = []
        for r in rows:
            code = r.get("ts_code")
            if not code:
                continue
            total, float_ = share_map.get(code, (None, None))
            list_date = str(r.get("list_date") or "")
            listing = (f"{list_date[:4]}-{list_date[4:6]}-{list_date[6:8]}"
                       if len(list_date) == 8 and list_date.isdigit() else None)
            out.append({
                "symbol": code,
                "name": r.get("name"),
                "code": r.get("symbol") or code.split(".")[0],
                "exchange": r.get("exchange"),
                "region": r.get("area"),
                "type": "stock",
                "ext": {
                    "listing_date": listing,
                    "total_shares": total,
                    "float_shares": float_,
                    "tick_size": None,
                },
            })
        return out

    def _latest_basic_day(self) -> str | None:
        """最近一个 daily_basic 有数据的交易日 (盘前/盘中当日可能还没有)。"""
        end = datetime.now()
        cal = self._calendar(end - timedelta(days=30), end)
        if not cal:
            return None
        today = end.strftime("%Y%m%d")
        for d in reversed([x for x in cal if x <= today]):
            frame = self._cached(self._basic_cache, d, "daily_basic", _BASIC_FIELDS)
            if not frame.is_empty():
                return d
        return None

    # ------------------------------------------------------------------ 日K

    def get_daily(self, symbols: list[str], start_time: datetime | None = None,
                  end_time: datetime | None = None, asset_type: str = "stock",
                  on_chunk_done=None) -> pl.DataFrame:
        """日K: [symbol, date, open, high, low, close, volume(手), amount(元)]。"""
        if asset_type != "stock":
            return pl.DataFrame()
        symbols = list(symbols or [])
        total = len(symbols)
        if total == 0:
            return pl.DataFrame()
        # 成本选择: 逐标的 n 次调用 vs 逐交易日 m 次调用 (后者每次拿全市场)
        days = self._date_window(start_time, end_time)
        if days and len(days) < total:
            rows = self._daily_by_dates(symbols, days)
            for i in range(len(days)):
                if on_chunk_done is not None:
                    on_chunk_done(int(total * (i + 1) / len(days)), total)
            return self._daily_frame(rows)
        rows: list[dict[str, Any]] = []
        for i, sym in enumerate(symbols):
            params = {"ts_code": sym}
            sd, ed = _date_str(start_time), _date_str(end_time)
            if sd:
                params["start_date"] = sd
            if ed:
                params["end_date"] = ed
            try:
                for r in self._call("daily", params, _DAILY_FIELDS):
                    amount = r.get("amount")
                    rows.append({
                        "symbol": r.get("ts_code") or sym,
                        "date": str(r.get("trade_date") or "")[:10].replace("-", ""),
                        "open": r.get("open"), "high": r.get("high"),
                        "low": r.get("low"), "close": r.get("close"),
                        "volume": r.get("vol"),
                        # tushare 的 daily.amount 单位是千元 → ×1000 才是元
                        "amount": (amount * 1000.0) if amount is not None else None,
                    })
            except TushareError as exc:
                logger.warning("tushare daily %s 失败: %s", sym, exc)
            if on_chunk_done is not None:
                on_chunk_done(i + 1, total)
        return self._daily_frame(rows)

    @staticmethod
    def _daily_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
        if not rows:
            return pl.DataFrame()
        df = pl.DataFrame(rows)
        return df.with_columns(
            pl.col("date").str.replace("-", "").str.to_date("%Y%m%d", strict=False),
            *[pl.col(c).cast(pl.Float64, strict=False) for c in
              ("open", "high", "low", "close", "volume", "amount")],
        ).select(["symbol", "date", "open", "high", "low", "close", "volume", "amount"]) \
         .sort(["symbol", "date"])

    # ------------------------------------------------------------------ 除权因子

    def get_adj_factors(self, symbols: list[str], start_time: datetime | None = None,
                        end_time: datetime | None = None, asset_type: str = "stock",
                        on_chunk_done=None) -> pl.DataFrame:
        """除权因子: [symbol, trade_date, ex_factor]。

        tushare 的 adj_factor 是**累积因子**; 宿主契约要**单事件比值**(非累积),
        这里按标的排序取相邻比值, 只保留比值≠1 的事件日。
        (事件比值的连乘 = 累积因子, 与 pipeline._apply_adj_factor 的 cum_prod 对齐 ✓)
        """
        if asset_type != "stock":
            return pl.DataFrame()
        symbols = list(symbols or [])
        total = len(symbols)
        if total == 0:
            return pl.DataFrame()
        days = self._date_window(start_time, end_time)
        if days and len(days) < total:
            out = self._adj_by_dates(symbols, days)
            for i in range(len(days)):
                if on_chunk_done is not None:
                    on_chunk_done(int(total * (i + 1) / len(days)), total)
            return self._adj_frame(out)
        out: list[dict[str, Any]] = []
        for i, sym in enumerate(symbols):
            params = {"ts_code": sym}
            sd, ed = _date_str(start_time), _date_str(end_time)
            if sd:
                params["start_date"] = sd
            if ed:
                params["end_date"] = ed
            try:
                raw = self._call("adj_factor", params, _ADJ_FIELDS)
            except TushareError as exc:
                logger.warning("tushare adj_factor %s 失败: %s", sym, exc)
                raw = []
            series = sorted(
                ((str(r.get("trade_date") or ""), float(r.get("adj_factor") or 0.0)) for r in raw
                 if r.get("trade_date") and r.get("adj_factor")),
                key=lambda x: x[0],
            )
            prev: float | None = None
            for day, factor in series:
                if prev is not None and prev > 0 and abs(factor / prev - 1.0) > 1e-9:
                    out.append({"symbol": sym, "trade_date": day, "ex_factor": factor / prev})
                prev = factor
            if on_chunk_done is not None:
                on_chunk_done(i + 1, total)
        return self._adj_frame(out)

    @staticmethod
    def _adj_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
        if not rows:
            return pl.DataFrame()
        df = pl.DataFrame(rows)
        return df.with_columns(
            pl.col("trade_date").str.replace("-", "").str.to_date("%Y%m%d", strict=False),
            pl.col("ex_factor").cast(pl.Float64, strict=False),
        ).select(["symbol", "trade_date", "ex_factor"]).sort(["symbol", "trade_date"])

    # ------------------------------------------------------------------ 财务

    # ------------------------------------------------------------------ 股本

    def _shares(self, symbols: list[str], latest_only: bool) -> pl.DataFrame:
        """历史股本表: [symbol, period_end, announce_date, total_shares, float_shares]。

        上游用 daily_basic 的**日频快照** (total_share / float_share, 单位=万股 → ×10000 折股)。
        全历史模式只保留**股本变动日** + 每只最后一行 → 行数极小, 但对"按公告日取有效流通股本"
        的换手率口径完全够用 (契约见 docs/custom-data-source.md: total_shares/float_shares, 单位股)。
        自证: 平安银行 daily_basic total_share=1940591.8198 万股 → ×10000 = 19,405,918,198 股
              = 宿主 instruments 维表的 total_shares ✓ (茅台/宁德同样核对通过)
        """
        want = set(symbols or [])
        if not want:
            return pl.DataFrame()
        end = datetime.now()
        cal = self._calendar(end - timedelta(days=_SHARES_DAYS * 2 + 60), end)
        if not cal:
            return pl.DataFrame()
        today = datetime.now().strftime("%Y%m%d")
        cal = [d for d in cal if d <= today]      # 交易所日历含未来交易日 → 钳到今天
        if not cal:
            return pl.DataFrame()
        if latest_only:
            # 当日可能还没出数据 (盘前/盘中) → 从最新往回找, 最多 5 天
            days = list(reversed(cal[-5:]))
        else:
            days = cal[-_SHARES_DAYS:]

        series: dict[str, list[tuple[str, float | None, float | None]]] = {}
        for day in days:
            frame = self._cached(self._basic_cache, day, "daily_basic", _BASIC_FIELDS)
            if frame.is_empty():
                if latest_only:
                    continue                      # 继续往回找
                continue
            sub = frame.filter(pl.col("ts_code").is_in(list(want)))
            for r in sub.to_dicts():
                total = r.get("total_share")
                float_ = r.get("float_share")
                if total is None and float_ is None:
                    continue
                # _cached 已把 万股 → 股 (×10000), 这里直接用
                series.setdefault(r.get("ts_code"), []).append(
                    (day, total, float_))
            if latest_only and series:
                break                             # 找到有数据的一天即可

        rows: list[dict[str, Any]] = []
        for sym, items in series.items():
            items.sort()
            if latest_only:
                day, total, float_ = items[-1]
                rows.append({"symbol": sym, "period_end": day, "announce_date": day,
                             "total_shares": total, "float_shares": float_})
                continue
            prev: tuple[float | None, float | None] | None = None
            for day, total, float_ in items:
                if prev is None or total != prev[0] or float_ != prev[1]:
                    rows.append({"symbol": sym, "period_end": day, "announce_date": day,
                                 "total_shares": total, "float_shares": float_})
                prev = (total, float_)
        if not rows:
            return pl.DataFrame()
        df = pl.DataFrame(rows, infer_schema_length=None)
        return df.with_columns(
            pl.col("period_end").str.strptime(pl.Date, "%Y%m%d", strict=False),
            pl.col("announce_date").str.strptime(pl.Date, "%Y%m%d", strict=False),
            pl.col("total_shares").cast(pl.Float64, strict=False),
            pl.col("float_shares").cast(pl.Float64, strict=False),
        ).select(["symbol", "period_end", "announce_date", "total_shares", "float_shares"]) \
         .sort(["symbol", "period_end"])

    def get_financials(self, table: str, symbols: list[str],
                       latest_only: bool = True) -> pl.DataFrame:
        """财务数据: [symbol, period_end, announce_date, 指标...]。"""
        if table == "shares":
            return self._shares(symbols, latest_only)
        spec = _FIN_TABLES.get(table)
        if spec is None:
            return pl.DataFrame()
        api_name, fields, col_map = spec
        rows_out: list[dict[str, Any]] = []
        for sym in symbols or []:
            try:
                raw = self._call(api_name, {"ts_code": sym}, fields)
            except TushareError as exc:
                logger.warning("tushare %s %s 失败: %s", table, sym, exc)
                continue
            for r in raw:
                if not r.get("end_date"):
                    continue
                row: dict[str, Any] = {
                    "symbol": sym,
                    "period_end": str(r.get("end_date"))[:10],
                    "announce_date": str(r.get("ann_date") or "")[:10] or None,
                }
                for src, dst in col_map.items():
                    row[dst] = r.get(src)
                rows_out.append(row)
        if not rows_out:
            return pl.DataFrame()
        df = pl.DataFrame(rows_out, infer_schema_length=None)
        if "announce_date" in df.columns:
            df = df.with_columns(pl.col("announce_date").fill_null(pl.lit("")))
        if latest_only:
            # 每 symbol 只保留最新一期 (announce_date 最新, 平手取 period_end 最新)
            df = (df.sort(["symbol", "announce_date", "period_end"])
                    .group_by("symbol", maintain_order=True).last())
        else:
            # 历史模式: 每 symbol 保留最近 _FIN_HISTORY_PERIODS 期
            df = (df.sort(["symbol", "period_end"])
                    .group_by("symbol", maintain_order=True)
                    .tail(_FIN_HISTORY_PERIODS))
        return df

    # ------------------------------------------------------------------ 辅助

    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        """设置页「试拉」: 有界样本, 不写盘。"""
        probe = list(symbols or [])[:3] or ["600519.SH", "000001.SZ"]
        try:
            if dataset == "daily":
                df = self.get_daily(probe, datetime(2026, 1, 1), datetime(2026, 12, 31))
            elif dataset == "adj_factor":
                df = self.get_adj_factors(probe, datetime(2026, 1, 1), datetime(2026, 12, 31))
            elif dataset == "financial":
                df = self.get_financials("income", probe, latest_only=True)
            elif dataset == "shares":
                df = self.get_financials("shares", probe, latest_only=False)
            else:
                return {"provider": self.name, "dataset": dataset, "rows": 0, "columns": [],
                        "preview": [], "error": "本插件不提供该数据集"}
            preview = [{k: (str(v) if isinstance(v, (date, datetime)) else v)
                        for k, v in row.items()}
                       for row in df.head(5).to_dicts()]
            return {"provider": self.name, "dataset": dataset, "rows": df.height,
                    "columns": list(df.columns), "preview": preview,
                    "warnings": list(self._last_warnings[-10:])}
        except Exception as exc:  # noqa: BLE001
            return {"provider": self.name, "dataset": dataset, "rows": 0, "columns": [],
                    "preview": [], "error": f"{type(exc).__name__}: {exc}"}

    def close(self) -> None:
        for client in self._clients.values():
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass
        self._clients.clear()
        self._client = None
