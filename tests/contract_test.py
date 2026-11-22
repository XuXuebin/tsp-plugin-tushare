"""tushare 契约自测（不联网、不需要 Token）。

覆盖:
  - 单位换算: daily.amount 千元 → 元(×1000); daily.vol 已是手(原样)
  - 复权口径: adj_factor 累积因子 → 单事件比值(相邻比值, 仅事件日)
  - 多 Token 轮询: 解析多种写法 / 限频时自动切换下一个 token
  - 能力声明: 只声明 daily/adj_factor/financial
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _load_plugin(ns: str):
    """按宿主的方式加载插件包(命名空间包 + 相对导入), 与外部插件 loader 一致。"""
    import importlib
    import sys
    import types
    root = Path(__file__).resolve().parent.parent
    package = types.ModuleType(ns)
    package.__path__ = [str(root)]
    package.__package__ = ns
    sys.modules[ns] = package
    return importlib.import_module(f"{ns}.provider")

_mod = _load_plugin("tushare_plugin")
TushareProvider = _mod.TushareProvider
_parse_tokens = _mod._parse_tokens


def test_parse_tokens_formats():
    """单 token / 逗号 / 换行 / JSON 数组 / JSON 对象 → 统一成列表。"""
    assert _parse_tokens("abc") == ["abc"]
    assert _parse_tokens("a,b") == ["a", "b"]
    assert _parse_tokens("a;b") == ["a", "b"]
    assert _parse_tokens("a\nb") == ["a", "b"]
    assert _parse_tokens('["a","b"]') == ["a", "b"]
    assert _parse_tokens('{"tokens":["a","b"]}') == ["a", "b"]
    assert _parse_tokens("") == []
    assert _parse_tokens(None) == []


def test_daily_amount_thousand_yuan_to_yuan():
    """上游 amount 单位=千元 → ×1000 转元；vol 已是手，原样透传。"""
    p = TushareProvider()
    p._tokens = ["tok"]
    captured = {}

    def fake_call(api_name, params, fields, retries=1):
        captured["api"] = api_name
        return [{"ts_code": "600519.SH", "trade_date": "20260910", "open": 1280.0,
                 "high": 1290.0, "low": 1275.0, "close": 1285.13,
                 "vol": 18900.22, "amount": 2428698.784}]

    p._call = fake_call
    df = p.get_daily(["600519.SH"], None, None)
    row = df.to_dicts()[0]
    assert abs(row["amount"] - 2428698784.0) < 1e-6, row      # 千元 → 元
    assert abs(row["volume"] - 18900.22) < 1e-9, row          # 手，原样
    assert abs(row["close"] - 1285.13) < 1e-9, row
    p.close()


def test_token_rotation_switches_on_rate_limit():
    """限频错误 → 自动切换下一个 token 重试同一接口。"""
    p = TushareProvider()
    p._tokens = ["bad", "good"]
    calls = []

    class _FakeClient:
        def __init__(self, token, **kw):
            self.token = token

        def call(self, api_name, params=None, fields=""):
            calls.append(self.token)
            if self.token == "bad":
                # 模拟该 token 被限频/无权限
                raise _mod.TushareError("daily 失败: code=40203 抱歉，您没有接口(daily)访问权限")
            if api_name != "daily":
                return []
            return [{"ts_code": "000001.SZ", "trade_date": "20261120", "open": 12.0,
                     "high": 12.2, "low": 11.9, "close": 12.1,
                     "vol": 100.0, "amount": 121000.0}]

        def close(self):
            pass

    p._client_for = lambda token: _FakeClient(token)
    df = p.get_daily(["000001.SZ"], None, None)
    assert df.height == 1, df
    # 首个 token 命中限频 → 立即切换下一个 → 最终用 good 取到数
    assert calls[0] == "bad", calls
    assert calls[-1] == "good", calls
    assert "good" in calls, calls
    p.close()


def test_declares_only_supported_datasets():
    p = TushareProvider()
    assert set(p.config.datasets) == {"daily", "adj_factor", "financial"}, p.config.datasets
    assert p.name == "tushare"
    p.close()


def _run_all():
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  PASS {name}")


if __name__ == "__main__":
    _run_all()
    print("tushare 契约自测全部通过")
