"""Tushare Pro 官方 HTTP 客户端 (纯 httpx, 无 SDK 依赖)。

Tushare 的响应形态是列数组: {"code":0,"data":{"fields":[...],"items":[[...]]}}
本层负责把它转成 list[dict] (宿主的 extract_rows 只认 dict / list[dict])。
"""

from __future__ import annotations

import time
from typing import Any

import httpx

API_URL = "https://api.tushare.pro"


class TushareError(RuntimeError):
    """上游明确失败 (无权限 / 限频 / 参数错); 消息里不含 token。"""


class TushareClient:
    def __init__(self, token: str, timeout: float = 20.0, interval: float = 0.35) -> None:
        if not token:
            raise TushareError("未配置 Tushare Token")
        self._token = token
        self._interval = interval
        self._client = httpx.Client(timeout=timeout, follow_redirects=True,
                                   headers={"Content-Type": "application/json",
                                            "User-Agent": "tick-stock-panel/tushare-plugin"})

    def call(self, api_name: str, params: dict[str, Any] | None = None,
             fields: str = "") -> list[dict[str, Any]]:
        """调用一个接口, 返回 list[dict]。失败抛 TushareError。"""
        payload = {"api_name": api_name, "token": self._token,
                   "params": params or {}, "fields": fields}
        try:
            resp = self._client.post(API_URL, json=payload)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise TushareError(f"{api_name} 请求失败: {type(exc).__name__}") from exc
        try:
            body = resp.json()
        except ValueError as exc:
            raise TushareError(f"{api_name} 响应不是 JSON") from exc
        code = body.get("code")
        if code != 0:
            msg = str(body.get("msg") or body.get("detail") or "")
            raise TushareError(f"{api_name} 失败: code={code} {msg[:180]}")
        data = body.get("data") or {}
        cols = data.get("fields") or []
        rows = data.get("items") or []
        return [dict(zip(cols, row)) for row in rows]

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass
