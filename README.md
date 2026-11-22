# tsp-plugin-tushare — Tushare Pro 数据源插件

> TickStock Panel **外部数据源插件** · 一键安装 · 支持多 Token 轮询

直连 Tushare Pro 官方 HTTP 接口（`api.tushare.pro`），**不需要安装 tushare SDK**，
纯 `httpx` 实现，零额外依赖。

| 数据集 | 上游接口 | 说明 |
| --- | --- | --- |
| `daily` | `daily` | 股票日 K；**`vol` 已是手**，**`amount` 是千元 → ×1000 转元** |
| `adj_factor` | `adj_factor` | 上游是**累积因子** → 转成宿主要的**单事件比值**（相邻因子比值，仅事件日出行） |
| `financial` | `income` / `balancesheet` / `cashflow` / `fina_indicator` | 映射为宿主 canonical 列（revenue / total_assets / roe …） |

## 为什么需要它

Tushare Pro 的接口覆盖广、但官方 SDK 依赖重（且需编译）。这个插件把常用能力
（日 K / 除权因子 / 财务三表+指标）做成零依赖的外部包，装上即可接管对应能力。

## 安装

1. 打开 **设置 → 导入与备份**
2. 拖入 Release 页的 `tsp-plugin-tushare-v1.1.0.zip`
3. 安装 → 「查看已安装扩展」→ 填 **API Key**（见下）
4. 在数据源详情里勾选要接管的能力

## 配置 API Key

设置页的 **API Key** 输入框（`TUSHARE_TOKEN`），**先探后存**：保存前会实探一次，
无效不会落盘。获取 Token：<https://tushare.pro/user/token>

也可用环境变量 `TUSHARE_TOKEN` 作为后备（`secrets.json` 优先于 `.env`）。

### 多 Token 轮询（v1.1.0）

Tushare 的限频是**按 token 计**的，多个 token 可以把每分钟配额线性摊平。
Key 框支持三种写法，效果相同：

```
token1,token2,token3            # 逗号分隔
token1
token2                          # 换行分隔
["token1","token2","token3"]    # JSON 数组
```

- 保存前**逐个实探**，返回每个 token 的可用状态
- 调用时按填写顺序**轮询**（round-robin），每次调用换一个 token
- 某个 token 触发限频 / 积分不足 → **自动切换下一个**重试同一接口
- 只有 1 个 token 时行为与旧版完全一致

## 口径自证（实测）

- `daily.vol` 是**手**：`600519.SH` 18,900.22 手 × 100 × 1285.13 元 ≈ 上游 `amount` × 1000 ✓
- `daily.amount` 是**千元**：`2428698.784`（千元）→ ×1000 = 2,428,698,784 元 ✓
- `adj_factor` 是**累积因子**：直接落盘会让前复权整体算错，这里转成**相邻比值**且只留事件日 ✓
- `trade_date` 批量模式：按交易日取全市场（一次调用覆盖 5,000+ 只），
  全市场日 K 从 ~50 分钟降到 **~4 分钟** ✓

## 已知边界

- Tushare 分钟接口（`stk_mins`）限频极严（实测 1~2 次/天），本插件**不声明** `minute`；
  五档无此接口 → 不声明 `depth5`；实时快照 `rt_k` 需单独开通权限。
- 财务表需要对应的积分/权限等级，不同账号能力不同（安装后可用「试拉」逐项验证）。
- 数据口径以 Tushare 官方文档为准，本插件只做单位与复权口径换算。

## 兼容性

- 宿主 `>= 0.3.37`，Python `>= 3.11`
- 依赖宿主自带：`httpx>=0.27`、`polars>=1.44,<1.45`

## 免责声明

本插件调用 Tushare Pro 官方接口，需遵守其服务条款与积分规则；数据准确性以 Tushare 官方
口径为准，请勿用于实盘下单或资金决策。

## 开发与测试

```bash
python tests/contract_test.py     # 契约自测(不联网、不需要 Token)
```

## License

MIT © XuXuebin
