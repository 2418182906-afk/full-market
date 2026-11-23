# ETF 全市场宽基份额追踪系统（独立部署版）

本项目是在原 ETF Share Tracker 基础上扩展的独立完整版本。保留原有核心 ETF 监测页面，并新增沪深两市七大宽基系列 ETF 份额汇总。

## 统计范围

- 上证50
- 沪深300
- 中证500
- 中证1000
- 中证A500
- 科创宽基：科创50、科创100、科创200、科创综指等可从基金简称识别的宽基 ETF
- 创业板：创业板指与创业板50合并统计

只统计沪深交易所上市 ETF。ETF 联接基金不纳入。当前自动分类依据交易所披露的基金简称；无法可靠识别的产品不会强行归类。增强、债券、红利、价值、质量、成长、策略等名称会被排除。首次采集后建议检查基金明细及未分类产品，必要时补充代码白名单。

## 数据与单位

- 系列份额单位：亿份。
- 单日变化：最新披露日相对前一披露日的份额变化。
- “七大系列合计”仅代表以上七类指定宽基 ETF，不代表债券、商品等全部 ETF 市场总份额。
- 份额变化不等同于人民币资金净流入/净流出；不同 ETF 净值不同，不能把亿份直接理解为亿元。
- 缺失日期不以前值填充；接口失败、未披露或未识别数据均可能形成缺口。
- 历史回补目标起点为 2024-06-03，能否完整回补取决于交易所接口可用性及历史披露字段。

## 新建 GitHub 仓库部署（推荐）

1. 解压本项目 ZIP。
2. 在 GitHub 新建一个**全新的 Public 仓库**，例如 `etf-share-tracker-full-market`。不要复用旧仓库，旧项目会保持不变。
3. 将解压后文件夹内的**全部内容**上传到新仓库根目录，必须保留隐藏目录 `.github/workflows/`。
4. 在仓库 `Settings → Pages → Build and deployment` 中，将 `Source` 设为 `GitHub Actions`。
5. 在 `Settings → Actions → General → Workflow permissions` 中选择 `Read and write permissions` 并保存。
6. 打开 `Actions → Update ETF data and deploy website`，点击 `Run workflow`，分支选择 `main`。
7. 首次回补可能较慢。进入该次运行详情，检查 `Update ETF data and regenerate HTML` 步骤日志；全部步骤绿色成功后，在 `Settings → Pages` 或 Actions 部署摘要查看网站 URL。

网站地址通常为：`https://你的GitHub用户名.github.io/仓库名/`。

## 自动更新时间

工作流配置两个 UTC cron：

- `50 15 * * 1-5`：北京时间周一至周五 23:50。
- `40 0 * * 2-6`：北京时间周二至周六 08:40，复核前一交易日可能延迟或修订的数据。

GitHub Actions 定时任务可能延迟启动；非交易日通常不会产生新的交易所份额披露。需要即时更新时，可在 Actions 页面手动运行。

## 本地运行

需要 Python 3.10+：

```bash
python -m pip install -r requirements.txt
python update_etf_tracker.py
```

生成或更新 `etf_share_tracker.html` 后，可直接用浏览器打开。也可使用本地 HTTP 服务：

```bash
python update_etf_tracker.py --serve --port 8765
```

然后访问 `http://127.0.0.1:8765`。

首次全量回补可使用：

```bash
python update_etf_tracker.py --force-full
```

## 文件结构

- `update_etf_tracker.py`：原有 ETF 数据更新、核心分析与 HTML 生成入口，并整合宽基采集。
- `broad_market.py`：沪深两市 ETF 数据采集、系列分类、历史缓存和汇总逻辑。
- `etf_share_tracker.html`：浏览器看板模板。
- `echarts.min.js`：本地 ECharts 图表库。
- `data/etf_share_cache.json`：原有核心 ETF 历史缓存。
- `data/broad_etf_cache.json`：全市场宽基 ETF 历史缓存（首次运行自动创建）。
- `.github/workflows/update-and-deploy.yml`：定时更新及 GitHub Pages 自动部署。

## 重要限制与核验建议

本项目通过公开交易所接口采集数据。交易所可能调整接口、字段、分页方式、访问频率或披露时间。代码通过 Python 语法检查不代表已在 GitHub Runner 上验证过所有实时接口。首次运行后请检查：

1. Actions 日志是否显示沪市、深市均成功返回数据；
2. 七个系列各自纳入的 ETF 数量及代码是否合理；
3. `data/broad_etf_cache.json` 是否有日期和基金记录；
4. 页面是否显示最新披露日、系列总份额和明细；
5. `partial_dates`、`errors` 是否提示接口缺口。

如果深交所接口字段变化或返回为空，页面会出现缺失数据，不能把空数据理解为该系列份额为零。请以交易所披露为准。

GitHub Pages 是公开网站。请勿在仓库提交密码、API 密钥或个人隐私。
