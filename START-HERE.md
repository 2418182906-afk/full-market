# 从这里开始

这是一个**独立的新项目包**，不会自动修改你之前的 `etracker` 仓库。

## 你要做的事情

1. 解压 `etf-share-tracker-full-market.zip`。
2. GitHub 网页端新建 Public 仓库，例如 `etf-share-tracker-full-market`。
3. 将解压后文件夹里的全部文件上传到新仓库根目录（包括 `.github` 隐藏目录）。
4. 按 `README-DEPLOY.md` 完成 Pages 与 Actions 权限设置。
5. 在 Actions 中手动运行一次工作流，查看日志和页面数据。

不要把整个外层文件夹再套一层上传；仓库根目录应直接看到 `update_etf_tracker.py`、`broad_market.py`、`etf_share_tracker.html`、`requirements.txt`、`data/` 和 `.github/`。
