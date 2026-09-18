# OpenAgent Heat Monitoring

追踪一个或多个 GitHub 仓库随时间的热度变化：Star / Fork / Issue / PR / Contributors / Commit / Release，并提供 FastAPI 接口与 ECharts 仪表盘。

## 数据来源与原理

GitHub API **不提供历史 Star/Fork 数量**，因此本项目采用两条腿走路：

1. **回溯重建（backfill）**：利用带时间戳的明细接口重建历史
   - Stars：`/stargazers` + `Accept: application/vnd.github.star+json` → `starred_at`
     - ⚠️ 该端点**现在必须携带 token**，匿名访问返回 401。无 token 时该流标记为 `auth_required`，Star 总量改由每日快照提供。
   - Forks：`/forks?sort=oldest` → `created_at`
   - Issues / PRs：`/issues?state=all`、`/pulls?state=all` → `created_at` / `closed_at` / `merged_at`
   - Commits：`/commits` → `commit.author.date`
   - Releases：`/releases` → `published_at`
   - Contributors：`/contributors`
2. **每日快照（snapshot）**：每天采集一次当前总量（stars/forks/watchers/open issues/open PRs/releases/contributors），作为长期权威基线。

事件明细存入 SQLite 的 `events` 表，分析层用 Pandas 还原每日/每周/每月的累计值与新增值。累计总量以最新快照为锚点（`总量(t) = 最新快照 - t 之后的新增量`），因此在回溯尚未完成时，绝对量依然准确。

**推荐配置 token**：匿名仅 60 请求/小时，且无法回溯 Star 历史。设置 `GITHUB_TOKEN` 后限流提升到 5000/小时。

## 快速开始

```bash
pip install -r requirements.txt

# 配置 token（匿名仅 60 次/小时，且无法回溯 Star）
cp .env.example .env   # 填入 GITHUB_TOKEN

# 编辑要监控的仓库
vi config.yaml

# 初始化数据库并采集（可反复执行，自动断点续采）
python scripts/collect.py

# 启动仪表盘（等价于 uvicorn app.main:app --reload）
./scripts/start.sh
# 打开 http://127.0.0.1:8000
```

启动脚本支持环境变量：`PORT=9000 HOST=0.0.0.0 ./scripts/start.sh`。

匿名访问限流严格，`config.yaml` 中 `collection.max_pages_per_run` 默认 10，即每次运行每个数据流最多抓 10 页（1000 条）。多次运行 `collect.py` 会从断点继续，直至回溯完成。

### Token 类型很重要

| Token 类型 | 限流 | `/stargazers`（Star 历史） |
| --- | --- | --- |
| 匿名 | 60/h | ❌ 401 |
| Fine-grained PAT | 5000/h | ❌ 403 `Resource not accessible by personal access token` |
| **Classic PAT**（勾选 `public_repo`） | 5000/h | ✅ 可用 |

> 想要完整的 Star 历史回溯，请使用 **Classic PAT**。Fine-grained PAT 目前无法访问 starring/subscribers 端点，此时 `stars` 流会标记为 `permission_denied` 并自动降级为每日快照记录 Star 总量，其余指标不受影响。

## 配置说明（config.yaml）

| 配置项 | 说明 |
| --- | --- |
| `database.url` | 默认 `sqlite:///data/monitoring.db`，可换成 PostgreSQL |
| `github.token_env` | 读取 token 的环境变量名 |
| `github.per_page` | 每页条数，最大 100 |
| `github.rate_limit_floor` | 剩余额度低于该值时停止本轮采集 |
| `collection.max_pages_per_run` | 每个数据流单次运行最多抓取页数 |
| `collection.refresh_recent_pages` | 每轮按 `updated` 倒序回扫的页数，用于捕获 Issue/PR 的关闭/合并状态变化 |
| `repositories` | `owner` / `name` 列表 |

可用环境变量 `MONITORING_CONFIG` 指定配置文件路径，便于多环境部署。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/repos` | 已追踪仓库及事件计数 |
| GET | `/api/repos/{owner}/{name}/summary?start&end` | 各指标区间起止值与变化量 |
| GET | `/api/repos/{owner}/{name}/timeseries?start&end&granularity=daily\|weekly\|monthly&metrics=` | 时间序列 |
| GET | `/api/repos/{owner}/{name}/snapshots?start&end` | 每日快照 |
| GET | `/api/repos/{owner}/{name}/contributors?limit=` | 贡献者排行 |
| GET | `/api/repos/{owner}/{name}/events/recent?limit=` | 最近动态 |
| POST | `/api/collect` | 后台触发采集 |
| GET | `/api/collect/status` | 采集状态 |
| GET | `/api/rate-limit` | 当前 API 限流额度 |

### 指标名

- 新增：`stars_new` `forks_new` `issues_opened` `issues_closed` `prs_opened` `prs_closed` `prs_merged` `commits` `releases_new`
- 累计/存量：`stars_total` `forks_total` `issues_open` `prs_open` `releases_total` `contributors_total`

## 数据模型

- `repositories`：仓库元信息
- `events`：`(repository_id, event_type, external_id)` 唯一，幂等写入，支持断点续采
- `snapshots`：`(repository_id, snapshot_date)` 唯一，每日总量
- `contributors`：当前贡献者列表
- `sync_state`：每个仓库每个数据流的回溯游标

## 定时采集

### GitHub Actions（推荐）

`.github/workflows/collect.yml` 已配置为每天 **北京时间 10:00 和 17:00** 自动采集（cron 使用 UTC：`0 2 * * *` 与 `0 9 * * *`），也支持在 Actions 页面手动触发。

数据库持久化到 `data` 分支：每次运行前从该分支恢复 `data/monitoring.db`，采集后提交回去，因此历史数据不会丢失。

需要在仓库 **Settings → Secrets and variables → Actions** 中配置：

- `GH_MONITOR_TOKEN`（可选）：填一个 Classic PAT（`public_repo`），用于 Star 历史回溯。未配置时回退到 Actions 默认的 `GITHUB_TOKEN`（可正常采集除 Star 历史外的指标）。

首次运行会自动创建 `data` 分支。工作流需要 `contents: write` 权限（已在文件中声明）。

### 本地 cron

```cron
0 2,9 * * * cd /path/to/openagent-heat-monitoring && python scripts/collect.py >> data/collect.log 2>&1
```

## 测试

```bash
pytest -q
```
