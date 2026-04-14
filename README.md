# Vague-Search

一个面向 Excel / WPS 表格文件的搜索采集项目，核心目标是：

- 从 Google 或 Bing 按关键词执行 `filetype:` 搜索
- 下载命中的表格文件
- 基于 Redis 做去重、断点续跑和结果缓存
- 基于 FastText 做语种识别
- 将结果按语种归档，便于后续清洗和分析

这个仓库更接近“内部生产脚本集合”而不是通用 SDK。当前代码以 Windows 本地环境为主，很多路径、Redis 地址、模型文件位置仍采用脚本内直接配置的方式。

## 项目结构

```text
Vague-Search/
├─ README.md
└─ 模糊搜索/
   ├─ 模糊搜索-google/
   │  ├─ google_1.py
   │  ├─ google按语种分类xlsx.py
   │  ├─ scheduler_both_google.py
   │  ├─ lid.176.bin
   │  ├─ url_class_keywords.json
   │  └─ utils/
   │     ├─ __init__.py
   │     ├─ domain_classifier.py
   │     └─ language_detector.py
   ├─ 模糊采集-bing/
   │  ├─ bing_1.py
   │  ├─ bing按语种分类xlsx.py
   │  ├─ scheduler_both_bing.py
   │  ├─ lid.176.bin
   │  ├─ utils/
   │  │  ├─ __init__.py
   │  │  ├─ search_utils.py
   │  │  ├─ download_utils.py
   │  │  ├─ file_utils.py
   │  │  └─ analysis_utils.py
   │  └─ xuehua/
   ├─ json/
   │  ├─ input/
   │  ├─ output/
   │  ├─ output_json/
   │  ├─ 构建加切块.py
   │  └─ 去除翻译失败.py
   └─ utils/
      ├─ json翻译api/
      ├─ redis测试/
      └─ wps_push_tool/
```

## 核心能力

### 1. Google 采集

入口脚本：`模糊搜索/模糊搜索-google/google_1.py`

当前版本特点：

- 使用 DrissionPage 驱动浏览器执行 Google 搜索
- 搜索表达式为 `"<关键词>" filetype:<扩展名>`
- 异步下载文件，按 MD5 重命名
- 用 Redis 记录关键词完成状态、URL 去重状态、结果队列
- 使用 `url_class_keywords.json` 做域名分类
- 使用 FastText 语言模型做语种识别
- 结果写入 Redis 列表，按语种分桶

### 2. Bing 采集

入口脚本：`模糊搜索/模糊采集-bing/bing_1.py`

当前版本特点：

- 使用 Bing 搜索表格文件
- 对 Bing 跳转链接做真实下载地址提取
- 支持下载后按 MD5 去重
- 关键词完成状态与结果写入 Redis
- 通过 FastText 识别文件内容语种
- 在脚本结束时输出 `RUN_RESULT_JSON:...` 结果，便于调度脚本解析

### 3. 下载后按语种归类

相关脚本：

- `模糊搜索/模糊搜索-google/google按语种分类xlsx.py`
- `模糊搜索/模糊采集-bing/bing按语种分类xlsx.py`

用途：

- 读取已下载的表格文件
- 提取单元格文本
- 用 FastText 判断语种
- 将文件移动到目标语种目录
- 将无内容、非目标语种、处理失败的文件分流到单独目录

### 4. 关键词 JSON 预处理

相关脚本：

- `模糊搜索/json/构建加切块.py`
- `模糊搜索/json/去除翻译失败.py`
- `模糊搜索/utils/json翻译api/json翻译.py`

用途：

- 构建原始关键词 JSON
- 按批次切块生成多个输入文件
- 清理翻译失败的数据
- 为搜索阶段准备统一格式的输入

## 数据流

项目的大致执行流程如下：

```text
关键词 JSON
  -> Google / Bing 搜索
  -> 解析搜索结果
  -> 下载 Excel / WPS 文件
  -> MD5 / URL 去重
  -> 语种识别 / 域名分类
  -> Redis results 队列
  -> 本地样张文件目录
  -> 按语种分类归档
```

## 输入数据格式

大部分搜索入口脚本默认读取 JSON 数组，每个元素通常类似：

```json
[
  {
    "外文": "keyword",
    "中文": "keyword",
    "语种": "id",
    "类别": "IT"
  }
]
```

其中最关键的是：

- `外文`
- `中文`
- `语种`
- `类别`

Google 与 Bing 脚本当前主要依赖 `外文` 字段作为搜索关键词。

## Redis 设计

项目大量依赖 Redis 保存运行状态。当前脚本中常见的 key 包括：

- `crawler:keyword_finished:google`
- `crawler:keyword_finished:bing`
- `crawler:seen_url:<lang>`
- `crawler:seen_md5`
- `crawler:results:<lang>`

作用说明：

- `keyword_finished:*`：标记关键词是否已完成，支持断点续跑
- `seen_url:*`：URL 去重
- `seen_md5`：文件内容去重
- `results:*`：结果输出队列，消费者可按语种拉取

## 环境依赖

当前仓库没有统一的 `requirements.txt`，需要按脚本实际依赖自行安装。常见依赖包括：

```bash
pip install requests aiohttp aiofiles redis openpyxl xlrd urllib3 DrissionPage
pip install fasttext
```

说明：

- `fasttext` 用于语种识别
- `DrissionPage` 用于浏览器自动化
- `openpyxl`、`xlrd` 用于读取表格
- 若运行环境缺少浏览器或对应内核，DrissionPage 部分需要额外配置

## 运行前配置

在运行脚本之前，建议先逐项检查这些内容：

### 1. 本地路径

多个脚本仍然使用硬编码路径，例如：

- 下载目录
- 关键词 JSON 路径
- `lid.176.bin` 模型路径
- 分类规则文件路径

以 `google_1.py` 为例，通常需要检查顶部这些全局参数：

- `path`
- `BASE_XLSX_DIR`
- `KEYWORD_PATH`
- `SEARCH_FILE_EXTENSION`
- `ALLOWED_DOWNLOAD_EXTENSIONS`
- `URL_CLASS_CONFIG_PATH`
- `LANGUAGE_MODEL_PATH`
- `INITIAL_URL`

以 `bing_1.py` 为例，通常需要检查顶部这些全局参数：

- `BASE_DIRECTORY`
- `JSON_INPUT_FILE`
- `SEARCH_FILE_TYPE`
- `TIME_FILTER`
- `MAX_CONCURRENT_WORKERS`
- `ALLOWED_EXTENSIONS`
- `FASTTEXT_MODEL_PATH`

### 2. Redis 连接

当前脚本默认使用：

```text
host = 10.229.32.166
port = 6379
db   = 5
```

如果你的环境不同，需要先修改脚本里的 Redis 配置。

### 3. 模型文件

语种识别依赖 `lid.176.bin`，Google 和 Bing 目录下各放了一份。运行前要确认文件存在且路径正确。

### 4. 浏览器自动化环境

Google 与 Bing 搜索依赖浏览器自动化。常见风险包括：

- 浏览器未安装
- 版本不兼容
- 无头模式下页面结构变化
- 验证码拦截
- 网络访问 Google / Bing 受限

## 运行方式

### 1. 运行 Google 单脚本采集

```bash
python 模糊搜索/模糊搜索-google/google_1.py
```

适用于：

- 单个 JSON 输入文件
- 本地调试 Google 采集逻辑
- 调整下载目录、扩展名、语种配置后直接运行

### 2. 运行 Bing 单脚本采集

```bash
python 模糊搜索/模糊采集-bing/bing_1.py
```

适用于：

- 单个 JSON 输入文件
- 本地调试 Bing 搜索和下载链路
- 调试 Redis 写入与 `RUN_RESULT_JSON` 输出

### 3. 运行按语种归类脚本

```bash
python 模糊搜索/模糊搜索-google/google按语种分类xlsx.py
python 模糊搜索/模糊采集-bing/bing按语种分类xlsx.py
```

适用于：

- 下载完成后的样张整理
- 将文件转移到目标语种目录
- 清理无内容或非目标语种文件

## 调度脚本说明

仓库中包含：

- `模糊搜索/模糊搜索-google/scheduler_both_google.py`
- `模糊搜索/模糊采集-bing/scheduler_both_bing.py`

它们的设计思路是：

- 批量读取一个目录下的多个 JSON 文件
- 并发调度两个搜索脚本
- 通过 `RUN_RESULT_JSON:` 前缀解析子进程输出
- 未完成时按重试策略再次执行

但需要注意：

- 当前仓库里可以明确看到 `google_1.py` 和 `bing_1.py`
- 调度脚本中仍引用 `google_2.py/google_3.py`、`bing_2.py/bing_3.py`
- 如果这些脚本不在你的本地工作区中，调度脚本将无法直接运行

换句话说，当前仓库更适合先使用 `google_1.py`、`bing_1.py` 单独运行，再按你的实际文件情况调整调度器。

## 输出结果

### 1. 本地文件

下载后的样张文件通常存放在：

- Google：`样张文件`
- Bing：`样张文件`

文件名通常被重命名为：

```text
<md5>.<ext>
```

### 2. Redis 结果队列

结果会被写入：

```text
crawler:results:<语言>
```

单条结果大致包含：

- `webSite`
- `crawlTime`
- `srcUrl`
- `title`
- `hash`
- `extend.keyword`
- `extend.language`
- `extend.doMain`
- `extend.type`

### 3. 运行结果输出

`bing_1.py` 会在结束时输出：

```text
RUN_RESULT_JSON:{...}
```

这主要是为了让调度脚本读取当前执行状态。

## 已知特点与限制

这个项目当前更偏向“可用脚本集”，不是完全产品化的通用工程。你在使用时需要特别注意：

- 路径大量写死，需要手动修改
- Redis 地址写死，需要手动修改
- 不同脚本的配置风格尚未完全统一
- 调度脚本与实际存在的入口脚本可能不完全一致
- 对 Google / Bing 页面结构比较敏感
- 长时间运行容易受到验证码、网络波动、页面改版影响
- 当前仓库没有统一依赖文件和自动化测试

## 推荐使用方式

如果你是第一次接手这个仓库，建议按下面顺序使用：

1. 先准备关键词 JSON，并确认 `外文` 字段正确。
2. 修改 `google_1.py` 或 `bing_1.py` 顶部全局配置。
3. 确认 Redis、FastText 模型、浏览器环境都可用。
4. 先跑单脚本，验证搜索、下载、去重、结果写入链路。
5. 下载完成后，再运行按语种分类脚本整理文件。
6. 最后再决定是否启用或改造调度脚本。

## 后续建议

如果准备继续维护这个项目，优先建议做这些事：

1. 增加 `requirements.txt` 或 `pyproject.toml`
2. 将所有硬编码路径改为命令行参数或配置文件
3. 统一 Google / Bing 的输出协议
4. 为 Redis key、JSON 输入输出格式补充更严格的文档
5. 修正调度脚本与实际入口脚本不一致的问题
6. 增加最基本的运行检查和错误恢复机制

---

如果你接下来准备继续整理这个仓库，最值得优先统一的文件通常是：

- `模糊搜索/模糊搜索-google/google_1.py`
- `模糊搜索/模糊采集-bing/bing_1.py`
- `模糊搜索/模糊搜索-google/scheduler_both_google.py`
- `模糊搜索/模糊采集-bing/scheduler_both_bing.py`
