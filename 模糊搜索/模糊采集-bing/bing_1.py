import subprocess
import time
import re
import os
import random
import logging
import urllib3
import json
import requests
from concurrent.futures import ThreadPoolExecutor
import threading
from pathlib import Path
import sys
import queue
from typing import Dict, List, Optional
from datetime import datetime
import traceback
import hashlib
import portalocker

# 尝试导入FastText
try:
    import fasttext
    fasttext_available = True
except ImportError:
    fasttext = None
    fasttext_available = False

# 导入DrissionPage异常
from DrissionPage.errors import PageDisconnectedError, BrowserConnectError

# 添加gen_md5.py所在目录
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from xuehua.gen_md5 import get_snowflake_id

# 导入工具模块
from utils.search_utils import create_browser_page, initialize_browser_for_search, \
    search_keyword_with_existing_page, SearchBoxNotFoundException
from utils.download_utils import download_file
from utils.file_utils import calculate_md5, generate_filename_from_md5, JSONLManager

# 禁用证书警告
urllib3.disable_warnings()

# ========================== 0. 全局上下文（用于WPS消息自动带四参数） ==========================
CURRENT_KEYWORD_FILE = ""
CURRENT_JSONL_FILENAME = ""
CURRENT_SCRIPT_PATH = os.path.abspath(__file__)
CURRENT_FINISHED_FILE = ""


def sanitize_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', '_', str(name), flags=re.UNICODE)


def safe_name(name: str) -> str:
    v = sanitize_filename(name).strip()
    return v if v else "未知"


def extract_lang_from_path(input_path: str) -> str:
    """
    从 json 路径提取语种：
    - 优先 output 后一级目录：...\\output\\法语\\xxx.json -> 法语
    - 兜底：父目录名
    """
    p = os.path.normpath(input_path)
    parts = p.split(os.sep)
    lang = ""
    try:
        idx = parts.index("output")
        if idx + 1 < len(parts):
            lang = parts[idx + 1]
    except ValueError:
        lang = ""

    if not lang:
        lang = os.path.basename(os.path.dirname(p))

    return safe_name(lang)


def build_finished_json_path(base_dir: str, json_input_path: str) -> Path:
    lang = extract_lang_from_path(json_input_path)
    return Path(base_dir) / f"{lang}_finished_keywords.json"


# ========================== 1. WPS 群机器人通知模块 ==========================
WPS_NOTIFY_ENABLED = True
WPS_ROBOT_WEBHOOK = "https://365.kdocs.cn/woa/api/v1/webhook/send?key=add87b4b34f7ecaebf14c4e133ab9d5c"

WPS_THROTTLE_SECONDS = 60
_LAST_WPS_TS: Dict[str, float] = {}

STATE = {
    "current_keyword": "无",
    "status": "初始化",
    "success_count": 0
}


def send_wps_robot(content: str, throttle_key="default") -> bool:
    """基础发送函数"""
    if not WPS_NOTIFY_ENABLED:
        return False
    now = time.time()
    last_ts = _LAST_WPS_TS.get(throttle_key, 0)
    # 强制发送 final, captcha, stop 类型的消息
    if throttle_key not in ["final", "captcha", "start", "stop"] and now - last_ts < WPS_THROTTLE_SECONDS:
        return False
    _LAST_WPS_TS[throttle_key] = now

    try:
        payload = {"msgtype": "text", "text": {"content": content}}
        resp = requests.post(WPS_ROBOT_WEBHOOK, json=payload, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        print(f"❌ WPS机器人异常: {e}")
        return False


def send_formatted_wps_msg(title: str, level: str = "info", fields: Dict = None, extra_text: str = "",
                           throttle_key: str = "default") -> bool:
    """
    统一格式化发送消息（自动注入四个参数）
    - 关键词文件 / JSONL文件 / py脚本 / finished文件
    """
    icons = {"start": "🚀", "success": "✅", "error": "🚨", "warning": "🔔", "info": "📝", "stop": "🛑"}
    icon = icons.get(level, "📝")

    # --- 自动注入四个参数 ---
    base_fields = {
        "关键词文件": CURRENT_KEYWORD_FILE or "未知",
        "JSONL文件": CURRENT_JSONL_FILENAME or "未知",
        "py脚本": CURRENT_SCRIPT_PATH or "未知",
        "finished文件": CURRENT_FINISHED_FILE or "未知",
    }

    merged_fields = {}
    merged_fields.update(base_fields)
    if fields:
        merged_fields.update(fields)  # 调用处传入的字段覆盖同名字段

    lines = [f"{icon} 【{title}】", "━━━━━━━━━━━━━━"]

    for k, v in merged_fields.items():
        lines.append(f"• {k}: {str(v).strip()}")

    if extra_text:
        lines.append("━━━━━━━━━━━━━━")
        clean_extra = extra_text.strip()
        if len(clean_extra) > 500:
            lines.append(f"...(前略)\n{clean_extra[-500:]}")
        else:
            lines.append(clean_extra)

    return send_wps_robot("\n".join(lines), throttle_key)


def get_error_context(e: Exception) -> str:
    """提取报错简要信息和行号"""
    try:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        current_file = os.path.basename(__file__)
        error_line = "未知"
        tb = exc_traceback
        while tb:
            frame = tb.tb_frame
            code = frame.f_code
            if os.path.basename(code.co_filename) == current_file:
                error_line = tb.tb_lineno
            tb = tb.tb_next
        return f"Line {error_line}: {str(e)}"
    except Exception:
        return str(e)


# ========================== 2. 彩色日志配置 ==========================
RESET, WHITE, YELLOW, GREEN, RED, BLUE = "\033[0m", "\033[37m", "\033[33m", "\033[32m", "\033[31m", "\033[34m"


class ColorFormatter(logging.Formatter):
    YELLOW_PATTERNS = ("启动浏览器", "配置", "初始化")
    BLUE_PATTERNS = ("执行搜索", "搜索关键词")
    GREEN_PATTERNS = ("下载成功", "搜索完成", "任务结束", "全部完成", "WPS通知发送成功", "改名完成")
    RED_PATTERNS = ("失败", "出错", "异常", "断开", "未找到", "崩溃", "停止", "改名失败")

    def format(self, record: logging.LogRecord) -> str:
        msg = super().format(record)
        if record.levelno >= logging.WARNING:
            return f"{RED}{msg}{RESET}"
        if any(p in msg for p in self.GREEN_PATTERNS):
            return f"{GREEN}{msg}{RESET}"
        if any(p in msg for p in self.BLUE_PATTERNS) and "搜索完成" not in msg:
            return f"{BLUE}{msg}{RESET}"
        if any(p in msg for p in self.YELLOW_PATTERNS):
            return f"{YELLOW}{msg}{RESET}"
        if any(p in msg for p in self.RED_PATTERNS):
            return f"{RED}{msg}{RESET}"
        return f"{WHITE}{msg}{RESET}"


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers.clear()
handler = logging.StreamHandler()
handler.setFormatter(ColorFormatter('[%(asctime)s] %(levelname)s - %(message)s'))
logger.addHandler(handler)


# ========================== 3. 爬虫逻辑 ==========================

class CrawlerConfig:
    MAX_PAGES_PER_KEYWORD = 15
    BROWSER_INIT_URL = "https://cn.bing.com/search?q=科技"
    BROWSER_RESTART_INTERVAL = 20
    MAX_SEARCHBOX_NOT_FOUND = 5
    CHROMIUM_CONFIG_NAME = 'fast_search'
    CHROMIUM_HEADLESS = False
    DEFAULT_MAX_WORKERS = 1
    DOWNLOAD_WORKERS = 10
    SEARCH_DOWNLOAD_PARALLEL = True


class DrissionPageCrawlerManager:
    LANG_MAP = {
        '__label__zh': '中文', '__label__en': '英语', '__label__es': '西班牙语',
        '__label__fr': '法语', '__label__de': '德语', '__label__ja': '日语',
        '__label__ko': '韩语', '__label__ru': '俄语', '__label__pt': '葡萄牙语',
        '__label__it': '意大利语', '__label__ar': '阿拉伯语'
    }

    def __init__(self, base_dir: str, max_workers: int = 5, fasttext_model_path: Optional[str] = None,
                 allowed_extensions: Optional[List[str]] = None, json_input_file: str = ""):
        self.base_dir = Path(base_dir)
        self.max_workers = max_workers
        self.seen_urls = set()
        self.global_seen_urls = set()
        self.seen_md5_hashes = set()
        self.lock = threading.RLock()

        self.download_executor = None
        self.download_running = False
        self.download_futures = []

        self.allowed_extensions = set()
        self.json_input_file = json_input_file

        if allowed_extensions:
            for ext in allowed_extensions:
                clean_ext = ext.lower().strip()
                if not clean_ext.startswith('.'):
                    clean_ext = '.' + clean_ext
                self.allowed_extensions.add(clean_ext)

        self.language_model = None
        if fasttext_available and fasttext_model_path and os.path.exists(fasttext_model_path):
            try:
                self.language_model = fasttext.load_model(fasttext_model_path)
                logging.info("📘 FastText语言检测模型加载成功")
            except Exception as e:
                logging.error(f"FastText模型加载失败: {e}")

        self.jsonl_dir = self.base_dir / "jsonl文件"
        self.download_dir = self.base_dir / "样张文件"
        self.jsonl_dir.mkdir(parents=True, exist_ok=True)
        self.download_dir.mkdir(parents=True, exist_ok=True)

        self.jsonl_file = self.find_or_create_jsonl_file()
        self.jsonl_manager = JSONLManager(self.jsonl_file)

        self.load_existing_md5_hashes()
        self.load_global_seen_urls()

        # finished 文件按语种拆分
        if self.json_input_file:
            self.finished_keywords_file = build_finished_json_path(str(self.base_dir), self.json_input_file)
        else:
            self.finished_keywords_file = self.base_dir / "finished_keywords.json"

        self.finished_keywords = self.load_finished_keywords()

        global CURRENT_JSONL_FILENAME
        CURRENT_JSONL_FILENAME = self.jsonl_file.name

        self._finished_last_reload_ts = 0
        self._finished_reload_interval = 10  # 秒

    def load_global_seen_urls(self):
        logging.info("📚 正在加载全局历史URL记录...")
        count = 0
        if self.jsonl_dir.exists():
            for fp in self.jsonl_dir.glob("*.jsonl"):
                try:
                    with open(fp, 'r', encoding='utf-8') as f:
                        for line in f:
                            if not line.strip():
                                continue
                            try:
                                data = json.loads(line)
                                if data.get('srcUrl'):
                                    self.global_seen_urls.add(data['srcUrl'])
                                    count += 1
                            except Exception:
                                pass
                except Exception:
                    pass
        logging.info(f"✅ 全局去重库构建完成，共包含 {count} 条历史URL")

    def get_language_folder(self, text: str) -> str:
        if not self.language_model or not text:
            return "其他语言"
        try:
            clean_text = text.replace('\n', ' ').strip()
            if not clean_text:
                return "其他语言"
            predictions = self.language_model.predict(clean_text)
            if predictions and predictions[0]:
                return self.LANG_MAP.get(predictions[0][0], "其他语言")
        except Exception:
            pass
        return "其他语言"

    def find_or_create_jsonl_file(self) -> Path:
        try:
            if self.json_input_file:
                norm_path = os.path.normpath(os.path.abspath(self.json_input_file)).lower()
                md5_name = hashlib.md5(norm_path.encode("utf-8")).hexdigest()
                jsonl_path = self.jsonl_dir / f"{md5_name}.jsonl"
                logging.info(f"📝 使用基于输入文件的JSONL: {jsonl_path.name}")
                return jsonl_path

            jsonl_files = list(self.jsonl_dir.glob("*.jsonl"))
            if jsonl_files:
                return max(jsonl_files, key=lambda f: f.stat().st_mtime)

            snowflake_id = get_snowflake_id(11, 1)
            return self.jsonl_dir / f"{snowflake_id}.jsonl"
        except Exception:
            return self.jsonl_dir / f"{int(time.time())}.jsonl"

    def start_download_executor(self):
        if not self.download_running and CrawlerConfig.SEARCH_DOWNLOAD_PARALLEL:
            self.download_executor = ThreadPoolExecutor(max_workers=CrawlerConfig.DOWNLOAD_WORKERS)
            self.download_running = True
            logging.info("🚀 启动全局下载线程池")

    def stop_download_executor(self):
        if self.download_running and self.download_executor:
            self.download_executor.shutdown(wait=True)
            self.download_running = False
            logging.info("✅ 全局下载线程池已关闭")

    def add_download_task(self, result: Dict, keyword: str, idx: int):
        if CrawlerConfig.SEARCH_DOWNLOAD_PARALLEL and self.download_running:
            self.download_futures.append(
                self.download_executor.submit(self.process_single_result_with_callback, result, keyword, idx)
            )
        else:
            self.process_single_result(result)

    def process_single_result_with_callback(self, result: Dict, keyword: str, idx: int) -> Optional[Dict]:
        processed_result = self.process_single_result(result)
        if processed_result:
            self.jsonl_manager.write_record(processed_result)
            STATE["success_count"] += 1
            logging.info(f"✅ [KWD-{idx}] 下载成功: {processed_result.get('title', '')[:20]}...")
        return processed_result

    def load_existing_md5_hashes(self):
        count = 0
        try:
            if self.download_dir.exists():
                for file_path in self.download_dir.rglob('*'):
                    if file_path.is_file() and len(file_path.name) > 32 and '.' in file_path.name:
                        self.seen_md5_hashes.add(file_path.name.split('.')[0].lower())
                        count += 1
        except Exception:
            pass
        logging.info(f"🔒 已加载 {count} 个文件MD5哈希")

    def is_md5_hash_exists(self, md5_hash: str) -> bool:
        return md5_hash.lower() in self.seen_md5_hashes

    def choose_keyword(self, item: Dict) -> Optional[str]:
        for candidate in [item.get('外文'), item.get('中文')]:
            if candidate and str(candidate).strip():
                return str(candidate).strip()
        return None

    def load_finished_keywords(self) -> set:
        try:
            if self.finished_keywords_file.exists():
                with open(self.finished_keywords_file, 'r', encoding='utf-8') as f:
                    return set(json.load(f).get('finished_keywords', []))
        except Exception:
            pass
        return set()

    import portalocker
    import json
    import os
    from pathlib import Path

    def save_finished_keyword(self, keyword: str):
        """
        多进程安全写入 finished_keywords_file
        - 跨进程文件锁：避免同时写
        - 写前重读：避免丢更新（lost update）
        - 临时文件 + os.replace：原子替换，避免写坏 JSON
        """
        # lock 文件与 json 同目录，名字固定，所有进程争抢同一个锁
        lock_path = str(self.finished_keywords_file) + ".lock"
        Path(lock_path).parent.mkdir(parents=True, exist_ok=True)

        try:
            # timeout: 等待锁的最长秒数（按你任务实际情况可调大些）
            with portalocker.Lock(lock_path, timeout=30):

                # 1) 写入前重读最新 finished 文件（关键：防止覆盖别人刚写的）
                finished = set()
                if self.finished_keywords_file.exists():
                    try:
                        with open(self.finished_keywords_file, "r", encoding="utf-8") as f:
                            finished = set(json.load(f).get("finished_keywords", []))
                    except Exception:
                        # 读失败（可能文件刚好被替换、或历史写坏），就当空集合重新构建
                        finished = set()

                # 2) 更新集合
                finished.add(keyword)

                # 3) 写入临时文件
                tmp_path = str(self.finished_keywords_file) + ".tmp"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump({"finished_keywords": sorted(finished)}, f, ensure_ascii=False, indent=2)
                    f.flush()
                    os.fsync(f.fileno())  # 强制落盘，降低断电/崩溃造成空文件概率

                # 4) 原子替换（同一磁盘分区内是原子的）
                os.replace(tmp_path, self.finished_keywords_file)

                # 5) 同步内存（仅本进程）
                with self.lock:
                    self.finished_keywords = finished

        except portalocker.exceptions.LockException as e:
            logging.error(f"❌ finished 文件加锁失败(超时/异常)：{e}")
        except Exception as e:
            logging.error(f"❌ finished 写入异常：{e}")

    def is_keyword_finished(self, keyword: str) -> bool:
        now = time.time()
        if now - getattr(self, "_finished_last_reload_ts", 0) > getattr(self, "_finished_reload_interval", 10):
            self.finished_keywords = self.load_finished_keywords()
            self._finished_last_reload_ts = now
        return keyword in self.finished_keywords

    def is_allowed_file_type(self, file_path: str) -> bool:
        if not self.allowed_extensions:
            return True
        return Path(file_path).suffix.lower() in self.allowed_extensions

    def extract_real_download_url_with_requests(self, session, download_link: str) -> str:
        if "www.bing.com/ck" in download_link:
            try:
                resp = session.get(download_link, timeout=(5, 10), allow_redirects=True)
                match = re.search(r'var\s+u\s*=\s*"([^"]+)"', resp.text)
                if match:
                    return match.group(1)
            except Exception:
                pass
        return download_link

    def process_single_result(self, result: Dict) -> Optional[Dict]:
        url = result['srcUrl']
        with self.lock:
            if url in self.seen_urls or url in self.global_seen_urls:
                return None
            self.seen_urls.add(url)

        try:
            with requests.Session() as session:
                session.headers.update({'User-Agent': 'Mozilla/5.0 ...'})
                real_url = self.extract_real_download_url_with_requests(session, url)

            detect_text = (result.get('title', '') + " " + result.get('abstract', ''))
            lang_folder_name = self.get_language_folder(detect_text)

            lang_save_dir = self.download_dir / lang_folder_name
            lang_save_dir.mkdir(parents=True, exist_ok=True)

            temp_filename = f"temp_{int(time.time())}_{random.randint(1000, 9999)}"
            ext = result['extend']['type']
            if ext and not ext.startswith('.'):
                ext = '.' + ext
            temp_filename += ext if ext else ".unknown"

            temp_save_path = lang_save_dir / temp_filename

            if not download_file(real_url, str(temp_save_path)):
                return None

            # ============ ✅ 下载后处理（带MD5改名日志 + 不残留temp） ============
            logging.info(f"📥 下载落盘完成(temp)：{temp_save_path}")

            if not self.is_allowed_file_type(str(temp_save_path)):
                logging.info(f"🗑️ 非允许后缀，删除：{temp_save_path.name}")
                if temp_save_path.exists():
                    temp_save_path.unlink()
                return None

            md5_hash = calculate_md5(str(temp_save_path))
            logging.info(f"🔑 计算MD5：{temp_save_path.name} -> {md5_hash}")

            if not md5_hash:
                logging.warning(f"⚠️ MD5为空，删除temp：{temp_save_path}")
                if temp_save_path.exists():
                    temp_save_path.unlink()
                return None

            if self.is_md5_hash_exists(md5_hash):
                logging.info(f"🔁 MD5已存在(去重命中)：{md5_hash}，删除temp：{temp_save_path}")
                if temp_save_path.exists():
                    temp_save_path.unlink()
                return None

            final_filename = generate_filename_from_md5(md5_hash, result['extend']['type'])
            final_save_path = lang_save_dir / final_filename

            logging.info(f"🧾 生成最终文件名：{final_filename}")
            logging.info(f"➡️ 准备改名：{temp_save_path} -> {final_save_path} | 目标是否存在={final_save_path.exists()}")

            if final_save_path.exists():
                logging.info(f"📌 目标已存在，删除temp并跳过：{final_save_path}")
                if temp_save_path.exists():
                    temp_save_path.unlink()
                with self.lock:
                    self.seen_md5_hashes.add(md5_hash.lower())
                return None

            try:
                if temp_save_path.exists():
                    os.replace(str(temp_save_path), str(final_save_path))
                logging.info(f"✅ 改名完成(MD5)：{final_save_path}")
            except Exception as e:
                logging.error(f"❌ 改名失败：{temp_save_path} -> {final_save_path} | {e}")
                if temp_save_path.exists():
                    temp_save_path.unlink()
                return None

            with self.lock:
                self.seen_md5_hashes.add(md5_hash.lower())

            result['hash'] = md5_hash
            result['localPath'] = str(final_save_path)
            result['extend']['language'] = lang_folder_name
            return result
            # ================================================================

        except Exception as e:
            logging.error(f"处理结果出错 {url}: {e}")
            return None

    def _is_browser_alive(self, page) -> bool:
        if not page:
            return False
        try:
            _ = page.url
            return True
        except (PageDisconnectedError, BrowserConnectError, Exception):
            return False

    def process_all_keywords_with_single_browser(self, pending_items: List, type_: str, time_: str):
        page = None
        processed_count = 0
        searchbox_not_found_count = 0

        try:
            for item_idx, (idx, item) in enumerate(pending_items):
                STATE["current_keyword"] = self.choose_keyword(item)

                if page and not self._is_browser_alive(page):
                    err_msg = "❌ 检测到浏览器连接断开，程序停止。"
                    logging.critical(err_msg)
                    send_formatted_wps_msg("浏览器断开", level="error", extra_text=err_msg)
                    return

                need_restart = (page is None) or \
                               ((processed_count % CrawlerConfig.BROWSER_RESTART_INTERVAL == 0 and processed_count > 0)
                                ) or (searchbox_not_found_count >= CrawlerConfig.MAX_SEARCHBOX_NOT_FOUND)

                if need_restart:
                    if page:
                        try:
                            page.quit()
                            time.sleep(2)
                        except Exception:
                            pass

                    if searchbox_not_found_count >= CrawlerConfig.MAX_SEARCHBOX_NOT_FOUND:
                        send_formatted_wps_msg(
                            "浏览器连续异常",
                            level="warning",
                            extra_text=f"连续 {searchbox_not_found_count} 次找不到搜索框，已尝试重启。"
                        )

                    searchbox_not_found_count = 0
                    logging.info(f"🔄 正在启动浏览器 (批次 {(processed_count // 20) + 1})...")

                    try:
                        page = create_browser_page(
                            config_name='fast_search' if CrawlerConfig.CHROMIUM_HEADLESS else 'visible_search',
                            headless=CrawlerConfig.CHROMIUM_HEADLESS,
                            enable_proxy=False
                        )
                        if not initialize_browser_for_search(page, CrawlerConfig.BROWSER_INIT_URL):
                            send_formatted_wps_msg("浏览器初始化失败", level="error", extra_text="无法打开初始页面")
                            return
                    except Exception as e:
                        logging.error(f"浏览器启动失败: {e}")
                        return

                keyword = STATE["current_keyword"]
                if not keyword or self.is_keyword_finished(keyword):
                    continue

                try:
                    logging.info(f"🎯 [KWD-{idx}] 搜索: {keyword}")
                    search_results = search_keyword_with_existing_page(
                        page=page, keyword=keyword, type_=type_, time_=time_,
                        language_model=self.language_model,
                        max_pages=CrawlerConfig.MAX_PAGES_PER_KEYWORD
                    )
                    searchbox_not_found_count = 0

                    if search_results:
                        logging.info(f"📥 [KWD-{idx}] 找到 {len(search_results)} 个结果")
                        if CrawlerConfig.SEARCH_DOWNLOAD_PARALLEL:
                            for result in search_results:
                                self.add_download_task(result, keyword, idx)

                    self.save_finished_keyword(keyword)
                    processed_count += 1
                    logging.info(f"🎉 关键词 '{keyword}' 搜索完成")

                except (PageDisconnectedError, BrowserConnectError):
                    logging.critical("❌ 浏览器连接断开，停止运行。")
                    send_formatted_wps_msg("浏览器断开", level="error", extra_text=f"关键词 {keyword} 时断开")
                    return

                except SearchBoxNotFoundException:
                    searchbox_not_found_count += 1
                    logging.error(f"❌ [KWD-{idx}] 未找到搜索框")
                    if searchbox_not_found_count == 1:
                        send_formatted_wps_msg(
                            "验证码警告",
                            level="warning",
                            fields={"关键词": keyword},
                            extra_text="未找到搜索框，可能需要验证码。"
                        )
                    continue

                except Exception as e:
                    logging.error(f"❌ [KWD-{idx}] 异常: {e}")
                    self.save_finished_keyword(keyword)
                    processed_count += 1
                    continue

                time.sleep(1.5)

        except KeyboardInterrupt:
            logging.warning("用户手动停止，正在等待主循环处理...")
            raise
        finally:
            if page:
                try:
                    page.quit()
                except Exception:
                    pass

    def run(self, json_file_path: str, type_: str, time_: str):
        start_dt = datetime.now()
        current_script_path = os.path.abspath(__file__)
        exit_reason = "正常结束"
        error_detail = ""

        global CURRENT_KEYWORD_FILE, CURRENT_SCRIPT_PATH, CURRENT_FINISHED_FILE, CURRENT_JSONL_FILENAME
        CURRENT_KEYWORD_FILE = json_file_path
        CURRENT_SCRIPT_PATH = current_script_path
        CURRENT_JSONL_FILENAME = self.jsonl_file.name
        self.finished_keywords_file = build_finished_json_path(str(self.base_dir), json_file_path)
        CURRENT_FINISHED_FILE = str(self.finished_keywords_file)

        try:
            if not os.path.exists(json_file_path):
                raise FileNotFoundError("文件不存在")
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.finished_keywords = self.load_finished_keywords()

            pending = []
            for idx, item in enumerate(data, start=1):
                keyword = self.choose_keyword(item)
                if keyword and not self.is_keyword_finished(keyword):
                    pending.append((idx, item))

            send_formatted_wps_msg(
                "任务开始 (Bing1)",
                level="start",
                fields={"关键词总数": f"{len(data)}", "待处理": f"{len(pending)}"},
                throttle_key="start"
            )

            if CrawlerConfig.SEARCH_DOWNLOAD_PARALLEL:
                self.start_download_executor()

            logging.info(f"📊 任务统计: 总数 {len(data)} | 待办 {len(pending)}")

            if pending:
                self.process_all_keywords_with_single_browser(pending, type_, time_)
            else:
                logging.info("🎉 所有关键词已完成")

        except KeyboardInterrupt:
            exit_reason = "用户手动停止"
            logging.warning("\n🛑 接收到停止指令，正在生成最终报告...")

        except Exception as e:
            exit_reason = "异常退出"
            error_detail = traceback.format_exc()
            logging.critical(f"程序崩溃: {e}")

        finally:
            if CrawlerConfig.SEARCH_DOWNLOAD_PARALLEL:
                self.stop_download_executor()

            end_dt = datetime.now()
            cost = end_dt - start_dt

            level_map = {"正常结束": "success", "用户手动停止": "stop", "异常退出": "error"}
            level = level_map.get(exit_reason, "info")

            fields = {
                "结束时间": end_dt.strftime("%H:%M:%S"),
                "总耗时": str(cost).split('.')[0],
                "退出原因": exit_reason,
                "本次成功下载": str(STATE["success_count"]),
                "执行脚本": os.path.basename(current_script_path)
            }

            if exit_reason != "正常结束":
                fields["最后关键词"] = STATE.get("current_keyword", "未知")

            if exit_reason == "异常退出":
                fields["错误摘要"] = get_error_context(Exception("异常退出"))

            ok = send_formatted_wps_msg(
                "任务结束 (Bing1)",
                level=level,
                fields=fields,
                extra_text=error_detail,
                throttle_key="final"
            )

            if ok:
                logger.info("WPS通知发送成功")
            else:
                logger.warning("WPS通知发送失败")

            logging.info("🏁 程序结束")

            # if len(pending)!=0:
            #     try:
            #         hello_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hello.py")
            #         subprocess.Popen([sys.executable, hello_path], cwd=os.path.dirname(hello_path))
            #         logging.info(f"🚀 已启动 hello.py: {hello_path}")
            #     except Exception as e:
            #         logging.error(f"❌ 启动 hello.py 失败: {e}")


if __name__ == '__main__':
    base_directory = r"D:\数据采集\data\bing"
    json_input_file = r"E:\Crawler\模糊搜索\模糊搜索\json\output\德语\心理学P.json"
    file_type = 'xlsx'

    crawler = DrissionPageCrawlerManager(
        base_dir=base_directory,
        max_workers=1,
        fasttext_model_path=r"lid.176.bin",
        allowed_extensions=['xlsx', 'xls', 'xlsb', 'xlsm'],
        json_input_file=json_input_file
    )
    crawler.run(json_input_file, file_type, '')
