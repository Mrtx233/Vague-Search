import time
import os
import random
import logging
import urllib3
import json
import requests
import redis
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from pathlib import Path
import sys
from typing import Dict, List, Optional
from urllib.parse import urlparse
from datetime import datetime

from utils.search_utils import search_keyword
from utils.download_utils import download_file
from utils.file_utils import calculate_md5, generate_filename_from_md5

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# WPS 推送：从「模糊搜索/utils」显式加载，避免与本地 utils（search_utils 等）冲突
def _load_wps_push():
    try:
        import importlib.util
        _wps_path = os.path.join(PROJECT_ROOT, "utils", "wps推送", "wps_push.py")
        if not os.path.isfile(_wps_path):
            raise FileNotFoundError(f"wps_push 不存在: {_wps_path}")
        spec = importlib.util.spec_from_file_location("wps_push", _wps_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.send_wps_robot, mod.notify_event
    except Exception as e:
        logging.warning("WPS 推送模块加载失败（将使用空实现）: %s", e)
        return None, None

_send_wps_robot, _notify_event = _load_wps_push()

if _send_wps_robot is not None and _notify_event is not None:
    send_wps_robot = _send_wps_robot
    notify_event = _notify_event
else:
    def send_wps_robot(content: str, throttle_key: str = "default", timeout: int = 10) -> bool:
        return False

    def notify_event(
        event_title: str,
        start_dt: datetime,
        config: Dict,
        extra: str = "",
        throttle_key: str = "event",
        error_detail: str = "",
        jsonl_filename: Optional[str] = None,
        script_name: Optional[str] = None,
    ) -> bool:
        return False

try:
    import fasttext

    fasttext_available = True
except ImportError:
    fasttext = None
    fasttext_available = False
    logging.warning("FastText未安装，语言检测功能将被禁用")


urllib3.disable_warnings()
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')

REDIS_HOST = "10.229.32.166"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PREFIX = "crawler"
PROGRESS_REPORT_INTERVAL_SECONDS = 3600

rds = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def resolve_json_input_file() -> str:
    if len(sys.argv) > 1 and sys.argv[1].strip():
        return os.path.abspath(sys.argv[1].strip())
    raise SystemExit("请传入json文件绝对路径参数，例如: python bing_3.py E:\\path\\to\\xx.json")


def finished_key_bing() -> str:
    return f"{REDIS_PREFIX}:keyword_finished:bing"


def is_finished_bing(keyword: str) -> bool:
    return bool(rds.sismember(finished_key_bing(), keyword))


def mark_finished_bing(keyword: str) -> bool:
    return rds.sadd(finished_key_bing(), keyword) == 1


def finished_count_bing() -> int:
    try:
        return int(rds.scard(finished_key_bing()))
    except Exception:
        return 0


def seen_url_key_bing(lang: str) -> str:
    return f"{REDIS_PREFIX}:seen_url:bing:{lang}"


def is_new_bing_url(lang: str, url: str) -> bool:
    return rds.sadd(seen_url_key_bing(lang), url) == 1


def seen_md5_key_bing() -> str:
    return f"{REDIS_PREFIX}:seen_md5"


def claim_bing_md5(md5_hash: str) -> bool:
    if not md5_hash:
        return False
    return rds.sadd(seen_md5_key_bing(), md5_hash.lower()) == 1


def rollback_bing_md5(md5_hash: str) -> None:
    if not md5_hash:
        return
    try:
        rds.srem(seen_md5_key_bing(), md5_hash.lower())
    except Exception:
        pass


def result_key_bing(lang: str) -> str:
    return f"{REDIS_PREFIX}:results:bing:{lang}"


def push_result_line_bing(lang: str, json_obj: Dict) -> None:
    line = json.dumps(json_obj, ensure_ascii=False, separators=(',', ':'))
    rds.rpush(result_key_bing(lang), line)


# ========== 爬虫配置类（简化版，防止页面卡住） ==========
class CrawlerConfig:
    """爬虫配置类

    说明：
    - 已移除复杂的 chromium_config.py，配置直接集成在代码中
    - 简化了浏览器启动参数，只保留核心必要参数
    - 增加了超时和重试机制，防止页面卡住导致元素识别失败
    - 使用正常加载模式，避免过度优化导致的兼容性问题
    """
    # 搜索配置
    MAX_PAGES_PER_KEYWORD = 15  # 每个关键词最大搜索页数
    BROWSER_INIT_URL = "https://cn.bing.com/search?q=科技"
    BROWSER_RESTART_INTERVAL = 50  # 每搜索多少个关键字后重启浏览器（避免内存泄漏）
    MAX_SEARCHBOX_NOT_FOUND = 5  # 连续未找到搜索框多少次后强制重启浏览器

    # Chromium配置（简化版）
    CHROMIUM_CONFIG_NAME = 'fast_search'  # Chromium配置方案
    CHROMIUM_HEADLESS = False  # 无头模式设置
    # False = 显示浏览器窗口（可以看到搜索过程，便于调试，推荐设置）
    # True  = 无头模式（隐藏窗口，运行更快，但可能更容易出现元素识别问题）

    # 并发配置
    DEFAULT_MAX_WORKERS = 1  # 关键字处理并发数（使用单个浏览器窗口）
    DOWNLOAD_WORKERS = 10  # 下载线程池大小（10个线程并发下载）

    # 搜索和下载并行配置
    SEARCH_DOWNLOAD_PARALLEL = True  # 搜索和下载是否并行执行


class DrissionPageCrawlerManager:
    def __init__(self, base_dir: str, max_workers: int = 5, fasttext_model_path: Optional[str] = None,
                 allowed_extensions: Optional[List[str]] = None):
        self.base_dir = Path(base_dir)
        self.max_workers = max_workers
        self.lock = threading.RLock()

        # 搜索和下载并行相关
        self.download_executor = None  # 全局下载线程池
        self.download_running = False  # 下载线程池是否运行中
        self.download_futures = []  # 下载任务futures列表

        # 允许的文件扩展名（小写）
        self.allowed_extensions = set()
        if allowed_extensions:
            for ext in allowed_extensions:
                # 统一处理为小写，并确保有点号前缀
                clean_ext = ext.lower().strip()
                if not clean_ext.startswith('.'):
                    clean_ext = '.' + clean_ext
                self.allowed_extensions.add(clean_ext)

        # 初始化FastText语言检测模型
        self.language_model = None
        if fasttext_available and fasttext_model_path and os.path.exists(fasttext_model_path):
            try:
                self.language_model = fasttext.load_model(fasttext_model_path)
                logging.info(f"FastText语言检测模型加载成功: {fasttext_model_path}")
            except Exception as e:
                logging.error(f"FastText模型加载失败: {e}")
        else:
            if fasttext_model_path:
                logging.warning(f"FastText模型文件不存在: {fasttext_model_path}")
            logging.warning("语言检测功能将被禁用")

        # 创建必要的目录
        self.download_dir = self.base_dir / "样张文件"
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # 已完成关键字（Redis）
        self.finished_keywords = self.load_finished_keywords()
        self.current_keyword = ""


        # logging.info(f" 已加载 {len(self.finished_keywords)} 个已搜索的关键字（Redis）")
        # logging.info(f" MD5去重Key: {seen_md5_key_bing()}")

    def start_download_executor(self):
        """启动全局下载线程池"""
        if not self.download_running and CrawlerConfig.SEARCH_DOWNLOAD_PARALLEL:
            self.download_executor = ThreadPoolExecutor(max_workers=CrawlerConfig.DOWNLOAD_WORKERS)
            self.download_running = True

    def stop_download_executor(self):
        """停止全局下载线程池"""
        if self.download_running and self.download_executor:
            # 等待所有下载任务完成
            if self.download_futures:
                logging.info(f" 等待 {len(self.download_futures)} 个下载任务完成...")
                for future in as_completed(self.download_futures):
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"下载任务出错: {e}")

            self.download_executor.shutdown(wait=True)
            self.download_running = False
            logging.info(" 全局下载线程池已关闭")

    def add_download_task(self, result: Dict, keyword: str, idx: int):
        """添加下载任务到队列"""
        if CrawlerConfig.SEARCH_DOWNLOAD_PARALLEL and self.download_running:
            future = self.download_executor.submit(
                self.process_single_result_with_callback,
                result,
                self.download_dir,
                keyword,
                idx
            )
            self.download_futures.append(future)
        else:
            # 传统同步下载
            return self.process_single_result(result, self.download_dir)

    def process_single_result_with_callback(self, result: Dict, download_dir: Path, keyword: str, idx: int) -> Optional[
        Dict]:
        """处理单个下载任务，带回调"""
        processed_result = self.process_single_result(result, download_dir)
        if processed_result:
            lang = processed_result.get('extend', {}).get('language', '未知')
            push_result_line_bing(lang, processed_result)
            logging.info(f" [关键字 {idx}] {keyword} - 文件下载完成: {processed_result.get('title', 'Unknown')}")
        return processed_result

    def choose_keyword(self, item: Dict) -> Optional[str]:
        """选择搜索关键词：优先外文，其次中文"""
        candidates = [
            item.get('外文'),
            item.get('外文'),
        ]
        for candidate in candidates:
            if candidate and str(candidate).strip():
                return str(candidate).strip()
        return None

    def load_finished_keywords(self) -> set:
        """从Redis加载已完成的关键字列表"""
        try:
            return set(rds.smembers(finished_key_bing()))
        except Exception as e:
            logging.error(f"加载Redis已完成关键字失败: {e}")
            return set()

    def save_finished_keyword(self, keyword: str):
        """保存已完成的关键字到Redis"""
        with self.lock:
            try:
                if mark_finished_bing(keyword):
                    self.finished_keywords.add(keyword)
                    logging.info(f"✓ 已保存完成的关键字: {keyword}")
            except Exception as e:
                logging.error(f" 保存Redis已完成关键字失败: {keyword} -> {e}")

    def add_finished_keyword(self, keyword: str):
        """添加已完成的关键字到内存集合（不保存文件）"""
        with self.lock:
            self.finished_keywords.add(keyword)
            logging.debug(f"✓ 已标记关键字为完成: {keyword} (总数: {len(self.finished_keywords)})")

    def is_keyword_finished(self, keyword: str) -> bool:
        """检查关键字是否已完成"""
        try:
            return is_finished_bing(keyword)
        except Exception:
            return keyword in self.finished_keywords

    def is_allowed_file_type(self, file_path: str) -> bool:
        """检查文件类型是否在允许的扩展名列表中"""
        if not self.allowed_extensions:
            return True  # 如果没有限制，允许所有类型

        try:
            # 从文件路径提取扩展名
            file_path_obj = Path(file_path)
            file_ext = file_path_obj.suffix.lower()

            return file_ext in self.allowed_extensions
        except Exception as e:
            logging.debug(f"检查文件类型失败: {e}")
            return False

    def extract_real_download_url_with_requests(self, session, download_link: str) -> str:
        """使用requests处理Bing跳转链接，提取真实下载地址"""
        if "www.bing.com/ck" in download_link:  # 检测是否为Bing跳转链接
            logging.debug(f"检测到Bing跳转链接，开始提取真实下载地址: {download_link}")
            try:
                # 请求Bing跳转页面
                redirect_resp = session.get(
                    download_link,
                    timeout=(5, 15),  # 连接超时5s，读取超时15s
                    allow_redirects=True
                )
                redirect_resp.raise_for_status()

                # 用正则匹配页面中的真实下载地址
                import re
                match = re.search(r'var\s+u\s*=\s*"([^"]+)"', redirect_resp.text)
                if not match:
                    raise Exception("未在Bing跳转页面中匹配到真实下载地址")

                # 提取真实下载地址
                real_download_link = match.group(1)
                logging.debug(f"成功提取真实下载地址: {real_download_link}")
                return real_download_link

            except Exception as e:
                logging.warning(f"Bing跳转链接处理失败，使用原链接: {str(e)}")
                return download_link

        return download_link  # 如果不是Bing跳转链接，直接返回原链接

    def process_single_result(self, result: Dict, download_dir: Path) -> Optional[Dict]:
        """处理单个搜索结果：提取真实下载链接并下载文件"""
        url = result['srcUrl']

        lang = result.get('extend', {}).get('language', '未知')
        if not is_new_bing_url(lang, url):
            logging.info(f"URL已处理过，跳过: {url}")
            return None

        try:
            # 使用requests session处理跳转链接，获取真实下载地址
            with requests.Session() as session:
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
                })

                # 处理Bing跳转链接，获取真实下载地址
                real_url = self.extract_real_download_url_with_requests(session, url)

            # 生成临时文件名用于下载
            temp_filename = f"temp_{int(time.time())}_{random.randint(1000, 9999)}"
            if result['extend']['type']:
                ext = result['extend']['type'] if result['extend']['type'].startswith(
                    '.') else f".{result['extend']['type']}"
                temp_filename += ext
            else:
                # 尝试从URL提取扩展名
                parsed = urlparse(real_url)
                path = parsed.path
                if '.' in path:
                    ext = '.' + path.split('.')[-1].lower()
                    temp_filename += ext

            temp_save_path = download_dir / temp_filename

            # 下载文件到临时路径
            if download_file(real_url, str(temp_save_path)):
                # 检查文件类型是否符合要求
                if not self.is_allowed_file_type(str(temp_save_path)):
                    logging.warning(f"文件类型不符合要求，删除文件: {temp_save_path}")
                    if temp_save_path.exists():
                        temp_save_path.unlink()
                    return None  # 不返回结果，不写入JSONL记录

                # 计算MD5
                md5_hash = calculate_md5(str(temp_save_path))
                if md5_hash:
                    md5_hash = md5_hash.lower()

                    # 使用 Redis 全局 MD5 去重
                    if not claim_bing_md5(md5_hash):
                        logging.info(f"MD5已存在（Redis去重），跳过重复下载和记录: MD5={md5_hash}")
                        # 删除临时文件
                        if temp_save_path.exists():
                            temp_save_path.unlink()
                        return None  # 不返回结果，不写入JSONL记录

                    try:
                        # 使用MD5值生成最终文件名
                        final_filename = generate_filename_from_md5(md5_hash, result['extend']['type'])
                        final_save_path = download_dir / final_filename

                        # 重命名文件
                        os.replace(str(temp_save_path), str(final_save_path))
                    except Exception:
                        rollback_bing_md5(md5_hash)
                        raise

                    result['hash'] = md5_hash
                    logging.info(f"文件处理完成: {final_filename} (MD5: {md5_hash})")
                    return result
                else:
                    logging.warning(f"计算MD5失败，删除临时文件: {temp_filename}")
                    if temp_save_path.exists():
                        temp_save_path.unlink()
                    result['hash'] = ""
                    return result
            else:
                logging.error(f"下载失败，跳过: {real_url}")
                return None

        except Exception as e:
            logging.error(f"处理结果时出错 {url}: {e}")
            return None

    def process_keyword_item(self, item: Dict, idx: int, type_: str, time_: str):
        """处理单个关键词项目"""
        keyword = self.choose_keyword(item)
        if not keyword:
            logging.info(f"条目 {idx} 中未找到可搜索的关键字，跳过。")
            return

        # 检查关键字是否已完成
        if self.is_keyword_finished(keyword):
            logging.info(f"[线程] 关键词 {keyword} 已处理过，跳过。")
            return

        logging.info(f" [关键字 {idx}] 开始处理：{keyword}")
        logging.info(f" [关键字 {idx}] 即将打开浏览器窗口进行搜索...")

        try:
            # 搜索获取结果（使用单个浏览器窗口）
            search_results = search_keyword(
                keyword=keyword,
                type_=type_,
                time_=time_,
                language_model=self.language_model,
                max_pages=CrawlerConfig.MAX_PAGES_PER_KEYWORD,
                init_url=CrawlerConfig.BROWSER_INIT_URL,
                headless=CrawlerConfig.CHROMIUM_HEADLESS
            )
            if not search_results:
                logging.warning(f" [Keyword {idx}] {keyword} found no results")
                self.save_finished_keyword(keyword)
                logging.info(f" [Keyword {idx}] no results, marked in Redis finished")
                return

            logging.info(f" [关键字 {idx}] {keyword} 找到 {len(search_results)} 个结果")
            logging.info(f" [关键字 {idx}] 开始使用 {CrawlerConfig.DOWNLOAD_WORKERS} 个线程并发下载...")

            # 并发下载和处理文件
            download_futures = []
            with ThreadPoolExecutor(max_workers=CrawlerConfig.DOWNLOAD_WORKERS) as download_executor:  # 下载线程池
                for result in search_results:
                    future = download_executor.submit(
                        self.process_single_result,
                        result,
                        self.download_dir
                    )
                    download_futures.append(future)

                # 处理下载结果
                success_count = 0
                for future in as_completed(download_futures):
                    try:
                        processed_result = future.result()
                        if processed_result:
                            lang = processed_result.get('extend', {}).get('language', '未知')
                            push_result_line_bing(lang, processed_result)
                            success_count += 1
                        # 下载失败或MD5重复的不写入记录
                    except Exception as e:
                        logging.error(f"处理下载结果时出错: {e}")

            # 只有搜索到内容时才标记关键字为已完成并保存到文件
            self.save_finished_keyword(keyword)
            logging.info(f" [关键字 {idx}] {keyword} 处理完成！")
            logging.info(f"📊 [关键字 {idx}] 成功下载 {success_count} 个文件")
            logging.info(f" [关键字 {idx}] 已保存到 Redis finished 集合")

        except Exception as e:
            logging.error(f" [关键字 {idx}] 处理关键词 {keyword} 时出错：{e}")
            # 发生错误时也保存关键字，避免无限重试
            self.save_finished_keyword(keyword)
            logging.info(f" [关键字 {idx}] 出错已保存到Redis finished集合（避免重试）")

    def process_incomplete_downloads(self):
        """Redis模式下不再需要本地JSONL补偿流程。"""
        # logging.info("Redis模式已启用，跳过本地JSONL未完成补偿流程")

    def process_all_keywords_with_single_browser(self, pending_items: List, type_: str, time_: str):
        """使用单个浏览器窗口处理所有关键词（每20个关键字重启一次浏览器）"""
        from utils.search_utils import create_browser_page, search_keyword_with_existing_page, \
            initialize_browser_for_search, SearchBoxNotFoundException

        page = None
        processed_count = 0  # 已处理的关键字计数器
        restart_interval = CrawlerConfig.BROWSER_RESTART_INTERVAL  # 重启间隔
        searchbox_not_found_count = 0  # 连续搜索框未找到次数
        max_searchbox_failures = CrawlerConfig.MAX_SEARCHBOX_NOT_FOUND  # 最大连续失败次数

        try:
            # 逐个处理关键词
            for item_idx, (idx, item) in enumerate(pending_items):
                # 检查是否需要重启浏览器（定期重启 或 搜索框连续失败）
                need_restart = (processed_count % restart_interval == 0) or (
                            searchbox_not_found_count >= max_searchbox_failures)

                if need_restart:
                    # 关闭旧浏览器（如果存在）
                    if page:
                        try:
                            if searchbox_not_found_count >= max_searchbox_failures:
                                logging.warning(f" 连续 {searchbox_not_found_count} 次未找到搜索框，强制重启浏览器！")
                            else:
                                logging.info(f" 已处理 {processed_count} 个关键字，正在关闭浏览器准备重启...")
                            page.quit()
                            logging.info(" 浏览器已关闭")
                            time.sleep(2)  # 等待浏览器完全关闭
                        except Exception as e:
                            logging.warning(f" 关闭浏览器时出错: {e}")

                    # 重置搜索框未找到计数器
                    searchbox_not_found_count = 0

                    # 创建新浏览器页面
                    batch_num = (processed_count // restart_interval) + 1
                    logging.info(f" 创建新浏览器窗口 (批次 {batch_num})...")
                    page = create_browser_page(
                        config_name='fast_search' if CrawlerConfig.CHROMIUM_HEADLESS else 'visible_search',
                        headless=CrawlerConfig.CHROMIUM_HEADLESS,
                        enable_proxy=False
                    )

                    # 初始化浏览器
                    if not initialize_browser_for_search(page, CrawlerConfig.BROWSER_INIT_URL):
                        logging.error(" 浏览器初始化失败")
                        return

                    logging.info(" 浏览器初始化完成，开始搜索关键词...")

                keyword = self.choose_keyword(item)
                if not keyword:
                    logging.info(f"条目 {idx} 中未找到可搜索的关键字，跳过。")
                    continue
                self.current_keyword = keyword

                # 检查关键字是否已完成
                if self.is_keyword_finished(keyword):
                    logging.info(f"[关键字 {idx}] {keyword} 已处理过，跳过。")
                    continue

                try:
                    logging.info(
                        f" [关键字 {idx}] 开始搜索：{keyword} (本批次第 {processed_count % restart_interval + 1}/{restart_interval} 个)")

                    # 使用现有浏览器页面搜索
                    search_results = search_keyword_with_existing_page(
                        page=page,
                        keyword=keyword,
                        type_=type_,
                        time_=time_,
                        language_model=self.language_model,
                        max_pages=CrawlerConfig.MAX_PAGES_PER_KEYWORD
                    )

                    # 搜索成功，重置搜索框未找到计数器
                    searchbox_not_found_count = 0

                    if not search_results:
                        logging.warning(f" [Keyword {idx}] {keyword} found no results")
                        self.save_finished_keyword(keyword)
                        logging.info(f" [Keyword {idx}] no results, marked in Redis finished")
                        processed_count += 1  # Count this keyword even if empty.
                        continue

                    logging.info(f" [关键字 {idx}] {keyword} 找到 {len(search_results)} 个结果")

                    # 添加下载任务
                    if CrawlerConfig.SEARCH_DOWNLOAD_PARALLEL:
                        logging.info(f" [关键字 {idx}] 添加 {len(search_results)} 个下载任务...")
                        for result in search_results:
                            self.add_download_task(result, keyword, idx)

                    # 标记关键字为已完成
                    self.save_finished_keyword(keyword)
                    logging.info(f" [关键字 {idx}] {keyword} 已标记完成")

                    processed_count += 1  # 成功处理后计数

                except SearchBoxNotFoundException as e:
                    # 搜索框未找到，增加计数器
                    searchbox_not_found_count += 1
                    logging.error(
                        f" [关键字 {idx}] 未找到搜索框 (连续 {searchbox_not_found_count}/{max_searchbox_failures} 次): {keyword}")

                    # 不保存关键字，允许重启后重试
                    # 不增加processed_count，这样下次循环会触发重启检查
                    if searchbox_not_found_count >= max_searchbox_failures:
                        logging.warning(f" 将在下次循环强制重启浏览器...")
                    continue

                except Exception as e:
                    logging.error(f" [关键字 {idx}] 处理关键词 {keyword} 时出错：{e}")
                    # 发生其他错误时保存关键字，避免无限重试
                    self.save_finished_keyword(keyword)
                    processed_count += 1  # 出错也计数
                    # 重置搜索框未找到计数器（因为这是其他类型的错误）
                    searchbox_not_found_count = 0
                    continue

                # 关键词间短暂延时
                time.sleep(1)

        finally:
            # 最后关闭浏览器
            if page:
                try:
                    logging.info(" 关闭浏览器窗口...")
                    page.quit()
                    logging.info(" 浏览器窗口已关闭")
                except Exception as e:
                    logging.warning(f" 关闭浏览器时出错: {e}")

    def run(self, json_file_path: str, type_: str, time_: str):
        """主运行函数"""
        json_file_path = os.path.abspath(json_file_path)
        start_dt = datetime.now()
        script_name = os.path.basename(__file__)
        exit_reason = "正常结束"
        run_config = {"keyword_path": json_file_path}

        progress_stop_event = threading.Event()
        progress_thread = None
        data = []
        all_keywords = []

        def progress_done_count() -> int:
            return sum(1 for kw in all_keywords if kw and self.is_keyword_finished(kw))

        try:
            if CrawlerConfig.SEARCH_DOWNLOAD_PARALLEL:
                self.start_download_executor()

            self.process_incomplete_downloads()
            self.finished_keywords = self.load_finished_keywords()
            # logging.info(f" 重新加载已搜索关键字: {len(self.finished_keywords)} 个")

            if not os.path.exists(json_file_path):
                logging.error(f"输入文件 {json_file_path} 不存在！")
                return

            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    logging.error("输入JSON文件格式错误，期望为数组格式。")
                    return

            # 在加载阶段就过滤掉没有有效关键词（None / 空串）的条目
            data = [item for item in data if self.choose_keyword(item)]
            all_keywords = [self.choose_keyword(item) for item in data]

            pending_items = []
            for idx, item in enumerate(data, start=1):
                keyword = self.choose_keyword(item)
                if keyword and not self.is_keyword_finished(keyword):
                    pending_items.append((idx, item))

            logging.info(
                f"总共 {len(data)} 个关键词，其中 {len(pending_items)} 个待处理，{len(data) - len(pending_items)} 个已完成")

            notify_event(
                "程序启动：Crawler开始运行",
                start_dt,
                run_config,
                extra=f"{progress_done_count()}/{len(all_keywords)}",
                throttle_key="startup",
                error_detail="关键词已加载，准备开始爬取",
                script_name=script_name,
            )

            def progress_report_worker():
                while not progress_stop_event.wait(PROGRESS_REPORT_INTERVAL_SECONDS):
                    notify_event(
                        "定时进度汇报_ml_bing2",
                        start_dt,
                        run_config,
                        extra=f"{progress_done_count()}/{len(all_keywords)} | 当前关键词: {self.current_keyword or ''}",
                        throttle_key="progress_30m",
                        error_detail="程序仍在运行中",
                        script_name=script_name,
                    )

            progress_thread = threading.Thread(target=progress_report_worker, daemon=True)
            progress_thread.start()

            if not pending_items:
                logging.info("所有关键词都已处理完成，无需重复执行！")
                return

            logging.info(f" 开始处理 {len(pending_items)} 个待处理关键词")
            self.process_all_keywords_with_single_browser(pending_items, type_, time_)
            logging.info(" 所有关键词搜索完成！")

        except KeyboardInterrupt:
            exit_reason = "用户手动停止"
            logging.warning("检测到手动中断，准备退出...")
        except json.JSONDecodeError as e:
            exit_reason = "异常退出"
            logging.error(f"解析JSON文件 {json_file_path} 时出错: {e}")
            notify_event(
                "严重异常：未处理Exception",
                start_dt,
                run_config,
                extra=f"当前关键词: {self.current_keyword or ''} | {progress_done_count()}/{len(all_keywords)}",
                throttle_key="fatal_exception",
                error_detail=str(e),
                script_name=script_name,
            )
        except Exception as e:
            exit_reason = "异常退出"
            logging.error(f"程序异常终止: {e}")
            notify_event(
                "严重异常：未处理Exception",
                start_dt,
                run_config,
                extra=f"当前关键词: {self.current_keyword or ''} | {progress_done_count()}/{len(all_keywords)}",
                throttle_key="fatal_exception",
                error_detail=str(e),
                script_name=script_name,
            )
        finally:
            progress_stop_event.set()
            if progress_thread:
                progress_thread.join(timeout=1)

            if CrawlerConfig.SEARCH_DOWNLOAD_PARALLEL:
                self.stop_download_executor()

            final_count = progress_done_count() if all_keywords else finished_count_bing()
            logging.info(f"最终统计: 共完成 {final_count} 个关键字")
            logging.info("结果已写入 Redis results 队列（按语种分桶）")
            logging.info("所有任务（搜索+下载）完全完成！")

            end_dt = datetime.now()
            final_msg = (
                "【Crawler运行结束】\n\n"
                f"结束原因: {exit_reason}\n"
                f"进度: {final_count}/{len(all_keywords)}\n"
                f"开始时间: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"结束时间: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"运行脚本: {script_name}\n"
                f"关键词文件: {json_file_path}"
            )
            send_wps_robot(final_msg, throttle_key="final")

            run_result = {
                "json_path": json_file_path,
                "exit_reason": exit_reason,
                "done": final_count,
                "total": len(all_keywords),
                "start_time": start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                "end_time": end_dt.strftime('%Y-%m-%d %H:%M:%S'),
            }
            print(f"RUN_RESULT_JSON:{json.dumps(run_result, ensure_ascii=False)}", flush=True)


# ---------- 脚本主入口 ----------
if __name__ == '__main__':
    # 配置参数
    base_directory = r"D:\采集中\bing"  # 基础目录
    json_input_file = resolve_json_input_file()  # 输入的关键词JSON文件（必须由参数传入）
    file_type = 'xlsx'  # 搜索的文件类型
    time_filter = ''  # 时间过滤条件（空字符串表示不限制）
    max_concurrent_workers = CrawlerConfig.DEFAULT_MAX_WORKERS  # 最大并发线程数

    allowed_extensions = ['xlsx', 'xls', 'ett', 'et', 'xlsb','xlsm']
    fasttext_model_path = r"lid.176.bin"

    # Redis 连接检测
    try:
        rds.ping()
        logging.info(f"Redis连接成功: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
    except Exception as e:
        logging.error(f"Redis连接失败: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB} -> {e}")
        raise SystemExit(1)

    # 创建爬虫管理器并运行
    crawler = DrissionPageCrawlerManager(base_directory, max_concurrent_workers, fasttext_model_path,allowed_extensions)
    crawler.run(json_input_file, file_type, time_filter)
