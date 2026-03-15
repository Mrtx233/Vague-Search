import socket
import re
import time
import os
import sys
import atexit
import threading
import logging
import json
import random
import hashlib
import asyncio
import importlib.util
from pathlib import Path
from typing import List, Tuple, Dict, Optional
from datetime import datetime

import requests
import aiohttp
import aiofiles
import redis
from DrissionPage import ChromiumPage, ChromiumOptions, Chromium

from utils import DomainClassifier, LanguageDetector

requests.packages.urllib3.disable_warnings()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

def _load_wps_push():
    """显式按文件路径加载 wps_push，避免包路径问题。"""
    module_path = Path(PROJECT_ROOT) / "utils" / "wps_push_tool" / "wps_push.py"
    if not module_path.exists():
        return None
    try:
        spec = importlib.util.spec_from_file_location("wps_push_dynamic", module_path)
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
    except Exception:
        return None
    return None


_wps_mod = _load_wps_push()
if _wps_mod:
    send_wps_robot = _wps_mod.send_wps_robot
    notify_event = _wps_mod.notify_event
    get_device_name = _wps_mod.get_device_name
else:
    def get_device_name() -> str:
        return "未知设备"

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

path = r"D:\采集中\google"
BASE_XLSX_DIR = os.path.join(path, '样张文件')
KEYWORD_PATH = r"E:\Crawler\模糊搜索\模糊搜索\json\output\德语\心理学B.json"
PAGE_TIMEOUT = 40
MAX_WORKERS = 8
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
URL_CLASS_CONFIG_PATH = 'url_class_keywords.json'

REDIS_HOST = "10.229.32.166"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PREFIX = "crawler"
PROGRESS_REPORT_INTERVAL_SECONDS = 3600


def rkey(kind: str, lang: str) -> str:
    return f"{REDIS_PREFIX}:{kind}:{lang}"


rds = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def resolve_keyword_path(default_path: str) -> str:
    if len(sys.argv) > 1 and sys.argv[1].strip():
        return os.path.abspath(sys.argv[1].strip())
    return os.path.abspath(default_path)


def is_allowed_file_extension(file_type: str, allowed_extensions: List[str]) -> bool:
    if not file_type:
        return False
    return file_type.lower() in [ext.lower() for ext in allowed_extensions]


def clear_browser_data(tab) -> None:
    try:
        tab.clear_cache(True, True)
        logger.info("已清除浏览器浏览数据")
    except Exception as e:
        logger.warning(f"清除浏览器浏览数据失败: {str(e)[:60]}")


def get_available_port(start_port=9200, end_port=9500) -> int:
    for port in range(start_port, end_port + 1):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('localhost', port))
                return port
        except OSError:
            continue
    return random.randint(start_port, end_port)


def calculate_file_md5(file_path: str, chunk_size: int = 16384) -> str:
    md5_hash = hashlib.md5()
    try:
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    except Exception as e:
        logger.warning(f"文件MD5计算失败: {os.path.basename(file_path)} -> {str(e)[:30]}")
        return ""


def is_finished_google(keyword: str) -> bool:
    return bool(rds.sismember(f"{REDIS_PREFIX}:keyword_finished:google", keyword))


def mark_finished_google(keyword: str) -> bool:
    return rds.sadd(f"{REDIS_PREFIX}:keyword_finished:google", keyword) == 1


def finished_count_google() -> int:
    try:
        return int(rds.scard(f"{REDIS_PREFIX}:keyword_finished:google"))
    except Exception:
        return 0


def is_new_url(lang: str, url: str) -> bool:
    return rds.sadd(rkey("seen_url", lang), url) == 1

def md5_set_key() -> str:
    return f"{REDIS_PREFIX}:seen_md5"


def is_new_md5(md5_hash: str) -> bool:
    if not md5_hash:
        return False
    return rds.sadd(md5_set_key(), md5_hash) == 1


def rollback_md5(md5_hash: str) -> None:
    if not md5_hash:
        return
    try:
        rds.srem(md5_set_key(), md5_hash)
    except Exception:
        pass


def push_jsonl_line(lang: str, json_obj: Dict) -> None:
    line = json.dumps(json_obj, ensure_ascii=False, separators=(',', ':'))
    rds.rpush(rkey("results", lang), line)


def load_keywords_with_status(json_path: str) -> List[str]:
    try:
        with open(json_path, 'r', encoding='utf-8-sig') as f:
            data = json.load(f)
            return [item['外文'] for item in data if '外文' in item]
    except Exception as e:
        logger.error(f"关键词文件加载失败: {str(e)}")
        return []


def test_network_connection() -> bool:
    test_urls = ["https://www.google.com", "https://httpbin.org/ip", "https://www.bing.com"]
    for test_url in test_urls:
        try:
            response = requests.get(test_url, timeout=10, verify=False)
            if response.status_code == 200:
                return True
        except Exception:
            continue
    logger.warning("网络连接测试失败，所有测试URL均无法访问")
    return False


async def download_file_async(url: str, save_dir: str, file_type: str, max_retries: int = 1) -> Tuple[Optional[str], Optional[str]]:
    headers = {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
    }

    temp_file = None
    for attempt in range(max_retries):
        try:
            timeout = aiohttp.ClientTimeout(total=45, connect=10)
            connector = aiohttp.TCPConnector(ssl=False, limit=150, limit_per_host=30)

            os.makedirs(save_dir, exist_ok=True)

            async with aiohttp.ClientSession(headers=headers, timeout=timeout, connector=connector) as session:
                async with session.get(url, allow_redirects=True) as response:
                    response.raise_for_status()

                    temp_file = os.path.join(save_dir, f"temp_{random.randint(10000, 99999)}.tmp")
                    async with aiofiles.open(temp_file, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)

                    md5_hash = calculate_file_md5(temp_file)
                    if not md5_hash:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                        return None, None

                    ext = file_type.lower() if file_type else "xlsx"
                    if not is_new_md5(md5_hash):
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                        logger.info(f"文件内容已存在（Redis MD5去重），跳过: {md5_hash[:12]}.{ext}")
                        return None, md5_hash

                    file_name = f"{md5_hash}.{ext}"
                    file_path = os.path.join(save_dir, file_name)
                    try:
                        os.replace(temp_file, file_path)
                    except Exception:
                        rollback_md5(md5_hash)
                        raise
                    logger.info(f"异步下载成功: {file_name}")
                    return file_path, md5_hash

        except aiohttp.ClientError as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            logger.warning(f"异步下载失败（已重试{max_retries}次): {url[:50]} -> {str(e)[:60]}")
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            logger.warning(f"异步下载失败（已重试{max_retries}次): {url[:50]} -> {str(e)[:60]}")
        finally:
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass

    return None, None


def parse_search_results(page: ChromiumPage, allowed_extensions: List[str]) -> List[Dict[str, str]]:
    items = []
    try:
        result_containers = page.eles('xpath://div[@class="N54PNb BToiNc"]')
        if not result_containers:
            logger.warning("未找到搜索结果容器（可能Google页面结构更新）")
            return items

        for containe in result_containers:
            try:
                container = containe.ele('xpath:.//div[@class="yuRUbf"]//a')
                url = (container.attr("href") or "").strip()
                title_ele = container.ele('xpath:.//h3')
                title = title_ele.text.strip() if title_ele else "无标题"
                if not url:
                    continue

                file_type = ""
                clean_url = url.split('?')[0].split('#')[0]
                ext_match = re.search(r'\.([a-zA-Z0-9]{1,10})$', clean_url)
                if ext_match:
                    file_type = ext_match.group(1).lower()
                else:
                    try:
                        file_type_ele = containe.ele('xpath:.//div[@class="eFM0qc BCF2pd"]', timeout=0.5)
                        if file_type_ele:
                            file_type_text = file_type_ele.text.strip()
                            if file_type_text and len(file_type_text) <= 10:
                                file_type = file_type_text.lower()
                    except Exception:
                        pass

                if not file_type or not is_allowed_file_extension(file_type, allowed_extensions):
                    continue

                lang_detect_text = ""
                site = ""
                try:
                    lang_detect_ele = containe.ele('xpath:.//div[@class="kb0PBd A9Y9g"]', timeout=0.5)
                    lang_detect_text = lang_detect_ele.text.strip() if lang_detect_ele else ""
                except Exception:
                    pass

                try:
                    site_ele = containe.ele('xpath:.//div[@class="byrV5b"]/cite', timeout=0.5)
                    if site_ele:
                        site = site_ele.text.strip()
                        match = re.match(r'^(.*?)›', site)
                        if match:
                            site = match.group(1).strip()
                except Exception:
                    pass

                items.append({
                    "url": url,
                    "title": title,
                    "file_type": file_type,
                    "lang_detect_text": lang_detect_text,
                    "webSite": site
                })
            except Exception as e:
                logger.warning(f"单条结果解析失败: {str(e)[:40]}")
                continue

        logger.info(f"成功解析 {len(items)} 条有效搜索结果")
        return items
    except Exception as e:
        logger.error(f"搜索结果解析异常: {str(e)[:40]}")
        return items

async def process_downloads_async(
    result_items: List[Dict[str, str]],
    keyword: str,
    page_num: int,
    crawl_time: int,
    domain_classifier: DomainClassifier,
    language_detector: LanguageDetector,
    allowed_extensions: List[str],
    max_workers: int
) -> Tuple[int, int]:
    if not result_items:
        return 0, 0

    semaphore = asyncio.Semaphore(max_workers)

    async def limited_task(item):
        async with semaphore:
            return await handle_download_task_async(item, keyword, page_num, crawl_time,
                                                   domain_classifier, language_detector, allowed_extensions)

    tasks = [limited_task(item) for item in result_items]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    success_count = 0
    failed_count = 0
    for result in results:
        if isinstance(result, Exception):
            failed_count += 1
            logger.error(f"异步任务异常: {str(result)[:50]}")
        elif result is True:
            success_count += 1
        else:
            failed_count += 1
    return success_count, failed_count


async def handle_download_task_async(
    result_item: Dict[str, str],
    original_keyword: str,
    page_num: int,
    crawl_time: int,
    domain_classifier: DomainClassifier,
    language_detector: LanguageDetector,
    allowed_extensions: List[str]
) -> bool:
    url = result_item["url"]
    title = result_item["title"]
    file_type = result_item["file_type"]
    lang_detect_text = result_item["lang_detect_text"]
    website = result_item.get("webSite", "")

    if not is_allowed_file_extension(file_type, allowed_extensions):
        return False

    final_language = language_detector.detect_with_threshold(lang_detect_text) or "未知"

    if not is_new_url(final_language, url):
        logger.info(f"URL已存在（Redis全局去重），跳过: {url[:50]}")
        return False

    file_path, md5_hash = await download_file_async(url, BASE_XLSX_DIR, file_type)
    if not md5_hash:
        logger.warning(f"异步下载失败，跳过记录: {url[:50]}")
        return False
    if not file_path:
        logger.info(f"文件内容重复（Redis MD5去重），跳过记录: {url[:50]} | MD5: {md5_hash[:10]}")
        return False
    domain_result = domain_classifier.classify_url(url)
    full_host = domain_result["full_host"]
    domain_class = domain_result["domain_class"]

    crawl_json = {
        "webSite": website if website else full_host,
        "crawlTime": crawl_time,
        "srcUrl": url,
        "title": title,
        "hash": md5_hash,
        "extend": {
            "publishTime": None,
            "keyword": original_keyword,
            "language": final_language,
            "doMain": domain_class,
            "type": file_type if file_type else ""
        }
    }

    push_jsonl_line(final_language, crawl_json)

    logger.info(
        f"异步记录生成成功 | 关键词: {original_keyword[:20]} | "
        f"标题: {title[:30]} | 网站: {website if website else full_host} | "
        f"语种: {final_language} | 类型: {file_type} | MD5: {md5_hash[:10]}"
    )
    return True


def navigate_to_next_page(page: ChromiumPage) -> bool:
    try:
        next_btn = page.ele('xpath:(//td[@class="d6cvqb BBwThe"])[2]/A[1]')
        time.sleep(2)
        next_btn.click()
        logger.info("成功翻页")
        return True
    except Exception:
        logger.info("到达最后一页")
        return False


async def process_keyword_async(
    page,
    keyword: str,
    max_workers: int,
    domain_classifier: DomainClassifier,
    language_detector: LanguageDetector,
    allowed_extensions: List[str]
) -> None:
    logger.info(f"开始处理关键词: {keyword}")

    page_num = 1
    total_success_count = 0

    while True:
        try:
            page.wait.load_start()
            page.wait(2)

            result_items = parse_search_results(page, allowed_extensions)
            if not result_items:
                logger.warning(f"第{page_num}页无有效结果，停止当前关键词")
                break

            current_crawl_time = int(time.time() * 1000)

            page_success_count, page_failed_count = await process_downloads_async(
                result_items, keyword, page_num, current_crawl_time,
                domain_classifier, language_detector, allowed_extensions, max_workers
            )

            total_success_count += page_success_count
            logger.info(f"第{page_num}页异步任务完成: 成功{page_success_count}个, 失败{page_failed_count}个")

            if not navigate_to_next_page(page):
                logger.info(f"关键词{keyword}已到最后一页（共{page_num}页）")
                break

            page_num += 1
            time.sleep(8)

        except Exception as e:
            logger.error(f"第{page_num}页处理异常: {str(e)[:40]}，停止当前关键词")
            break

    if total_success_count == 0:
        logger.info(f"关键词'{keyword}'无成功下载")
    else:
        logger.info(f"关键词'{keyword}'处理完成，共成功下载{total_success_count}个文件")


def process_keyword(page, keyword: str, max_workers: int,
                    domain_classifier: DomainClassifier, language_detector: LanguageDetector,
                    allowed_extensions: List[str]) -> None:
    asyncio.run(process_keyword_async(page, keyword, max_workers, domain_classifier, language_detector, allowed_extensions))


if __name__ == '__main__':
    LOG_FORMAT = '%(asctime)s | %(levelname)-6s | %(module)s:%(lineno)d | %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        encoding='utf-8',
        handlers=[logging.StreamHandler()]
    )
    logger = logging.getLogger(__name__)
    start_dt = datetime.now()
    script_name = os.path.basename(__file__)
    exit_reason = "正常结束"
    keyword_path = resolve_keyword_path(KEYWORD_PATH)
    keywords: List[str] = []
    skipped_count = 0
    completed_count = 0
    run_result_state = {"emitted": False}

    def emit_run_result_once() -> None:
        if run_result_state["emitted"]:
            return
        run_result = {
            "json_path": os.path.abspath(keyword_path),
            "exit_reason": exit_reason,
            "done": skipped_count + completed_count,
            "total": len(keywords),
            "start_time": start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            "end_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        print(f"RUN_RESULT_JSON:{json.dumps(run_result, ensure_ascii=False)}", flush=True)
        run_result_state["emitted"] = True

    atexit.register(emit_run_result_once)

    config: Dict = {
        "search_file_extension": "xlsx",
        "allowed_download_extensions": ['xlsx', 'xls', 'ett', 'et', 'xlsb', 'xlsm'],
        "keyword_path": keyword_path,
        "max_workers": MAX_WORKERS,
        "domain_config_path": URL_CLASS_CONFIG_PATH,
        "language_model_path": 'lid.176.bin',
        "language_confidence_threshold": 0.8,
        "initial_url": 'https://www.google.com.hk/search?q=ddd&oq=ddd&gs_lcrp=EgZjaHJvbWUyBggAEEUYOdIBCDEwODlqMGoxqAIAsAIB&sourceid=chrome&ie=UTF-8&sei=dgvnaNTjBs3C4-EP5Nj16Qs'
    }

    try:
        rds.ping()
    except Exception as e:
        exit_reason = "异常退出"
        logger.error(f"Redis 连接失败: {REDIS_HOST}:{REDIS_PORT} -> {e}")
        raise

    domain_classifier = DomainClassifier(config["domain_config_path"])
    language_detector = LanguageDetector(config["language_model_path"], config["language_confidence_threshold"])

    if not domain_classifier.is_config_loaded():
        logger.warning("域名分类配置加载失败，域名分类功能将失效")
    if not language_detector.is_model_loaded():
        logger.warning("语言检测模型加载失败，语言检测功能将失效")

    os.makedirs(BASE_XLSX_DIR, exist_ok=True)
    logger.info(f"文件存储目录: {BASE_XLSX_DIR}")
    logger.info(f"搜索使用的文件类型: {config['search_file_extension']}")
    logger.info(f"允许下载的文件类型: {', '.join(config['allowed_download_extensions'])}")

    if not test_network_connection():
        exit_reason = "异常退出"
        logger.error("网络连接异常，请检查网络设置")
        raise SystemExit(1)

    tab = None

    keywords = load_keywords_with_status(config["keyword_path"])
    logger.info(f"总关键词数: {len(keywords)}")

    if not keywords:
        exit_reason = "异常退出"
        logger.error("无有效关键词，程序终止")
        # 浏览器还未打开，不需要关闭
        raise SystemExit(1)

    # 启动推送前先从 Redis 统计“当前json中已完成”的数量
    skipped_count = sum(1 for kw in keywords if is_finished_google(kw))
    
    # 如果所有关键词都已完成，直接退出，不启动浏览器
    if skipped_count >= len(keywords):
        logger.info(f"所有 {len(keywords)} 个关键词均已完成，无需启动浏览器。")
        # 构造退出原因和结果
        exit_reason = "正常结束"
        completed_count = 0 # 此次运行完成数为0
        
        # 触发一次结果输出
        run_result = {
            "json_path": os.path.abspath(keyword_path),
            "exit_reason": exit_reason,
            "done": skipped_count,
            "total": len(keywords),
            "start_time": start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            "end_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        print(f"RUN_RESULT_JSON:{json.dumps(run_result, ensure_ascii=False)}", flush=True)
        run_result_state["emitted"] = True # 防止atexit重复输出
        
        # 不使用 SystemExit，而是直接 return 让它正常退出，或者用 sys.exit(0)
        # 但因为外面有 atexit，sys.exit(0) 也会触发。
        # 我们这里直接 sys.exit(0) 即可
        sys.exit(0)

    # 有未完成的任务，才启动浏览器
    try:
        chrome_options = ChromiumOptions()
        chrome_options.set_user_agent(USER_AGENT)
        chrome_options.set_local_port(get_available_port())

        chrome = Chromium(chrome_options)
        tab = chrome.latest_tab
        tab.clear_cache(True, True)
        tab.get(config["initial_url"])

    except Exception as e:
        exit_reason = "异常退出"
        logger.error(f"浏览器初始化失败: {str(e)}")
        raise SystemExit(1)

    current_keyword = None
    run_config = {"keyword_path": config["keyword_path"]}

    notify_event(
        "程序启动：Crawler开始运行",
        start_dt,
        run_config,
        extra=f"{skipped_count + completed_count}/{len(keywords)}",
        throttle_key="startup",
        error_detail="关键词已加载，准备开始爬取",
        script_name=script_name,
    )

    progress_stop_event = threading.Event()

    def progress_report_worker():
        while not progress_stop_event.wait(PROGRESS_REPORT_INTERVAL_SECONDS):
            notify_event(
                "定时进度汇报（1小时）",
                start_dt,
                run_config,
                extra=f"{skipped_count + completed_count}/{len(keywords)}",
                throttle_key="progress_60m",
                error_detail="程序仍在运行中",
                script_name=script_name,
            )

    progress_thread = threading.Thread(target=progress_report_worker, daemon=True)
    progress_thread.start()

    try:
        for keyword in keywords:
            current_keyword = keyword

            if is_finished_google(keyword):
                logger.info(f"跳过已完成关键词: {keyword}")
                continue

            captcha_detected = False
            try:
                ele = tab.ele('xpath://hr[@noshade]')
                captcha_detected = bool(ele)
            except Exception as captcha_err:
                if captcha_detected:
                    logger.warning(f"验证码通知失败: {str(captcha_err)[:80]}")

            if captcha_detected:
                exit_reason = "检测到Google验证码，终止本次运行等待重试"
                logger.warning("检测到验证码，终止本次运行，等待重试")
                raise SystemExit(2)

            search_box = tab.ele('xpath://textarea[@name="q"]')
            search_box.click()
            search_box.clear()
            time.sleep(0.3)

            search_query = f'"{keyword}" filetype:{config["search_file_extension"]}'
            tab.wait(10)
            search_box.input(search_query)
            search_box.input('\n')
            logger.info(f"已提交搜索: {search_query}")

            process_keyword(
                tab, keyword, config["max_workers"],
                domain_classifier, language_detector, config["allowed_download_extensions"]
            )

            mark_finished_google(keyword)
            completed_count += 1

            clear_browser_data(tab)
            time.sleep(random.uniform(1, 2))

    except KeyboardInterrupt:
        exit_reason = "用户手动停止"
        logger.warning("检测到手动中断，准备退出...")

    except Exception as e:
        exit_reason = "异常退出"
        logger.error(f"程序异常终止 | 当前关键词: {current_keyword} | 错误: {str(e)}")
        if current_keyword:
            if mark_finished_google(current_keyword):
                completed_count += 1

    finally:
        progress_stop_event.set()
        progress_thread.join(timeout=1)

        end_dt = datetime.now()
        if tab:
            tab.close()

        final_msg = (
            "【Crawler运行结束】\n\n"
            f"设备: {get_device_name()}\n\n"
            f"结束原因: {exit_reason}\n"
            f"进度: {skipped_count + completed_count}/{len(keywords)}\n"
            f"开始时间: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"结束时间: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"运行脚本: {script_name}\n"
            f"关键词文件: {config['keyword_path']}"
        )
        send_wps_robot(final_msg, throttle_key="final")
        logger.info("程序执行完毕，浏览器已关闭")
        emit_run_result_once()
