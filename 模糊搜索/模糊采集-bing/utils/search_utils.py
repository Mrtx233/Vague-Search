import time
import random
import logging
import re
import socket
import requests
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse

try:
    from DrissionPage import ChromiumPage, ChromiumOptions
except ImportError:
    try:
        from DrissionPage import WebPage as ChromiumPage
        from DrissionPage import ChromiumOptions
    except ImportError:
        raise ImportError("请安装 DrissionPage: pip install DrissionPage")
from .analysis_utils import detect_language, extract_domain_parts, determine_domain_class


# ========== Chromium 浏览器配置（简化版，防止页面卡住） ==========
def get_simple_chromium_config(headless=False, proxy_server=True):
    """获取简化的Chromium配置

    只保留核心必要参数，避免过度优化导致页面卡住
    """
    return {
        # 'arguments': [
        #     '--disable-blink-features=AutomationControlled',  # 隐藏自动化特征
        #     '--no-first-run',  # 跳过首次运行
        #     '--no-default-browser-check',  # 不检查默认浏览器
        #     '--disable-infobars',  # 禁用信息栏
        #     # 以下参数可选，根据需要启用
        #     # '--disable-images',  # 禁用图片加载（如果需要加快速度）
        # ],
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'headless': headless,
        'window_size': '1920,1080',
        'proxy_server': proxy_server
    }


CHROMIUM_CONFIGS = {
    'fast_search': get_simple_chromium_config(headless=False),
    'visible_search': get_simple_chromium_config(headless=False)
}


def get_available_port(start_port=9200, end_port=9500) -> int:
    """获取一个可用的随机端口号（用于浏览器启动）"""
    for port in range(start_port, end_port + 1):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('localhost', port))
                return port
        except OSError:
            continue
    return random.randint(start_port, end_port)


def configure_chromium_options(port: int,
                               config_name: str = 'fast_search',
                               headless: bool = True,
                               proxy_server: Optional[str] = None) -> ChromiumOptions:
    """配置Chromium浏览器选项（简化版）"""
    co = ChromiumOptions()

    # 获取预定义配置
    if config_name in CHROMIUM_CONFIGS:
        config = CHROMIUM_CONFIGS[config_name]
        pass
    else:
        # 使用自定义配置
        config = get_simple_chromium_config(headless=headless)
        pass

    # 基础设置
    co.headless(config['headless'])
    co.set_local_port(port)
    co.set_user_agent(config['user_agent'])

    # 应用所有参数
    # for arg in config['arguments']:
    #     co.set_argument(arg)

    # 设置代理（如果提供）
    if proxy_server:
        co.set_proxy(proxy_server)

    # logging.info(f"   📊 启用参数数量: {len(config['arguments'])}")
    # 浏览器配置细节日志已静音，避免控制台噪声

    return co


def create_browser_page(config_name: str = 'fast_search',
                        headless: bool = True,
                        enable_proxy: bool = False,
                        proxy_server: Optional[str] = None,
                        chromium_path: Optional[str] = None) -> ChromiumPage:
    """创建并配置Chromium浏览器页面

    Args:
        config_name: 配置名称 ('fast_search', 'minimal', 'debug', 'stealth')
        headless: 是否无头模式
        enable_proxy: 是否启用代理
        proxy_server: 代理服务器地址
        chromium_path: Chromium可执行文件路径
    """
    # 获取随机端口
    port = get_available_port()

    # 处理代理设置
    if enable_proxy and not proxy_server:
        # 这里可以从配置文件获取默认代理
        proxy_server = None  # 或者从环境变量/配置文件获取

    # 使用专用函数配置Chromium选项
    co = configure_chromium_options(
        port=port,
        config_name=config_name,
        headless=headless,
        proxy_server=proxy_server
    )

    # 设置Chromium可执行文件路径（如果指定）
    if chromium_path:
        co.set_browser_path(chromium_path)

    if proxy_server:
        pass

    try:
        # 创建Chromium页面
        page = ChromiumPage(addr_or_opts=co)
        # 设置页面超时（增加超时时间，防止卡住）
        page.set.timeouts(base=15, page_load=60, script=60)

        # 设置窗口大小（如果不是无头模式）
        if not headless:
            page.set.window.size(1920, 1080)

        # 设置加载策略为正常模式（不禁用图片，避免元素识别问题）
        try:
            page.set.load_mode.normal()  # 使用正常加载模式
        except Exception as e:
            pass

        return page
    except Exception as e:
        logging.error(f"Chromium浏览器启动失败: {e}")
        logging.error("建议检查:")
        logging.error(" - Chrome/Chromium 是否已安装")
        logging.error(f" - 端口 {port} 是否被占用")
        logging.error(" - 系统资源是否充足")
        if chromium_path:
            logging.error(f" - 指定的Chromium路径是否正确: {chromium_path}")
        raise


def get_search_url(keyword: str, file_type: str, page_num: int = 1) -> str:
    """构建Bing搜索URL"""
    page_start = (page_num - 1) * 10 + 1
    url = f"https://www.bing.com/search?q={keyword}+filetype:{file_type}&first={page_start}"
    return url


def extract_file_type_from_url(url: str) -> Optional[str]:
    """从URL中提取文件类型"""
    try:
        parsed = urlparse(url)
        path = parsed.path.lower()
        if '.' in path:
            return path.split('.')[-1]
    except:
        pass
    return None


def extract_website_domain(url: str) -> Optional[str]:
    """从URL中提取网站域名"""
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    except:
        return None


def extract_real_download_url(session, download_link: str) -> str:
    """处理Bing跳转链接，提取真实下载地址（使用requests）"""
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


def extract_real_download_url_with_page(page: ChromiumPage, download_link: str) -> str:
    """处理Bing跳转链接，提取真实下载地址"""
    if "www.bing.com/ck" in download_link:  # 检测是否为Bing跳转链接
        logging.info(f"检测到Bing跳转链接，开始提取真实下载地址: {download_link}")
        try:
            # 使用DrissionPage访问跳转页面
            page.get(download_link)
            time.sleep(2)  # 等待页面加载

            # 尝试从页面源码中匹配真实下载地址
            page_source = page.html
            match = re.search(r'var\s+u\s*=\s*"([^"]+)"', page_source)
            if not match:
                # 如果正则匹配失败，尝试等待重定向
                time.sleep(2)
                current_url = page.url
                if current_url != download_link and not current_url.startswith('https://www.bing.com'):
                    real_download_link = current_url
                    logging.info(f"通过重定向获取真实下载地址: {real_download_link}")
                    return real_download_link
                else:
                    raise Exception("未在Bing跳转页面中匹配到真实下载地址")
            else:
                # 提取并赋值真实下载地址
                real_download_link = match.group(1)
                logging.info(f"成功提取真实下载地址: {real_download_link}")
                return real_download_link

        except Exception as e:
            # 捕获Bing链接处理过程中的异常
            raise Exception(f"Bing跳转链接处理失败: {str(e)}") from e

    return download_link  # 如果不是Bing跳转链接，直接返回原链接


def parse_search_results(page: ChromiumPage, keyword: str, page_num: int, language_model: Optional[object] = None) -> \
        List[Dict]:
    """解析搜索结果页面，提取链接、标题、网站域名等信息"""
    results = []
    try:
        # 优化：减少等待时间，0.5秒足够（原来是2秒）
        time.sleep(2)

        # 获取搜索结果元素
        result_elements = page.eles('xpath://li[@class="b_algo"]')

        for idx, element in enumerate(result_elements):
            try:
                # 提取标题和链接 - 优化：减少超时时间到1秒（原来3秒）
                title_element = element.ele('xpath:.//h2/a', timeout=1)
                if not title_element:
                    continue

                title = title_element.text.strip()
                link = title_element.attr('href')

                if not title or not link:
                    continue

                # 移除文件扩展名
                title = re.sub(r'\.[a-z]*$', '', title)

                crawl_time = int(time.time() * 1000)  # 当前时间戳（毫秒）

                # 尝试提取网站域名 - 优化：减少超时时间到0.5秒（原来1秒）
                website = ""
                try:
                    # 从当前搜索结果元素中查找对应的cite元素
                    cite_element = element.ele('xpath:.//div[@class="b_attribution"]//cite', timeout=0.5)
                    if cite_element:
                        cite_text = cite_element.text.strip()
                        # 查找第一个›前的内容
                        if '›' in cite_text:
                            website = cite_text.split('›')[0].strip()
                        else:
                            website = cite_text
                        logging.debug(f"提取网站域名: {website}")
                except Exception as e:
                    logging.debug(f"提取网站域名失败: {e}")

                # 如果提取失败，使用URL提取域名作为备用
                if not website:
                    website = extract_website_domain(link) or ""

                # 语言检测 - 优化：减少超时时间到0.5秒（原来1秒）
                language = None
                try:
                    # 从当前搜索结果元素中查找对应的描述文本
                    description_element = element.ele('xpath:.//p[@class="b_lineclamp2"]', timeout=0.5)
                    if description_element:
                        caption_text = description_element.text.strip()
                        if caption_text and language_model:
                            lang_code, confidence = detect_language(caption_text, language_model)
                            if confidence > 0.8:
                                language = lang_code
                            logging.debug(f"语言检测: {lang_code} (置信度: {confidence})")
                        else:
                            logging.debug(f"描述文本为空或无语言模型: '{caption_text[:50]}...'")
                except Exception as e:
                    logging.debug(f"语言检测失败: {e}")

                # 暂时使用原始链接，不在搜索阶段处理跳转（下载时再处理）
                initial_link = link

                # 提取文件类型 - 使用原始链接
                file_type = extract_file_type_from_url(link)

                # 优化：简化文件类型提取逻辑，减少全页面查找
                # 如果从链接无法提取文件类型，尝试从当前元素中提取
                if not file_type:
                    try:
                        # 从当前元素内查找文件类型标识（不查找全页面）
                        type_span = element.ele('xpath:.//h2/span', timeout=0.3)
                        if type_span:
                            type_text = type_span.text.strip()
                            # 去掉两边的[]
                            if type_text.startswith('[') and type_text.endswith(']'):
                                file_type = type_text[1:-1]
                    except Exception as e:
                        logging.debug(f"从元素提取文件类型失败: {e}")

                # 域名分类 - 优先使用提取的website，其次使用链接域名
                domain_class = ""
                try:
                    if website:
                        # 如果成功提取了website，直接用于分类
                        domain_class = determine_domain_class(website, "")
                        logging.debug(f"使用网站域名进行分类: {website} -> {domain_class}")

                    # 如果website分类失败，尝试使用链接域名
                    if not domain_class:
                        domain_info = extract_domain_parts(link)
                        full_host = domain_info["full_host"]
                        domain_class = determine_domain_class(full_host, domain_info["suffix"])
                        logging.debug(f"使用链接域名进行分类: {full_host} -> {domain_class}")
                except Exception as e:
                    logging.debug(f"域名分类失败: {e}")

                result = {
                    "webSite": website,
                    "crawlTime": crawl_time,
                    "srcUrl": initial_link,  # 使用初始链接，下载时再处理跳转
                    "title": title,
                    "hash": "",  # 下载后填充
                    "extend": {
                        "publishTime": None,
                        "keyword": keyword,
                        "language": language,
                        "doMain": domain_class,
                        "type": file_type or ""
                    }
                }

                results.append(result)
                logging.info(
                    f'{keyword} --- 第 {page_num} 页 第 {idx + 1} 条：{title} (语言: {language or "未知"}, 域名类型: {domain_class or "未知"})')

            except Exception as e:
                logging.warning(f"解析第 {idx + 1} 条结果时出错: {e}")
                continue

    except Exception as e:
        logging.warning(f"解析 {keyword} 第 {page_num} 页时出错: {e}")

    return results


def initialize_browser_for_search(page: ChromiumPage, init_url: str = None) -> bool:
    """使用指定链接初始化浏览器"""
    try:
        # 如果没有提供初始化URL，使用默认的
        if not init_url:
            init_url = "https://cn.bing.com/search?q=a&form=QBLH&sp=-1&lq=0&pq=a&sc=12-1&qs=n&sk=&cvid=EBBAC49591414CD3A51D24AA23E3841C"

        page.get(init_url)
        time.sleep(2)  # 等待页面加载
        return True
    except Exception as e:
        logging.error(f"浏览器初始化失败: {e}")
        return False


class SearchBoxNotFoundException(Exception):
    """搜索框未找到异常"""
    pass


def perform_search_in_browser(page: ChromiumPage, keyword: str, file_type: str, max_retries: int = 3) -> bool:
    """在浏览器中执行搜索（带重试机制）

    Returns:
        bool: 搜索是否成功

    Raises:
        SearchBoxNotFoundException: 当未找到搜索框时抛出
    """
    for attempt in range(max_retries):
        try:
            # 构建搜索查询
            search_query = f"'{keyword}' filetype:{file_type}"
            # logging.info(f"🔍 执行搜索: {search_query} (尝试 {attempt + 1}/{max_retries})")

            # 优化：减少等待时间到1秒（原来2秒）
            time.sleep(1)

            # 优化：减少超时时间到5秒（原来10秒）
            search_box = None
            try:
                search_box = page.ele('xpath://input[@name="q"]', timeout=5)
            except:
                try:
                    search_box = page.ele('xpath://div[@class="b_searchboxForm"]/input[@class="b_searchbox"]',
                                          timeout=5)
                except:
                    pass

            if not search_box:
                logging.warning(f"⚠️ 未找到搜索框（尝试 {attempt + 1}/{max_retries}）")
                if attempt < max_retries - 1:
                    time.sleep(1)  # 等待后重试
                    continue
                else:
                    logging.error("❌ 多次尝试后仍未找到搜索框")
                    raise SearchBoxNotFoundException("未找到搜索框")

            # 清空搜索框并输入新的搜索内容
            search_box.click()
            time.sleep(0.3)  # 优化：减少到0.3秒（原来0.5秒）
            search_box.clear()
            time.sleep(0.5)  # 优化：减少到0.5秒（原来1秒）
            search_box.input(search_query)
            time.sleep(1)  # 优化：减少到1秒（原来2秒）

            # 提交搜索（按回车键）
            search_box.input('\n')
            time.sleep(1.5)  # 优化：减少到1.5秒（原来2秒）

            # logging.info("✅ 搜索执行完成")
            return True

        except SearchBoxNotFoundException:
            # 重新抛出搜索框未找到异常
            raise
        except Exception as e:
            logging.warning(f"⚠️ 执行搜索失败（尝试 {attempt + 1}/{max_retries}）: {e}")
            if attempt < max_retries - 1:
                time.sleep(1)  # 等待后重试
            else:
                logging.error(f"❌ 多次尝试后搜索仍然失败")
                return False

    return False


def go_to_next_page(page: ChromiumPage, max_retries: int = 2) -> bool:
    """点击下一页（带重试机制）"""
    for attempt in range(max_retries):
        try:
            # 优化：减少等待时间到1秒（原来2秒）
            time.sleep(1)

            # 优化：减少超时时间到3秒（原来10秒）
            next_button = page.ele('xpath://li[@class="b_pag"]//li[last()]/a', timeout=3)
            if not next_button:
                logging.info("ℹ️ 未找到下一页按钮，可能已到最后一页")
                return False

            # 点击下一页
            logging.info(f"📄 点击下一页... (尝试 {attempt + 1}/{max_retries})")
            next_button.click()
            time.sleep(1.5)  # 优化：减少等待时间到1.5秒（原来2秒）
            logging.info("✅ 翻页成功")
            return True

        except Exception as e:
            logging.warning(f"⚠️ 翻页失败（尝试 {attempt + 1}/{max_retries}）: {e}")
            if attempt < max_retries - 1:
                time.sleep(3)  # 等待后重试
            else:
                logging.debug(f"❌ 多次尝试后翻页仍然失败")
                return False

    return False


def search_keyword(keyword: str, type_: str, time_: str, language_model: Optional[object] = None, max_pages: int = 15,
                   init_url: str = None, headless: bool = True) -> List[Dict]:
    """搜索指定关键词并返回所有结果（使用单个浏览器窗口）"""
    all_results = []
    page = None
    seen_urls: Set[str] = set()  # 存储所有已见过的URL
    consecutive_duplicate_pages = 0  # 连续重复页计数器

    try:
        logging.info(f"开始搜索关键词: {keyword} (文件类型: {type_})")

        # 创建浏览器页面
        config_name = 'fast_search' if headless else 'visible_search'
        page = create_browser_page(
            config_name=config_name,
            headless=headless,
            enable_proxy=False
        )

        # 初始化浏览器
        if not initialize_browser_for_search(page, init_url):
            logging.error("浏览器初始化失败，跳过搜索")
            return all_results

        # 执行搜索
        if not perform_search_in_browser(page, keyword, type_):
            logging.error("搜索执行失败，跳过搜索")
            return all_results

        # 逐页获取结果
        for page_num in range(1, max_pages + 1):
            # 检查是否已连续3页重复
            if consecutive_duplicate_pages >= 1:
                logging.info(f"⚠️ 已连续3页获取到重复URL，跳过当前关键词: {keyword}")
                break

            try:
                logging.info(f"正在解析第 {page_num} 页搜索结果...")

                # 解析当前页面的搜索结果
                page_results = parse_search_results(page, keyword, page_num, language_model)

                # 提取当前页的所有URL
                current_page_urls = [result['srcUrl'] for result in page_results]

                # 检查当前页是否所有URL都已存在
                if current_page_urls and all(url in seen_urls for url in current_page_urls):
                    consecutive_duplicate_pages += 1
                    logging.info(f"ℹ️ 第 {page_num} 页所有URL均已存在，连续重复页计数: {consecutive_duplicate_pages}")
                else:
                    consecutive_duplicate_pages = 0  # 重置计数器
                    # 将当前页URL添加到已见集合
                    seen_urls.update(current_page_urls)
                    all_results.extend(page_results)
                    # logging.info(f"✅ 第 {page_num} 页解析完成，获得 {len(page_results)} 个结果")

                # 如果没有结果，可能已到最后一页
                if not page_results:
                    logging.info(f"ℹ️ 第 {page_num} 页无搜索结果，可能已到最后一页")
                    break

                # 如果不是最后一页且未达到连续重复上限，尝试翻页
                if page_num < max_pages and consecutive_duplicate_pages < 2:
                    if not go_to_next_page(page):
                        logging.info("ℹ️ 无法翻页，可能已到最后一页")
                        break

                    # 优化：减少翻页后延时（原来2-4秒，现在1-2秒）
                    delay = random.uniform(1, 2)
                    logging.debug(f"⏱️ 翻页后等待 {delay:.1f} 秒...")
                    time.sleep(delay)

            except Exception as e:
                logging.error(f"❌ 处理第 {page_num} 页时出错: {e}")
                continue

        logging.info(f"关键词 '{keyword}' 搜索完成，共获得 {len(all_results)} 个结果")
        logging.info(f"🔄 浏览器窗口保持打开，准备搜索下一个关键字...")

    except Exception as e:
        logging.error(f"❌ 搜索关键词 '{keyword}' 时出错: {e}")
        # 如果出错，关闭浏览器页面
        if page:
            try:
                page.quit()
            except:
                pass
        raise  # 重新抛出异常

    return all_results, page  # 返回结果和页面对象


def search_keyword_with_existing_page(page: ChromiumPage, keyword: str, type_: str, time_: str,
                                      language_model: Optional[object] = None, max_pages: int = 15) -> List[Dict]:
    """使用现有浏览器页面搜索关键词（不关闭页面）

    Raises:
        SearchBoxNotFoundException: 当未找到搜索框时抛出
    """
    all_results = []
    seen_urls: Set[str] = set()  # 存储所有已见过的URL
    consecutive_duplicate_pages = 0  # 连续重复页计数器

    # logging.info(f"🔍 [复用窗口] 搜索关键词: {keyword} (文件类型: {type_})")

    # 执行搜索（如果找不到搜索框会抛出 SearchBoxNotFoundException）
    perform_search_in_browser(page, keyword, type_)

    # 逐页获取结果
    for page_num in range(1, max_pages + 1):
        # 检查是否已连续3页重复
        if consecutive_duplicate_pages >= 2:
            logging.info(f"⚠️ 已连续3页获取到重复URL，跳过当前关键词: {keyword}")
            break

        try:
            logging.info(f"📖 正在解析第 {page_num} 页搜索结果...")

            # 解析当前页面的搜索结果
            page_results = parse_search_results(page, keyword, page_num, language_model)

            # 提取当前页的所有URL
            current_page_urls = [result['srcUrl'] for result in page_results]

            # 检查当前页是否所有URL都已存在
            if current_page_urls and all(url in seen_urls for url in current_page_urls):
                consecutive_duplicate_pages += 1
                logging.info(f"ℹ️ 第 {page_num} 页所有URL均已存在，连续重复页计数: {consecutive_duplicate_pages}")
            else:
                consecutive_duplicate_pages = 0  # 重置计数器
                # 将当前页URL添加到已见集合
                seen_urls.update(current_page_urls)
                all_results.extend(page_results)
                logging.info(f"✅ 第 {page_num} 页解析完成，获得 {len(page_results)} 个结果")

            # 如果没有结果，可能已到最后一页
            if not page_results:
                logging.info(f"ℹ️ 第 {page_num} 页无搜索结果，可能已到最后一页")
                break

            # 如果不是最后一页且未达到连续重复上限，尝试翻页
            if page_num < max_pages and consecutive_duplicate_pages < 2:
                if not go_to_next_page(page):
                    logging.info("ℹ️ 无法翻页，可能已到最后一页")
                    break

                # 优化：减少翻页后延时（原来2-4秒，现在1-2秒）
                delay = random.uniform(1, 2)
                logging.debug(f"⏱️ 翻页后等待 {delay:.1f} 秒...")
                time.sleep(delay)

        except Exception as e:
            logging.error(f"❌ 处理第 {page_num} 页时出错: {e}")
            continue

    logging.info(f"关键词 '{keyword}' 搜索完成，共获得 {len(all_results)} 个结果")
    return all_results
