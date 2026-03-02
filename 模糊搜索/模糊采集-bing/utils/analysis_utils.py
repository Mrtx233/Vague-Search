import logging
import tldextract
from urllib.parse import urlparse
from typing import Dict, Optional, Tuple

# 尝试导入FastText，如果导入失败则设为None
try:
    import fasttext
    fasttext_available = True
except ImportError:
    fasttext = None
    fasttext_available = False

# 域名分类关键词映射表
URL_CLASS_KEYWORDS = {
    "GOV": [
        "gov", "gouv", "gob", "gov.cn", "gov.uk", "gov.au", "gob.mx", "gov.sk",
        "iuventa.sk", "ministerio", "dept", "agency", "state", "federal", "municipio", "prefecture",
        "govt", "government", "county", "cityhall", "municipal", "provincial", "regulatory",
        "authority", "federalreserve", "irs", "fbi", "garda", "police.uk", "gc.ca", "govt.nz",
        "bund.de", "kementerian", "presidency", "parliament", "congress", "senate", "council"
    ],
    "EDU": [
        "edu", "ac", "university", "college", "institute", "academic", "research", "science",
        "edu.cn", "ac.uk", "edu.au", "uni.", "polytechnic", "scholar", "professor", "lab", "campus",
        "school", "kindergarten", "preschool", "highschool", "middleschool", "primaryschool",
        "vocational", "technicalschool", "academy", "faculty", "thesis", "dissertation",
        "scholarship", "student", "alumni", "tutor", "mentor", "curriculum", "semester", "credit",
        "degree", "certificate", "phd", "master", "bachelor", "department"
    ],
    "EDUCOMM": [
        "coursera", "udemy", "edx", "mooc", "khanacademy", "futurelearn", "skillshare",
        "pluralsight", "udacity", "udemycdn", "linkedinlearning", "codecademy", "lynda",
        "treehouse", "datacamp", "opensap", "masterclass", "skillsoft", "open2study", "alison",
        "udemyforbusiness", "codewars", "freecodecamp", "edmodo", "classdojo", "duolingo",
        "memrise", "babbel", "busuu", "lingoda", "edxforbusiness", "pluralsightone", "codecademypro"
    ],
    "ORG": [
        "un.org", "who.int", "unesco", "imf", "ilo", "worldbank", "ngo", "wto",
        "icrc", "org", "redcross", "oxfam", "greenpeace", "amnesty", "transparency.org",
        "icj", "icc", "union", "association", "alliance", "council", "forum",
        "oecd", "g20", "asean", "nato", "fao", "rotary", "lionsclub", "charity", "foundation",
        "trust", "coalition", "network", "initiative", "movement", "society", "msf", "care",
        "billgatesfoundation", "rockefellerfoundation", "gatesfoundation", "wef", "club"
    ],
    "WIKI": [
        "wikipedia", "wikidata", "wiktionary", "baike.baidu", "hudong", "citizendium",
        "encarta", "britannica", "factmonster", "infoplease", "scribd",
        "wikiwand", "fandom", "wikisource", "wikimedia", "wikinews", "wikiversity",
        "encyclopedia", "columbiaencyclopedia", "worldbook", "grolier", "sogoubaike", "360baike",
        "wikipediafoundation", "baiduencyclopedia"
    ],
    "NEWS": [
        "bbc", "cnn", "reuters", "xinhuanet", "nytimes", "dw.com", "guardian", "ft.com",
        "bloomberg", "cnbc", "apnews", "aljazeera", "huffpost", "vox.com", "wsj.com",
        "time.com", "economist", "thetimes", "telegraph", "chinatimes", "japantimes",
        "nbcnews", "abcnews", "cbsnews", "foxnews", "msnbc", "usa today", "latimes", "washingtonpost",
        "atlantic", "newyorker", "nationalgeographic", "sciencemag", "nature", "chinadaily",
        "globaltimes", "southchinamorningpost", "asahishimbun", "yomiurishimbun", "afp",
        "people.com.cn", "cctv.com", "凤凰网", "澎湃新闻", "界面新闻"
    ],
    "COMMERCE": [
        "amazon", "ebay", "alibaba", "taobao", "jd.com", "shop", "mall", "retail", "bestbuy",
        "walmart", "tmall", "flipkart", "rakuten", "mercadolibre",
        "ecommerce", "shopify", "etsy", "zappos", "sephora", "macys", "target", "costco",
        "aldi", "lidl", "carrefour", "newegg", "pinduoduo", "suning", "gome", "shein", "asos",
        "zara", "h&m", "uniqlo", "nike", "adidas", "paypal", "stripe", "shopee", "lazada",
        "coupang", "mercado", "wayfair", "overstock", "chewy", "dell", "hp", "apple", "samsung"
    ],
    "SOCIAL": [
        "facebook", "twitter", "instagram", "linkedin", "tiktok", "wechat", "weibo", "snapchat",
        "reddit", "pinterest", "discord", "telegram", "quora",
        "whatsapp", "qq", "douban", "xiaohongshu", "bilibili", "tumblr", "flickr", "vimeo",
        "twitch", "medium", "slack", "microsoftteams", "signal", "mastodon", "mewe", "parler",
        "periscope", "vine", "foursquare", "goodreads", "letterboxd", "behance", "dribbble",
        "kuaishou", "xiaohongshu", "douyin", "kwai", "line", "kakao"
    ]
}

def detect_language(text: str, model: Optional[object]) -> Tuple[str, float]:
    """
    使用FastText识别文本语种
    :param text: 待检测文本
    :param model: FastText预训练模型实例
    :return: (语言编码, 置信度)，如("en", 0.9876)
    """
    # 模型未加载时直接返回未知
    if not model:
        logging.debug("FastText模型未初始化，无法执行语言检测")
        return 'unknown', 0.0

    # 文本为空时返回未知
    processed_text = text.strip().replace("\n", " ").replace("\r", "")
    if not processed_text:
        logging.debug("无有效检测文本（空字符串或仅空白字符）")
        return 'unknown', 0.0

    try:
        # 预测最可能的1种语言
        labels, probs = model.predict(processed_text, k=1)
        # 提取语言编码（FastText标签格式为__label__xx）
        lang_code = labels[0].replace("__label__", "").lower()
        confidence = round(probs[0], 4)  # 置信度保留4位小数

        logging.debug(
            f"语言检测完成 | 文本预览: {processed_text[:50]} | "
            f"语种: {lang_code} | 置信度: {confidence}"
        )
        return lang_code, confidence

    except Exception as e:
        logging.error(
            f"语言检测失败 | 文本预览: {processed_text[:50]} | "
            f"错误信息: {str(e)[:50]}"
        )
        return 'unknown', 0.0

def extract_domain_parts(url: str) -> Dict[str, str]:
    """提取URL的域名信息，返回包含full_host的字典（用于webSite字段）"""
    try:
        parsed_url = urlparse(url)
        full_host = parsed_url.hostname or ""

        # 若hostname为空，用tldextract补充解析
        if not full_host:
            ext = tldextract.extract(url)
            full_host = f"{ext.subdomain}.{ext.domain}.{ext.suffix}".lstrip('.')  # 去除开头多余的点

        ext = tldextract.extract(url)
        return {
            "subdomain": ext.subdomain,
            "domain": ext.domain,
            "suffix": ext.suffix,
            "registered_domain": ext.registered_domain or ext.domain + '.' + ext.suffix if ext.domain and ext.suffix else "",
            "full_host": full_host
        }
    except Exception as e:
        logging.warning(f"URL域名提取失败: {url} -> {str(e)[:30]}")
        return {"full_host": "", "subdomain": "", "domain": "", "suffix": "", "registered_domain": ""}

def determine_domain_class(full_host: str, suffix: str) -> str:
    """根据域名和后缀确定doMain分类"""
    # 组合完整的域名信息用于匹配
    domain_info = f"{full_host}.{suffix}".lower() if full_host else suffix.lower()
    full_host_lower = full_host.lower()

    # 特殊优先级处理 - WIKI类网站优先级最高
    wiki_keywords = URL_CLASS_KEYWORDS.get("WIKI", [])
    for keyword in wiki_keywords:
        if keyword.lower() in full_host_lower or keyword.lower() in domain_info:
            return "WIKI"
    
    # 按优先级顺序匹配分类（WIKI已经处理过，跳过）
    for domain_class, keywords in URL_CLASS_KEYWORDS.items():
        if domain_class == "WIKI":
            continue  # 跳过WIKI，已经处理过
        for keyword in keywords:
            if keyword.lower() in domain_info:
                return domain_class
    
    return ""  # 无匹配时返回空字符串

