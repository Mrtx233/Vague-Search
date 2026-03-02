import base64
import requests
import hashlib
import time
import re
import json
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


class YoudaoTranslator:
    """有道翻译工具类
    
    使用示例：
        translator = YoudaoTranslator()
        result = translator.translate("Hello World", "zh-CHS")  # 翻译成中文
        result = translator.translate("你好世界", "en")  # 翻译成英语
    
    支持的语言代码：
        zh-CHS: 中文简体
        zh-CHT: 中文繁体
        en: 英语
        ja: 日语
        ko: 韩语
        fr: 法语
        es: 西班牙语
        pt: 葡萄牙语
        it: 意大利语
        ru: 俄语
        de: 德语
        ar: 阿拉伯语
        auto: 自动检测（仅用于源语言）
    """
    
    def __init__(self, cookie=None):
        """初始化翻译器
        
        Args:
            cookie: 可选的cookie字符串，如果不提供则使用默认cookie
        """
        if cookie is None:
            # 使用默认cookie
            self.cookie = "OUTFOX_SEARCH_USER_ID=1145141919@114.51.41.91; _uetsid=faadcbd1145141810a082c1e8b007b95c; _uetvid=faadcbd1145141810a082c1e8b007b95c; OUTFOX_SEARCH_USER_ID_NCOO=1145141919.8109926; DICT_DOCTRANS_SESSION_ID=MTE0NTE0MTkxOTgxMGFiY2RlZmc="
        else:
            self.cookie = cookie
    
    def _get_product_keys(self, key="webfanyi", use_temp=False):
        """获取有道翻译产品ID与产品密钥（私有方法）"""
        if use_temp:
            return "webfanyi-key-getter-2025", "yU5nT5dK3eZ1pI4j"
        
        try:
            url = "https://shared.ydstatic.com/dict/translation-website/0.6.6/js/app.78e9cb0d.js"
            data = requests.get(url, timeout=10).text
            keyid_pattern = r'async\(\{commit:e\},t\)\=\>\{const\s+a="' + key + '([^"]+)",n="([^"]+)"'
            match = re.search(keyid_pattern, data)
            if match:
                keyid = key + match.group(1)
                const_sign = match.group(2)
                return keyid, const_sign
        except Exception as e:
            print(f"获取产品密钥失败，使用默认值: {e}")
        
        return "webfanyi-key-getter-2025", "yU5nT5dK3eZ1pI4j"
    
    def _get_sign(self, const_sign):
        """根据产品密钥生成加密签名（私有方法）"""
        mystic_time = str(int(time.time() * 1000))
        sign = f"client=fanyideskweb&mysticTime={mystic_time}&product=webfanyi&key={const_sign}"
        return hashlib.md5(sign.encode('utf-8')).hexdigest()
    
    def _get_keys(self):
        """获取有道secretKey和AES加密的密钥（私有方法）"""
        keyid, const_sign = self._get_product_keys()
        sign = self._get_sign(const_sign)
        
        req = requests.get(
            "https://dict.youdao.com/webtranslate/key",
            params={
                "keyid": keyid,
                "sign": sign,
                "client": "fanyideskweb",
                "product": "webfanyi",
                "appVersion": "1.0.0",
                "vendor": "web",
                "pointParam": "client,mysticTime,product",
                "mysticTime": str(int(time.time() * 1000)),
                "keyfrom": "fanyi.web",
                "mid": "1",
                "screen": "1",
                "model": "1",
                "network": "wifi",
                "abtest": "0",
                "yduuid": "abcdefg"
            },
            headers={
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Connection": "keep-alive",
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://fanyi.youdao.com",
                "Referer": "https://fanyi.youdao.com/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "sec-ch-ua": "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "cookie": self.cookie
            },
            timeout=10
        )
        
        return req.json()['data']
    
    def translate(self, text, target_lang="zh-CHS", source_lang="auto"):
        """翻译文本
        
        Args:
            text: 要翻译的文本
            target_lang: 目标语言，默认为中文简体(zh-CHS)
                可选值：zh-CHS(中文简体), zh-CHT(中文繁体), en(英语), ja(日语), 
                       ko(韩语), fr(法语), es(西班牙语), pt(葡萄牙语), it(意大利语), 
                       ru(俄语), de(德语), ar(阿拉伯语)等
            source_lang: 源语言，默认为auto(自动检测)
        
        Returns:
            dict: 翻译结果字典，包含翻译文本等信息
        
        Raises:
            Exception: 翻译过程中出现错误
        """
        try:
            # 获取密钥信息
            keys = self._get_keys()
            aeskey = keys['aesKey']
            aesiv = keys['aesIv']
            secret_key = keys['secretKey']
            
            sign = self._get_sign(secret_key)
            mystic_time = str(int(time.time() * 1000))
            
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Connection": "keep-alive",
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://fanyi.youdao.com",
                "Referer": "https://fanyi.youdao.com/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "sec-ch-ua": "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "cookie": self.cookie
            }
            
            data = {
                "i": text,
                "from": source_lang,
                "to": target_lang,
                "useTerm": "false",
                "dictResult": "true",
                "keyid": "webfanyi",
                "sign": sign,
                "client": "fanyideskweb",
                "product": "webfanyi",
                "appVersion": "1.0.0",
                "vendor": "web",
                "pointParam": "client,mysticTime,product",
                "mysticTime": mystic_time,
                "keyfrom": "fanyi.web",
                "mid": "1",
                "screen": "1",
                "model": "1",
                "network": "wifi",
                "abtest": "0",
                "yduuid": "abcdefg"
            }
            
            req = requests.post(
                "https://dict.youdao.com/webtranslate",
                data=data,
                headers=headers,
                timeout=10
            )
            
            # AES解密
            encode_aes_key = hashlib.md5(aeskey.encode()).digest()
            encode_aes_iv = hashlib.md5(aesiv.encode()).digest()
            
            cipher = AES.new(encode_aes_key, AES.MODE_CBC, encode_aes_iv)
            ctxs = base64.urlsafe_b64decode(req.text)
            
            decrypted = unpad(cipher.decrypt(ctxs), AES.block_size)
            result = json.loads(decrypted.decode('utf-8'))
            
            return result
            
        except Exception as e:
            raise Exception(f"翻译失败: {str(e)}")
    
    def get_translation_text(self, text, target_lang="zh-CHS", source_lang="auto"):
        """获取纯翻译文本（简化版）
        
        Args:
            text: 要翻译的文本
            target_lang: 目标语言，默认为中文简体(zh-CHS)
            source_lang: 源语言，默认为auto(自动检测)
        
        Returns:
            str: 翻译后的文本
        """
        try:
            result = self.translate(text, target_lang, source_lang)
            
            # 提取翻译文本
            if 'translateResult' in result:
                translations = []
                for item in result['translateResult']:
                    for trans in item:
                        if 'tgt' in trans:
                            translations.append(trans['tgt'])
                return ''.join(translations)
            
            return ""
            
        except Exception as e:
            return f"翻译出错: {str(e)}"


if __name__ == "__main__":
    # 使用示例
    translator = YoudaoTranslator()
    
    # 示例1：翻译英文到中文
    print("示例1 - 英文翻译成中文：")
    result = translator.get_translation_text("World is just a large loop.", "zh-CHS")
    print(f"翻译结果: {result}\n")
    
    # 示例2：翻译中文到英文
    print("示例2 - 中文翻译成英文：")
    result = translator.get_translation_text("你好，世界！", "en")
    print(f"翻译结果: {result}\n")
    
    # 示例3：获取完整的翻译结果（JSON格式）
    print("示例3 - 获取完整翻译结果：")
    full_result = translator.translate("Hello", "zh-CHS")
    print(f"完整结果: {json.dumps(full_result, ensure_ascii=False, indent=2)}")
  