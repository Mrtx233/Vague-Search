#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @Author : myxfc
# 更新时间 2025/08/18 封装为类

from urllib.parse import urlencode
import requests
import hashlib
import time
import json
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import base64


class YoudaoTranslator:
    def __init__(self):
        # 初始化密钥和参数
        self.secretKey_param = "SRz6r3IGA6lj9i5zW0OYqgVZOtLDQe3e"
        self.aes_iv_str = "ydsecret://query/iv/C@lZe2YzHtZ2CYgaXKSVfsb7Y4QWHjITPPZ0nQp87fBeJ!Iv6v^6fvi2WN@bYpJ4"
        self.aes_key_str = "ydsecret://query/key/B*RGygVywfNBwpmBaZg*WT7SIOUP2T0C9WHMZN39j^DAdaZhAnxvGcCY6VYFwnHl"

        # 计算密钥和IV
        self.iv = self.md5_hash(self.aes_iv_str)
        self.key = self.md5_hash(self.aes_key_str)

        # 请求相关配置
        self.url = 'https://dict.youdao.com/webtranslate'
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "connection": "keep-alive",
            "Cookie": "OUTFOX_SEARCH_USER_ID=-57891657@125.86.188.112; OUTFOX_SEARCH_USER_ID_NCOO=118049373.81209917; _uetsid=54ad8ce0060011f0a15787a3554a5b20; _uetvid=54ade1c0060011f09c2211cd64baad7a; DICT_DOCTRANS_SESSION_ID=ZDlmNTMyNDYtOTdjZS00Y2MzLTkwZDktN2IzY2Q4NjM5MDVj",
            "host": "dict.youdao.com",
            "origin": "https://fanyi.youdao.com",
            "referer": "https://fanyi.youdao.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
        }

    def md5_hash(self, s):
        """生成MD5哈希值"""
        md5 = hashlib.md5()
        md5.update(s.encode('utf-8'))
        return md5.digest()  # 返回bytes类型

    def decrypt(self, ciphertext):
        """解密响应数据"""
        # 处理URL安全的Base64并移除干扰字符
        ciphertext = ciphertext.replace('-', '+').replace('_', '/').replace(' ', '')
        try:
            cipher_bytes = base64.b64decode(ciphertext, validate=True)
            aes_cipher = AES.new(self.key, AES.MODE_CBC, self.iv)
            decrypted = aes_cipher.decrypt(cipher_bytes)
            return unpad(decrypted, AES.block_size).decode('utf-8')
        except (ValueError, TypeError) as e:
            print(f"解密失败: {str(e)}")
            return None

    def translate(self, text, target_lang):
        """
        翻译文本
        :param text: 要翻译的文本
        :param target_lang: 目标语言代码，如'ar'表示阿拉伯语，'zh-CHS'表示中文
        :return: 翻译结果的tgt值，如果失败返回None
        """
        try:
            # 生成时间戳和签名
            mystic_time = str(int(time.time() * 1000))
            sign_str = f"client=fanyideskweb&mysticTime={mystic_time}&product=webfanyi&key={self.secretKey_param}"
            sign = hashlib.md5(sign_str.encode()).hexdigest()

            # 构建请求数据
            data = {
                "i": text,
                "from": "auto",
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

            # 发送请求
            response = requests.post(self.url, headers=self.headers, data=data, timeout=10)
            response.raise_for_status()  # 处理HTTP错误

            # 解密响应
            encrypted_data = response.text.strip()
            decrypted_text = self.decrypt(encrypted_data)

            if not decrypted_text:
                print("无法解密响应数据")
                return None

            # 解析JSON并提取tgt值
            result = json.loads(decrypted_text)
            if result.get("code") == 0 and result.get("translateResult"):
                return result["translateResult"][0][0]["tgt"]
            else:
                print(f"翻译失败: {result.get('message', '未知错误')}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"请求异常: {str(e)}")
            return None
        except json.JSONDecodeError:
            print("响应不是有效JSON格式")
            return None
        except (KeyError, IndexError) as e:
            print(f"解析翻译结果失败: {str(e)}")
            return None



if __name__ == "__main__":
    translator = YoudaoTranslator()

    # 示例1: 英文翻译成阿拉伯语
    result1 = translator.translate("happy", "ar")
    print(f"英文到阿拉伯语: {result1}")  # 应输出: سعيدة

    # 示例2: 中文翻译成英语
    result2 = translator.translate("你好", "en")
    print(f"中文到英语: {result2}")  # 应输出: hello
