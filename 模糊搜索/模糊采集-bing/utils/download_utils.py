import requests
import os
import time
import random
import logging
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_fixed
from typing import Optional

@retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
def download_file(url: str, save_path: str, timeout: int = 30) -> bool:
    """下载文件，支持断点续传"""
    try:
        # 检查文件是否已存在（断点续传）
        resume_header = {}
        if os.path.exists(save_path):
            existing_size = os.path.getsize(save_path)
            resume_header['Range'] = f'bytes={existing_size}-'
            logging.info(f"断点续传，已下载 {existing_size} 字节")

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
            **resume_header
        }

        response = requests.get(url, headers=headers, stream=True, timeout=timeout, verify=False,)

        # 处理断点续传响应
        if response.status_code == 206:  # 部分内容
            mode = 'ab'
        elif response.status_code == 200:  # 完整内容
            mode = 'wb'
        else:
            response.raise_for_status()

        # 写入文件
        with open(save_path, mode) as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        logging.info(f"文件下载成功: {save_path}")
        return True

    except Exception as e:
        logging.error(f"下载文件失败 {url}: {e}")
        # 删除不完整的文件
        if os.path.exists(save_path):
            try:
                os.remove(save_path)
            except:
                pass
        return False

def get_proxy():
    """获取代理配置"""
    proxyHost = "u10097.20.tp.16yun.cn"
    proxyPort = "6447"
    proxyUser = "16XUBRYU"
    proxyPass = "669482"
    proxyMeta = f"http://{proxyUser}:{proxyPass}@{proxyHost}:{proxyPort}"
    proxies = {
        "HTTP": "http://127.0.0.1:7897",
        "HTTPS": "http://127.0.0.1:7897",
    }
    tunnel = random.randint(1, 10000)
    headers = {"Proxy-Tunnel": str(tunnel)}
    return proxies


