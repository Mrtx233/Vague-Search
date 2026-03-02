import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Set
import threading


def calculate_md5(file_path: str) -> Optional[str]:
    """计算文件的MD5值"""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logging.error(f"计算文件MD5失败 {file_path}: {e}")
        return None


def generate_filename_from_md5(md5_hash: str, file_type: str) -> str:
    """使用MD5值生成文件名"""
    try:
        # 确定文件扩展名
        if file_type:
            ext = file_type if file_type.startswith('.') else f'.{file_type}'
        else:
            ext = '.xlsx'  # 默认扩展名

        return f"{md5_hash}{ext}"

    except Exception as e:
        logging.error(f"生成文件名失败: {e}")
        # 备用方案：使用时间戳
        timestamp = int(time.time())
        return f"{timestamp}.bin"


def remove_chars(string):
    """清理文件名中的特殊字符"""
    special_chars = ["/", "<", '\n', '\r', '\\', '*', '?', '"', ' ', '|', '\t', ">", '\u3000', ':', '\xa0', '\x20']
    for char in special_chars:
        string = string.replace(char, '')
    return string


class JSONLManager:
    """JSONL文件管理器"""

    def __init__(self, jsonl_file: Path):
        self.jsonl_file = jsonl_file
        self.lock = threading.RLock()

    def write_record(self, record: Dict):
        """写入单条JSONL记录"""
        with self.lock:
            try:
                with open(self.jsonl_file, 'a', encoding='utf-8') as f:
                    json_line = json.dumps(record, ensure_ascii=False)
                    f.write(json_line + '\n')
                logging.debug(f"写入JSONL记录: {record['srcUrl']}")
            except Exception as e:
                logging.error(f"写入JSONL记录失败: {e}")

    def remove_record_by_url(self, url: str):
        """根据URL从JSONL文件中删除对应记录"""
        with self.lock:
            try:
                if not self.jsonl_file.exists():
                    return

                # 读取所有记录
                records = []
                with open(self.jsonl_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                record = json.loads(line)
                                # 保留不匹配的记录
                                if record.get('srcUrl') != url:
                                    records.append(record)
                                else:
                                    logging.info(f"从JSONL中删除下载失败的记录: {url}")
                            except json.JSONDecodeError:
                                # 保留无法解析的行
                                records.append({'_raw_line': line})

                # 重写文件
                with open(self.jsonl_file, 'w', encoding='utf-8') as f:
                    for record in records:
                        if '_raw_line' in record:
                            f.write(record['_raw_line'] + '\n')
                        else:
                            json_line = json.dumps(record, ensure_ascii=False)
                            f.write(json_line + '\n')

            except Exception as e:
                logging.error(f"从JSONL删除记录失败: {e}")

    def update_record_hash(self, url: str, hash_value: str):
        """更新JSONL记录中的hash字段"""
        with self.lock:
            try:
                if not self.jsonl_file.exists():
                    return

                # 读取所有记录
                records = []
                updated = False
                with open(self.jsonl_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                record = json.loads(line)
                                # 更新匹配的记录
                                if record.get('srcUrl') == url:
                                    record['hash'] = hash_value
                                    updated = True
                                    logging.debug(f"更新JSONL记录hash: {url} -> {hash_value}")
                                records.append(record)
                            except json.JSONDecodeError:
                                # 保留无法解析的行
                                records.append({'_raw_line': line})

                # 如果有更新，重写文件
                if updated:
                    with open(self.jsonl_file, 'w', encoding='utf-8') as f:
                        for record in records:
                            if '_raw_line' in record:
                                f.write(record['_raw_line'] + '\n')
                            else:
                                json_line = json.dumps(record, ensure_ascii=False)
                                f.write(json_line + '\n')

            except Exception as e:
                logging.error(f"更新JSONL记录hash失败: {e}")

    def load_finished_keywords(self) -> Set[str]:
        """从JSONL文件中加载已处理的关键字"""
        finished_keywords = set()
        try:
            if self.jsonl_file.exists():
                with open(self.jsonl_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                record = json.loads(line)
                                keyword = record.get('extend', {}).get('keyword')
                                if keyword:
                                    finished_keywords.add(keyword)
                            except json.JSONDecodeError:
                                continue
                logging.debug(f"从JSONL文件加载了 {len(finished_keywords)} 个已处理关键字")
            return finished_keywords
        except Exception as e:
            logging.error(f"从JSONL文件加载关键字失败: {e}")
            return set()

    def get_incomplete_records(self) -> tuple[List[Dict], List[Dict]]:
        """获取未完成下载的记录和已完成的记录"""
        incomplete_records = []
        complete_records = []

        try:
            if self.jsonl_file.exists():
                with open(self.jsonl_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                record = json.loads(line)
                                if not record.get('hash'):  # hash为空或不存在
                                    incomplete_records.append(record)
                                else:
                                    complete_records.append(record)
                            except json.JSONDecodeError:
                                continue
        except Exception as e:
            logging.error(f"❌ 读取JSONL文件失败: {e}")

        return incomplete_records, complete_records


