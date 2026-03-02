#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
域名分类器模块
用于根据URL域名信息进行网站分类
"""

import json
import logging
import tldextract
from urllib.parse import urlparse
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class DomainClassifier:
    """域名分类器类"""
    
    def __init__(self, config_path: str):
        """
        初始化域名分类器
        :param config_path: 域名分类配置文件路径
        """
        self.config_path = config_path
        self.url_class_keywords = {}
        self._load_config()
    
    def _load_config(self) -> None:
        """从JSON文件加载域名分类关键词配置"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            # 验证配置格式
            if not isinstance(config_data, dict):
                raise ValueError("配置文件格式错误：根节点必须是字典")
            
            # 验证每个分类的关键词列表
            for category, keywords in config_data.items():
                if not isinstance(keywords, list):
                    raise ValueError(f"配置文件格式错误：分类 '{category}' 的值必须是列表")
                if not all(isinstance(kw, str) for kw in keywords):
                    raise ValueError(f"配置文件格式错误：分类 '{category}' 中包含非字符串关键词")
            
            self.url_class_keywords = config_data
            total_keywords = sum(len(kws) for kws in config_data.values())
            logger.info(f"域名分类配置加载成功：{len(config_data)} 个分类，共 {total_keywords} 个关键词")
        
        except FileNotFoundError:
            logger.error(f"域名分类配置文件不存在: {self.config_path}")
            self.url_class_keywords = {}
        except json.JSONDecodeError as e:
            logger.error(f"域名分类配置文件JSON格式错误: {str(e)}")
            self.url_class_keywords = {}
        except Exception as e:
            logger.error(f"域名分类配置文件加载失败: {str(e)}")
            self.url_class_keywords = {}
    
    def is_config_loaded(self) -> bool:
        """检查配置是否成功加载"""
        return bool(self.url_class_keywords)
    
    def extract_domain_parts(self, url: str) -> Dict[str, str]:
        """
        提取URL的域名信息，返回包含full_host的字典（用于webSite字段）
        :param url: 待解析的URL
        :return: 域名信息字典
        """
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
            logger.warning(f"URL域名提取失败: {url} -> {str(e)[:30]}")
            return {"full_host": "", "subdomain": "", "domain": "", "suffix": "", "registered_domain": ""}
    
    def determine_domain_class(self, full_host: str, suffix: str) -> str:
        """
        根据域名和后缀确定域名分类
        :param full_host: 完整主机名
        :param suffix: 域名后缀
        :return: 域名分类字符串，未匹配时返回空字符串
        """
        if not self.url_class_keywords:
            return ""
        
        # 组合完整的域名信息用于匹配
        domain_info = f"{full_host}.{suffix}".lower() if full_host else suffix.lower()

        # 按优先级顺序匹配分类（靠前的分类优先级更高）
        for domain_class, keywords in self.url_class_keywords.items():
            for keyword in keywords:
                if keyword.lower() in domain_info:
                    return domain_class

        # 未匹配到任何分类则返回空
        return ""
    
    def classify_url(self, url: str) -> Dict[str, str]:
        """
        对URL进行完整的域名分类分析
        :param url: 待分析的URL
        :return: 包含域名信息和分类结果的字典
        """
        domain_info = self.extract_domain_parts(url)
        domain_class = self.determine_domain_class(
            domain_info["full_host"], 
            domain_info["suffix"]
        )
        
        return {
            **domain_info,
            "domain_class": domain_class
        }
    
    def reload_config(self) -> bool:
        """
        重新加载配置文件
        :return: 是否加载成功
        """
        self._load_config()
        return self.is_config_loaded()
    
    def get_categories(self) -> List[str]:
        """获取所有可用的分类名称"""
        return list(self.url_class_keywords.keys())
    
    def get_keywords_for_category(self, category: str) -> List[str]:
        """
        获取指定分类的关键词列表
        :param category: 分类名称
        :return: 关键词列表
        """
        return self.url_class_keywords.get(category, [])
