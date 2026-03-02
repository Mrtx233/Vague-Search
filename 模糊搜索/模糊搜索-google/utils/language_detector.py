#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
语言检测器模块
使用 FastText 模型进行文本语种识别

增强：
1) 保留原有 detect_with_threshold -> 返回语种代码（如 "es"）
2) 新增 detect_with_threshold_zh -> 返回语种中文名（如 "西班牙语"）
3) 新增 detect_language_zh -> 返回(中文名, 置信度)
4) 新增 lang_code_to_zh_name -> 代码转中文名（含 unknown 处理）
"""

import os
import logging
from typing import Tuple, Optional, List

try:
    import fasttext
except ImportError:
    fasttext = None

logger = logging.getLogger(__name__)


class LanguageDetector:
    """语言检测器类"""

    # 常见语言代码 -> 中文名（可按需要扩展）
    LANG_NAME_ZH = {
        "zh": "中文",
        "en": "英语",
        "es": "西班牙语",
        "fr": "法语",
        "de": "德语",
        "pt": "葡萄牙语",
        "it": "意大利语",
        "ru": "俄语",
        "ja": "日语",
        "ko": "韩语",
        "ar": "阿拉伯语",
        "hi": "印地语",
        "tr": "土耳其语",
        "nl": "荷兰语",
        "sv": "瑞典语",
        "no": "挪威语",
        "da": "丹麦语",
        "fi": "芬兰语",
        "pl": "波兰语",
        "cs": "捷克语",
        "el": "希腊语",
        "he": "希伯来语",
        "th": "泰语",
        "vi": "越南语",
        "id": "印尼语",
        "ms": "马来语",
        "uk": "乌克兰语",
        "ro": "罗马尼亚语",
        "hu": "匈牙利语",
        "bg": "保加利亚语",
        "sr": "塞尔维亚语",
        "hr": "克罗地亚语",
        "sk": "斯洛伐克语",
        "sl": "斯洛文尼亚语",
        "et": "爱沙尼亚语",
        "lv": "拉脱维亚语",
        "lt": "立陶宛语",
        "fa": "波斯语",
        "ur": "乌尔都语",
        "bn": "孟加拉语",
        "ta": "泰米尔语",
        "te": "泰卢固语",
        "mr": "马拉地语",
        "gu": "古吉拉特语",
        "pa": "旁遮普语",
        "sw": "斯瓦希里语",
    }

    def __init__(self, model_path: str, confidence_threshold: float = 0.8):
        """
        初始化语言检测器
        :param model_path: FastText模型文件路径
        :param confidence_threshold: 语言检测置信度阈值
        """
        self.model_path = model_path
        self.confidence_threshold = confidence_threshold
        self.model = None
        self._load_model()

    def _load_model(self) -> None:
        """加载FastText语言检测模型"""
        if fasttext is None:
            logger.error("FastText库未安装，无法进行语言检测")
            return

        try:
            if not os.path.exists(self.model_path):
                logger.error(f"FastText模型文件不存在: {self.model_path}")
                logger.error("请从 https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin 下载模型文件")
                return

            # 加载模型（禁用UTF-8警告）
            fasttext.FastText.eprint = lambda x: None
            self.model = fasttext.load_model(self.model_path)
            logger.info(f"FastText语言检测模型加载成功: {self.model_path}")

        except Exception as e:
            logger.error(f"FastText模型加载失败: {str(e)}")
            self.model = None

    def is_model_loaded(self) -> bool:
        """检查模型是否成功加载"""
        return self.model is not None

    @staticmethod
    def _normalize_code(code: str) -> str:
        """规范化语言代码（兼容 __label__xx ）"""
        if not code:
            return "unknown"
        c = code.strip().lower()
        if c.startswith("__label__"):
            c = c.replace("__label__", "")
        return c or "unknown"

    def lang_code_to_zh_name(self, code: str) -> str:
        """语言代码 -> 中文名"""
        c = self._normalize_code(code)
        if c == "unknown":
            return "未知"
        return self.LANG_NAME_ZH.get(c, f"未知({c})")

    def detect_language(self, text: str) -> Tuple[str, float]:
        """
        使用FastText识别文本语种
        :param text: 待检测文本
        :return: (语言编码, 置信度)，如("en", 0.9876)
        """
        if not self.model:
            logger.warning("FastText模型未初始化，无法执行语言检测")
            return 'unknown', 0.0

        processed_text = text.strip().replace("\n", " ").replace("\r", "")
        if not processed_text:
            logger.debug("无有效检测文本（空字符串或仅空白字符）")
            return 'unknown', 0.0

        try:
            labels, probs = self.model.predict(processed_text, k=1)
            lang_code = self._normalize_code(labels[0])
            confidence = round(float(probs[0]), 4)

            logger.debug(
                f"语言检测完成 | 文本预览: {processed_text[:50]} | "
                f"语种: {lang_code} | 置信度: {confidence}"
            )
            return lang_code, confidence

        except Exception as e:
            logger.error(
                f"语言检测失败 | 文本预览: {processed_text[:50]} | "
                f"错误信息: {str(e)[:50]}"
            )
            return 'unknown', 0.0

    def detect_with_threshold(self, text: str) -> str:
        """
        使用置信度阈值进行语言检测
        :param text: 待检测文本
        :return: 语言编码，置信度低于阈值时返回'unknown'
        """
        lang_code, confidence = self.detect_language(text)

        if confidence >= self.confidence_threshold:
            return lang_code
        else:
            logger.debug(f"语言检测置信度过低 ({confidence:.4f} < {self.confidence_threshold})，返回unknown")
            return 'unknown'

    # ✅ 新增：阈值检测后直接返回中文名
    def detect_with_threshold_zh(self, text: str) -> str:
        """
        使用置信度阈值进行语言检测（返回中文名）
        :param text: 待检测文本
        :return: 语言中文名，置信度低于阈值时返回'未知'
        """
        code = self.detect_with_threshold(text)
        return self.lang_code_to_zh_name(code)

    # ✅ 新增：直接返回(中文名, 置信度)
    def detect_language_zh(self, text: str) -> Tuple[str, float]:
        """
        识别文本语种（返回中文名）
        :param text: 待检测文本
        :return: (中文名, 置信度)
        """
        code, conf = self.detect_language(text)
        return self.lang_code_to_zh_name(code), conf

    def batch_detect(self, texts: List[str]) -> List[Tuple[str, float]]:
        """
        批量语言检测
        :param texts: 文本列表
        :return: (语言编码, 置信度) 元组列表
        """
        if not self.model:
            return [('unknown', 0.0)] * len(texts)

        results = []
        for text in texts:
            results.append(self.detect_language(text))

        return results

    def get_supported_languages(self) -> List[str]:
        """
        获取模型支持的语言列表
        :return: 支持的语言编码列表
        """
        if not self.model:
            return []

        try:
            labels = self.model.get_labels()
            languages = [self._normalize_code(label) for label in labels]
            return sorted(set(languages))
        except Exception as e:
            logger.error(f"获取支持语言列表失败: {str(e)}")
            return []

    def reload_model(self) -> bool:
        """
        重新加载模型
        :return: 是否加载成功
        """
        self._load_model()
        return self.is_model_loaded()

    def set_confidence_threshold(self, threshold: float) -> None:
        """
        设置置信度阈值
        :param threshold: 新的置信度阈值 (0.0-1.0)
        """
        if 0.0 <= threshold <= 1.0:
            self.confidence_threshold = threshold
            logger.info(f"语言检测置信度阈值已更新为: {threshold}")
        else:
            logger.warning(f"无效的置信度阈值: {threshold}，应在0.0-1.0之间")

    def get_model_info(self) -> dict:
        """
        获取模型信息
        :return: 模型信息字典
        """
        if not self.model:
            return {"loaded": False, "path": self.model_path}

        try:
            return {
                "loaded": True,
                "path": self.model_path,
                "confidence_threshold": self.confidence_threshold,
                "supported_languages_count": len(self.get_supported_languages())
            }
        except Exception:
            return {"loaded": True, "path": self.model_path, "error": "无法获取详细信息"}
