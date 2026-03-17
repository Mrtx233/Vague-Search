"""
JSON批量翻译工具 - 使用有道翻译API
支持多线程并发翻译，自动管理翻译进度

使用方法（已按你的需求改为“固定目录批量遍历”）：
    1. 把需要翻译的JSON文件放到：模糊搜索/json/input   （相对本脚本）
    2. 运行此脚本，按提示输入：需要翻译的JSON键值、目标语种
    3. 脚本会遍历 input 目录下所有 .json 文件并翻译
    4. 输出保存到：模糊搜索/json/output/语种中文名/  （相对本脚本）

注意：
    - JSON文件内容必须是列表格式（list）
    - 不修改翻译相关逻辑（多线程、调用有道API、写入字段等保持不变）
"""

import json
import logging
import threading
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from youdao_api import YoudaoTranslator

# -------------------------- 路径基准（绝对路径写死） --------------------------
DEFAULT_INPUT_DIR = r"D:\code_Python\Vague-Search\模糊搜索\json\output_json\new"
DEFAULT_OUTPUT_DIR = r"D:\code_Python\Vague-Search\模糊搜索\json\output"



# -------------------------- 0. 语言代码映射（仅用于输出路径目录名） --------------------------
LANG_NAME_MAP = {
    "zh-CHS": "中文简体",
    "zh-CHT": "中文繁体",
    "en": "英语",
    "ja": "日语",
    "ko": "韩语",
    "fr": "法语",
    "es": "西班牙语",
    "pt": "葡萄牙语",
    "it": "意大利语",
    "ru": "俄语",
    "de": "德语",
    "ar": "阿拉伯语",
    "th": "泰语",
    "pl": "波兰语",
    "id": "印度尼西亚语",
    "tr": "土耳其语",
}


# -------------------------- 1. 用户输入配置（已改：不再输入文件路径） --------------------------
def get_user_config():
    """获取用户输入的配置信息（输入目录固定遍历）"""
    config = {}

    # 固定输入目录
    config["input_dir"] = DEFAULT_INPUT_DIR

    # 获取需要翻译的字段名
    config["source_field"] = input("请输入需要翻译的JSON键值（例如：中文）: ").strip()

    # 获取目标语种
    print("\n支持的语言代码：")
    print("  zh-CHS(中文简体)  zh-CHT(中文繁体)  en(英语)  ja(日语)")
    print("  ko(韩语)  fr(法语)  es(西班牙语)  pt(葡萄牙语)")
    print("  it(意大利语)  ru(俄语)  de(德语)  ar(阿拉伯语)")
    print("  th(泰语)  pl(波兰语)  id(印度尼西亚语)  tr(土耳其语)")
    config["target_language"] = input("\n请输入目标语种代码（例如：es 或 en）: ").strip()

    # 固定输出目录：output/语种中文名/
    lang_cn = LANG_NAME_MAP.get(config["target_language"], config["target_language"])
    config["output_dir"] = os.path.join(DEFAULT_OUTPUT_DIR, lang_cn)

    # 扫描 input 目录下的 json 文件
    json_files = list_json_files(config["input_dir"])
    config["json_files"] = json_files

    # 显示配置信息确认
    print("\n===== 配置信息确认 =====")
    print(f"输入目录: {config['input_dir']}")
    print(f"扫描到JSON文件数量: {len(json_files)}")
    if len(json_files) <= 20:
        for p in json_files:
            print(f"  - {os.path.basename(p)}")
    else:
        for p in json_files[:10]:
            print(f"  - {os.path.basename(p)}")
        print(f"  ...（省略）...  共 {len(json_files)} 个文件")

    print(f"输出目录: {config['output_dir']}")
    print(f"需要翻译的字段: {config['source_field']}")
    print(f"目标语种: {config['target_language']}")

    confirm = input("\n确认开始翻译？(y/n): ").strip().lower()
    if confirm != "y":
        print("已取消翻译操作")
        exit(0)

    return config


def list_json_files(input_dir: str):
    """遍历 input_dir 下所有 .json 文件（仅一层，不递归）"""
    if not os.path.exists(input_dir) or not os.path.isdir(input_dir):
        raise FileNotFoundError(f"输入目录不存在或不是目录: {input_dir}")

    files = []
    for name in os.listdir(input_dir):
        path = os.path.join(input_dir, name)
        if os.path.isfile(path) and name.lower().endswith(".json"):
            files.append(path)

    files.sort()
    return files


# -------------------------- 2. 配置基础参数 --------------------------
# 多线程配置
MAX_WORKERS = 3  # 并发线程数，建议不要太大，避免触发反爬（有道翻译建议3-5个）
BATCH_SIZE = 10  # 每批处理数量，用于控制整体节奏
# Cookie配置（可选，如果默认cookie失效可以在这里设置自定义cookie）
CUSTOM_COOKIE = None  # 示例："OUTFOX_SEARCH_USER_ID=xxx; ..."


# -------------------------- 3. 初始化翻译API --------------------------
def init_translate_api(js_path=None, proxy=None):
    """初始化翻译API（现在使用有道翻译）"""
    try:
        # 使用有道翻译器
        translator = YoudaoTranslator()
        return translator
    except Exception as e:
        logging.error(f"API初始化失败：{str(e)}", exc_info=True)
        raise


# -------------------------- 4. 读取和保存JSON文件 --------------------------
def read_json_file(file_path):
    """读取JSON文件，返回数据列表（兼容 UTF-8 BOM）。"""
    try:
        # 使用 utf-8-sig 以兼容带 BOM 的 UTF-8 文件
        with open(file_path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError("JSON文件内容必须是列表格式")

        logging.info(f"成功读取JSON文件：{file_path}，共{len(data)}条数据")
        return data
    except Exception as e:
        logging.error(f"读取JSON文件失败：{file_path} - {str(e)}", exc_info=True)
        raise


def save_updated_json(data, output_path):
    """保存更新后的JSON数据到文件"""
    try:
        # 确保输出目录存在
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"更新后的JSON文件已保存至：{output_path}")
    except Exception as e:
        logging.error(f"保存JSON文件失败：{output_path} - {str(e)}", exc_info=True)
        raise


# -------------------------- 5. 翻译处理函数 --------------------------
def translate_item(translator, item, index, source_field, target_language):
    """翻译单个条目

    Args:
        translator: YoudaoTranslator实例
        item: JSON数据项
        index: 索引
        source_field: 需要翻译的字段名
        target_language: 目标语言（如：en, ja, zh-CHS等）
    """
    try:
        # 获取需要翻译的文本
        source_text = item.get(source_field)

        # 处理空值情况
        if not source_text or str(source_text).strip() == "":
            return index, item, False, f"{source_field}字段为空"

        # 使用有道翻译API执行翻译
        translate_result = translator.get_translation_text(
            text=source_text,
            target_lang=target_language,
            source_lang="auto"
        )

        # 验证翻译结果
        if not translate_result or translate_result.startswith("翻译出错"):
            return index, item, False, f"翻译结果为空或出错: {translate_result}"

        # 更新字段
        item["外文"] = translate_result
        item["语种"] = target_language

        # 记录翻译前后的文本（可选，用于调试）
        logging.debug(f"原文: {str(source_text)[:50]}... -> 译文: {translate_result[:50]}...")

        return index, item, True, "翻译成功"

    except Exception as e:
        logging.error(f"第{index + 1}条翻译异常: {str(e)}")
        return index, item, False, f"翻译失败: {str(e)}"


# -------------------------- 6. 多线程批量处理 --------------------------
def batch_translate(json_data, source_field, target_language, cookie=None):
    """批量翻译JSON数据

    Args:
        json_data: JSON数据列表
        source_field: 需要翻译的字段名
        target_language: 目标语言
        cookie: 可选的cookie字符串
    """
    total_count = len(json_data)
    success_count = 0
    fail_count = 0
    results = [None] * total_count  # 用于保存按原顺序的结果
    lock = threading.Lock()  # 用于线程安全的计数

    logging.info(f"开始批量翻译，共{total_count}条数据，使用{MAX_WORKERS}个线程")
    logging.info(f"翻译字段: {source_field}，目标语种: {target_language}")
    logging.info(f"使用有道翻译API进行翻译")

    # 分批次处理，避免一次性发起太多请求
    for batch_start in range(0, total_count, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_count)
        logging.info(
            f"处理批次 {batch_start // BATCH_SIZE + 1}/{(total_count + BATCH_SIZE - 1) // BATCH_SIZE}，处理 {batch_start + 1}-{batch_end} 条数据"
        )

        # 为每个批次创建新的线程池
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交所有任务
            futures = []
            for i in range(batch_start, batch_end):
                # 每个线程使用独立的翻译器实例，避免共享问题
                translator = YoudaoTranslator(cookie=cookie)
                future = executor.submit(
                    translate_item,
                    translator,
                    json_data[i],
                    i,
                    source_field,
                    target_language
                )
                futures.append(future)

            # 处理结果
            for future in as_completed(futures):
                try:
                    index, item, success, msg = future.result()
                    results[index] = item

                    with lock:
                        if success:
                            success_count += 1
                        else:
                            fail_count += 1

                    # 打印进度
                    progress = (success_count + fail_count) / total_count * 100
                    logging.info(f"进度: {progress:.1f}% - 第{index + 1}条: {msg}")

                except Exception as e:
                    logging.error(f"处理结果时出错: {str(e)}", exc_info=True)
                    with lock:
                        fail_count += 1

    logging.info(f"\n翻译完成！总计：{total_count}条，成功：{success_count}条，失败：{fail_count}条")
    return results


# -------------------------- 7. 主执行流程（已改：遍历 input 目录所有 JSON） --------------------------
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )

    try:
        # 获取用户配置（不再输入文件路径）
        config = get_user_config()

        if not config["json_files"]:
            logging.warning(f"输入目录下没有找到任何 .json 文件：{config['input_dir']}")
            exit(0)

        # 逐个文件处理
        for idx, input_file in enumerate(config["json_files"], start=1):
            file_name = os.path.basename(input_file)
            output_file = os.path.join(config["output_dir"], file_name)

            logging.info(f"\n=== [{idx}/{len(config['json_files'])}] 开始处理文件：{file_name} ===")
            logging.info(f"输入文件: {input_file}")
            logging.info(f"输出文件: {output_file}")

            # 读取原始JSON数据
            original_data = read_json_file(input_file)

            # 批量翻译（使用有道翻译API）
            updated_data = batch_translate(
                json_data=original_data,
                source_field=config["source_field"],
                target_language=config["target_language"],
                cookie=CUSTOM_COOKIE  # 如果需要自定义cookie可以在配置中设置
            )

            # 保存结果
            save_updated_json(updated_data, output_file)

            logging.info(f"=== 文件完成：{file_name} ===")

        logging.info("\n=== 全部文件处理完成 ===")

    except Exception as e:
        logging.error(f"程序执行失败：{str(e)}", exc_info=True)
