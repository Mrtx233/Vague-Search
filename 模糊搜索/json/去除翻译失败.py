import json
from pathlib import Path


def filter_json_file(json_path: Path, target_language: str) -> tuple[int, int]:
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if not isinstance(data, list):
            print(f"[跳过] 文件不是列表结构: {json_path}")
            return 0, 0

        original_count = len(data)
        filtered_data = [
            item for item in data
            if isinstance(item, dict) and item.get("语种") == target_language
        ]

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=4)

        return original_count, len(filtered_data)

    except Exception as e:
        print(f"[错误] {json_path}: {e}")
        return 0, 0


def main():
    folder_path = input("请输入 JSON 文件夹路径: ").strip().strip('"')
    target_language = input("请输入要保留的语种值: ").strip()

    folder = Path(folder_path)
    if not folder.exists() or not folder.is_dir():
        print("文件夹路径无效")
        return

    json_files = list(folder.rglob("*.json"))
    if not json_files:
        print("没有找到 JSON 文件")
        return

    for json_file in json_files:
        before, after = filter_json_file(json_file, target_language)
        print(f"{json_file} | {before} -> {after}")

    print("全部处理完成")


if __name__ == "__main__":
    main()