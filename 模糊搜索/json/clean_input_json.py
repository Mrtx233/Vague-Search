import argparse
import json
from pathlib import Path
from typing import Any


DEFAULT_INPUT_DIR = Path(__file__).resolve().parent / "input"


def is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def clean_json_file(file_path: Path) -> tuple[int, int]:
    text = file_path.read_text(encoding="utf-8-sig")
    data = json.loads(text)

    if not isinstance(data, list):
        raise ValueError("JSON 根节点不是列表")

    cleaned_data = []
    removed_count = 0

    for item in data:
        if not isinstance(item, dict):
            cleaned_data.append(item)
            continue

        if is_empty(item.get("外文")) or is_empty(item.get("中文")):
            removed_count += 1
            continue

        cleaned_data.append(item)

    if removed_count > 0:
        file_path.write_text(
            json.dumps(cleaned_data, ensure_ascii=False, indent=4),
            encoding="utf-8-sig",
        )

    return len(data), removed_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="清理 JSON 文件中外文或中文为空的字典项。"
    )
    parser.add_argument(
        "input_dir",
        nargs="?",
        default=str(DEFAULT_INPUT_DIR),
        help="要遍历的目录，默认是当前脚本目录下的 input 文件夹。",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()

    if not input_dir.exists() or not input_dir.is_dir():
        print(f"[ERROR] 目录不存在或不是文件夹: {input_dir}")
        return 1

    json_files = sorted(input_dir.rglob("*.json"))
    if not json_files:
        print(f"[INFO] 未找到 JSON 文件: {input_dir}")
        return 0

    total_files = 0
    changed_files = 0
    total_removed = 0

    for file_path in json_files:
        total_files += 1
        try:
            total_count, removed_count = clean_json_file(file_path)
            total_removed += removed_count

            if removed_count > 0:
                changed_files += 1
                print(
                    f"[CLEAN] {file_path} | 原有 {total_count} 条，删除 {removed_count} 条，保留 {total_count - removed_count} 条"
                )
            else:
                print(f"[OK] {file_path} | 无需修改")
        except Exception as exc:
            print(f"[SKIP] {file_path} | {exc}")

    print(
        f"[DONE] 共扫描 {total_files} 个文件，修改 {changed_files} 个文件，删除 {total_removed} 条记录"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
