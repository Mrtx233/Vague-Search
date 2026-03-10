import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Set


def is_empty(value: Any) -> bool:
    """判断值是否为空（None、空字符串或全空白）。"""
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def dedup_in_file(path: Path, seen_zh: Set[str]) -> None:
    """
    在单个 JSON 文件中根据“中文”去重：
    - 全局使用 seen_zh 记录已出现过的中文；
    - 如果当前条目的中文在 seen_zh 中已存在，则删除该条目；
    - 否则保留并加入 seen_zh。
    """
    try:
        text = path.read_text(encoding="utf-8-sig")
        data = json.loads(text)
    except Exception as e:
        print(f"[SKIP] 读取或解析失败，跳过: {path} | {e}")
        return

    if not isinstance(data, list):
        print(f"[SKIP] 根节点不是数组，跳过: {path}")
        return

    original_len = len(data)
    cleaned: List[Dict[str, Any]] = []

    for item in data:
        if not isinstance(item, dict):
            # 非字典直接保留
            cleaned.append(item)
            continue

        zh = item.get("中文")
        if is_empty(zh):
            # 中文为空的，不参与去重逻辑，直接保留
            cleaned.append(item)
            continue

        zh_key = str(zh).strip()
        if zh_key in seen_zh:
            # 已出现过的中文，丢弃
            continue

        seen_zh.add(zh_key)
        cleaned.append(item)

    removed = original_len - len(cleaned)
    if removed <= 0:
        print(f"[OK  ] 无需修改: {path}")
        return

    # 备份原文件
    backup_path = path.with_suffix(path.suffix + ".bak_zh")
    try:
        if not backup_path.exists():
            backup_path.write_text(text, encoding="utf-8-sig")
    except Exception as e:
        print(f"[WARN] 创建备份失败（继续去重）: {backup_path} | {e}")

    try:
        path.write_text(
            json.dumps(cleaned, ensure_ascii=False, indent=4),
            encoding="utf-8-sig",
        )
        print(f"[DEDUP] {path} | 原有 {original_len} 条，删除 {removed} 条重复中文，保留 {len(cleaned)} 条")
    except Exception as e:
        print(f"[ERR ] 写回失败: {path} | {e}")


def main() -> int:
    """
    遍历指定目录（默认为当前脚本所在目录下的 input 子目录）下所有 .json 文件，
    使用全局 set 记录“中文”，只保留每个中文的第一条记录，其余重复项删除。
    """
    if len(sys.argv) > 1 and sys.argv[1].strip():
        base_dir = Path(sys.argv[1]).expanduser().resolve()
    else:
        base_dir = (Path(__file__).resolve().parent / "input").resolve()

    if not base_dir.exists() or not base_dir.is_dir():
        print(f"[ERR ] 目录不存在或不是目录: {base_dir}")
        return 1

    print(f"[INFO] 开始按“中文”去重目录: {base_dir}")

    json_files = sorted(base_dir.rglob("*.json"))
    if not json_files:
        print("[INFO] 未找到任何 .json 文件")
        return 0

    seen_zh: Set[str] = set()
    for path in json_files:
        dedup_in_file(path, seen_zh)

    print("[DONE] 所有 JSON 按“中文”去重完成。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

