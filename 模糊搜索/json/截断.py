import json
import sys
from pathlib import Path
from typing import List, Any


def chunk_list(data: List[Any], size: int) -> List[List[Any]]:
    """按固定大小切分列表。"""
    return [data[i : i + size] for i in range(0, len(data), size)]


def index_to_label(idx: int) -> str:
    """
    将 0,1,2... 转成 A,B,C... 的标签。
    这里只实现到 Z（26 个），超出则继续用 AA, AB... 以免意外报错。
    """
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if idx < len(letters):
        return letters[idx]

    # 26 进制字母编码（简易实现）
    label = ""
    n = idx
    while True:
        n, r = divmod(n, 26)
        label = letters[r] + label
        if n == 0:
            break
        n -= 1
    return label


def main() -> int:
    """
    将心理学 JSON 按每 300 个字典拆分为多个文件：
    默认输入: 当前脚本所在目录下的 `input/心理学.json`
    输出示例: 心理学A.json、心理学B.json、...
    """
    # 允许通过命令行指定输入文件路径和切分大小
    input_path_arg = sys.argv[1].strip() if len(sys.argv) > 1 and sys.argv[1].strip() else ""
    chunk_size_arg = sys.argv[2].strip() if len(sys.argv) > 2 and sys.argv[2].strip() else ""

    if input_path_arg:
        input_path = Path(input_path_arg).expanduser().resolve()
    else:
        # 默认路径：模糊搜索/json/input/心理学.json
        input_path = (Path(__file__).resolve().parent / "input" / "心理学.json").resolve()

    try:
        chunk_size = int(chunk_size_arg) if chunk_size_arg else 300
        if chunk_size <= 0:
            raise ValueError
    except ValueError:
        print(f"[ERR ] 非法的切分大小: {chunk_size_arg!r}，必须为正整数。")
        return 1

    if not input_path.exists():
        print(f"[ERR ] 输入文件不存在: {input_path}")
        return 1

    try:
        text = input_path.read_text(encoding="utf-8-sig")
        data = json.loads(text)
    except Exception as e:
        print(f"[ERR ] 读取或解析 JSON 失败: {input_path} | {e}")
        return 1

    if not isinstance(data, list):
        print(f"[ERR ] 输入 JSON 根节点不是数组: {input_path}")
        return 1

    total = len(data)
    if total == 0:
        print(f"[INFO] 输入文件为空列表，无需切分: {input_path}")
        return 0

    chunks = chunk_list(data, chunk_size)
    out_dir = input_path.parent

    print(f"[INFO] 输入文件: {input_path}")
    print(f"[INFO] 总条目数: {total}，切分大小: {chunk_size}，将生成 {len(chunks)} 个文件。")

    for idx, chunk in enumerate(chunks):
        label = index_to_label(idx)
        out_name = f"心理学{label}.json"
        out_path = out_dir / out_name

        out_path.write_text(
            json.dumps(chunk, ensure_ascii=False, indent=4),
            encoding="utf-8-sig",
        )
        print(f"[WRITE] {out_path} | 包含 {len(chunk)} 条")

    print("[DONE] 心理学 JSON 切分完成。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

