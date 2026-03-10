import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# bing_2.py 在 stdout 打印运行结果时使用的前缀
RUN_RESULT_PREFIX = "RUN_RESULT_JSON:"
# 默认 json 文件夹（可按需修改）
DEFAULT_JSON_DIR = r"D:\code_Python\Vague-Search\模糊搜索\json\output\葡萄牙语"


def list_json_files(json_dir: Path) -> List[Path]:
    """收集目录下的 json 文件，并按文件名升序排序。"""
    return sorted(
        [p for p in json_dir.iterdir() if p.is_file() and p.suffix.lower() == ".json"],
        key=lambda p: p.name.lower(),
    )


def run_bing2_once(script_path: Path, json_path: Path) -> Tuple[int, Optional[Dict]]:
    """单次运行 bing_2.py，并返回 (return_code, run_result)。"""
    cmd = [sys.executable, str(script_path), str(json_path)]
    print(f"[RUN ] {' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    parsed_from_stdout = None
    if proc.stdout is not None:
        for line in proc.stdout:
            line = line.rstrip("\n")
            print(line)
            if line.startswith(RUN_RESULT_PREFIX):
                payload = line[len(RUN_RESULT_PREFIX):].strip()
                try:
                    parsed_from_stdout = json.loads(payload)
                except Exception:
                    pass

    return_code = proc.wait()
    return return_code, parsed_from_stdout


def normalize_int(value, default: int = 0) -> int:
    """安全转 int，失败回退默认值。"""
    try:
        return int(value)
    except Exception:
        return default


def main(default_json_dir: str = "") -> int:
    """批量调度入口：逐个 json 运行 bing_2.py。"""
    parser = argparse.ArgumentParser(description="批量调度 bing_2.py 逐个运行 json")
    parser.add_argument(
        "json_dir",
        nargs="?",
        default=default_json_dir or None,
        help="json目录绝对路径（不传则使用脚本内默认目录）",
    )
    parser.add_argument("--retry-delay", type=int, default=60, help="未完成时重试等待秒数")
    parser.add_argument("--max-retries-per-json", type=int, default=50, help="每个json最大重试次数，达到后跳过到下一个")
    parser.add_argument(
        "--continue-on-manual-stop",
        action="store_true",
        help="遇到'用户手动停止'时也继续重试当前json",
    )
    args = parser.parse_args()
    if not args.json_dir:
        parser.error("请传入 json_dir，或先在 DEFAULT_JSON_DIR 中配置默认目录。")

    json_dir = Path(args.json_dir).expanduser().resolve()
    script_path = Path(__file__).with_name("bing_2.py").resolve()
    stop_on_manual = not args.continue_on_manual_stop

    if not json_dir.exists() or not json_dir.is_dir():
        print(f"[ERR ] json目录不存在或不是目录: {json_dir}")
        return 1
    if not script_path.exists() or not script_path.is_file():
        print(f"[ERR ] bing脚本不存在: {script_path}")
        return 1

    json_files = list_json_files(json_dir)
    if not json_files:
        print(f"[ERR ] 目录下没有json文件: {json_dir}")
        return 1

    print(f"[INFO] 共发现 {len(json_files)} 个json文件")
    index = 0

    while index < len(json_files):
        current_json = json_files[index]
        print(f"[INFO] 开始处理 ({index + 1}/{len(json_files)}): {current_json}")

        retries = 0
        while True:
            retries += 1
            return_code, run_result = run_bing2_once(script_path, current_json)

            if run_result is None:
                print(f"[WARN] 未收到运行结果，returncode={return_code}")
                done = 0
                total = 1
                exit_reason = "无结果"
            else:
                done = normalize_int(run_result.get("done"), 0)
                total = normalize_int(run_result.get("total"), 0)
                exit_reason = str(run_result.get("exit_reason") or "")
                print(f"[INFO] 本次结果: exit_reason={exit_reason}, done={done}, total={total}")

            if exit_reason == "用户手动停止" and stop_on_manual:
                print("[STOP] 检测到手动停止，调度器结束。")
                return 130

            if total > 0 and done >= total:
                print(f"[DONE] 当前json完成: {current_json.name}")
                index += 1
                break

            if args.max_retries_per_json > 0 and retries >= args.max_retries_per_json:
                print(f"[SKIP] 当前json达到最大重试次数({args.max_retries_per_json})，跳过: {current_json.name}")
                index += 1
                break

            print(f"[RETRY] 当前json未完成，{args.retry_delay}秒后重试: {current_json.name}")
            time.sleep(max(args.retry_delay, 0))

    print("[ALL ] 全部json处理完成。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(DEFAULT_JSON_DIR))
