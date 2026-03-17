import argparse
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# google 脚本在 stdout 打印运行结果时使用的前缀
RUN_RESULT_PREFIX = "RUN_RESULT_JSON:"
# 默认 json 文件夹（可按需修改）
DEFAULT_JSON_DIR = r"D:\code_Python\Vague-Search\模糊搜索\json\output\印地语"


def list_json_files(json_dir: Path) -> List[Path]:
    """收集目录下的 json 文件，并按文件名升序排序。"""
    return sorted(
        [p for p in json_dir.iterdir() if p.is_file() and p.suffix.lower() == ".json"],
        key=lambda p: p.name.lower(),
    )


def run_google_once(script_path: Path, json_path: Path) -> Tuple[int, Optional[Dict]]:
    """单次运行 google 脚本，并返回 (return_code, run_result)。"""
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


def process_json_files(script_path: Path, json_files: List[Path], args, thread_name: str = "Thread") -> None:
    """处理 JSON 文件列表（单个线程）"""
    print(f"[{thread_name}] 处理 {len(json_files)} 个 JSON 文件...")

    for index, current_json in enumerate(json_files):
        print(f"[{thread_name}] 开始处理 ({index + 1}/{len(json_files)}): {current_json}")

        retries = 0
        while True:
            retries += 1
            return_code, run_result = run_google_once(script_path, current_json)

            if run_result is None:
                print(f"[{thread_name}] [WARN] 未收到运行结果，returncode={return_code}")
                done = 0
                total = 1
                exit_reason = "无结果"
            else:
                done = normalize_int(run_result.get("done"), 0)
                total = normalize_int(run_result.get("total"), 0)
                exit_reason = str(run_result.get("exit_reason") or "")
                print(f"[{thread_name}] [INFO] 本次结果: exit_reason={exit_reason}, done={done}, total={total}")

            if exit_reason == "用户手动停止" and not args.continue_on_manual_stop:
                print(f"[{thread_name}] [STOP] 检测到手动停止，线程结束。")
                return

            if total > 0 and done >= total:
                print(f"[{thread_name}] [DONE] 当前json完成: {current_json.name}")
                break

            if args.max_retries_per_json > 0 and retries >= args.max_retries_per_json:
                print(f"[{thread_name}] [SKIP] 当前json达到最大重试次数({args.max_retries_per_json})，跳过: {current_json.name}")
                break

            print(f"[{thread_name}] [RETRY] 当前json未完成，{args.retry_delay}秒后重试: {current_json.name}")
            time.sleep(max(args.retry_delay, 0))

    print(f"[{thread_name}] 所有 JSON 文件处理完成。")


def main(default_json_dir: str = "") -> int:
    """双线程调度入口：同时运行 google_2.py（逆序）和 google_3.py（正序）"""
    parser = argparse.ArgumentParser(description="双线程调度 google_2.py 和 google_3.py 同时运行")
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
    script_path_2 = Path(__file__).with_name("google_2.py").resolve()
    script_path_3 = Path(__file__).with_name("google_3.py").resolve()

    if not json_dir.exists() or not json_dir.is_dir():
        print(f"[ERR ] json目录不存在或不是目录: {json_dir}")
        return 1
    if not script_path_2.exists() or not script_path_2.is_file():
        print(f"[ERR ] google_2.py 脚本不存在: {script_path_2}")
        return 1
    if not script_path_3.exists() or not script_path_3.is_file():
        print(f"[ERR ] google_3.py 脚本不存在: {script_path_3}")
        return 1

    json_files = list_json_files(json_dir)
    if not json_files:
        print(f"[ERR ] 目录下没有json文件: {json_dir}")
        return 1

    print(f"[INFO] 共发现 {len(json_files)} 个json文件")

    with ThreadPoolExecutor(max_workers=2) as executor:
        reversed_files = list(reversed(json_files))
        future_2 = executor.submit(process_json_files, script_path_2, reversed_files, args, "google_2")

        print("[INFO] 延迟 60 秒后启动 google_3.py ……")
        time.sleep(60)

        future_3 = executor.submit(process_json_files, script_path_3, json_files, args, "google_3")

        for future in as_completed([future_2, future_3]):
            try:
                future.result()
            except Exception as e:
                print(f"[ERR ] 线程执行出错: {e}")

    print("[ALL ] 所有线程执行完成。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(DEFAULT_JSON_DIR))
