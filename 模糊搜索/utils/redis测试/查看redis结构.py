import argparse
from pathlib import Path
from typing import Any

# 来自 模糊搜索-google/google_1.py
REDIS_HOST = "10.229.32.166"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PREFIX = "crawler"


def scan_keys(r: Any, pattern: str, count: int = 1000):
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=count)
        for key in keys:
            yield key
        if cursor == 0:
            break

def main():
    parser = argparse.ArgumentParser(description="仅列举 Redis key 名称")
    parser.add_argument("--host", default=REDIS_HOST, help="Redis host")
    parser.add_argument("--port", type=int, default=REDIS_PORT, help="Redis port")
    parser.add_argument("--db", type=int, default=REDIS_DB, help="Redis db")
    parser.add_argument("--prefix", default=REDIS_PREFIX, help="只扫描该前缀，如 crawler")
    parser.add_argument("--output", default="", help="输出目录；会写入 key 名称文本文件")
    args = parser.parse_args()

    try:
        import redis
    except ImportError:
        print("未检测到 redis 依赖，请先安装：pip install redis")
        return

    r = redis.Redis(host=args.host, port=args.port, db=args.db, decode_responses=True)
    print(f"[连接] redis://{args.host}:{args.port}/{args.db}")
    print(f"[PING] {r.ping()}")

    pattern = f"{args.prefix}:*"
    keys = sorted(list(scan_keys(r, pattern)))
    print(f"[扫描] pattern={pattern} keys={len(keys)}")
    if not keys:
        return

    for k in keys:
        print(k)

    if args.output:
        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "redis_key_names.txt"
        out_path.write_text("\n".join(keys) + "\n", encoding="utf-8")
        print(f"[写入] {out_path}")


if __name__ == "__main__":
    main()
