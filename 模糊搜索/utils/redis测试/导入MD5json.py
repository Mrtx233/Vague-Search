import argparse
import json
import re
import sys
from pathlib import Path
from typing import List, Tuple

import redis


DEFAULT_JSON_PATH = r"D:\数据采集\0227\0219全部md5_files.json"
DEFAULT_REDIS_HOST = "10.229.32.166"
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DB = 0
DEFAULT_REDIS_KEY = "crawler:seen_md5"
DEFAULT_CHUNK_SIZE = 2000

MD5_PATTERN = re.compile(r"^[0-9a-f]{32}$")


def load_json(path: Path) -> list:
    encodings = ("utf-8-sig", "utf-8", "gbk")
    for encoding in encodings:
        try:
            with path.open("r", encoding=encoding) as f:
                data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("JSON root must be a list")
            return data
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Cannot decode file with {encodings}: {path}")


def extract_md5_values(rows: list) -> Tuple[List[str], int]:
    md5_list: List[str] = []
    invalid_count = 0

    for row in rows:
        md5_value = None
        if isinstance(row, dict):
            md5_value = row.get("md5")
        elif isinstance(row, str):
            md5_value = row

        if not isinstance(md5_value, str):
            invalid_count += 1
            continue

        md5_value = md5_value.strip().lower()
        if not MD5_PATTERN.fullmatch(md5_value):
            invalid_count += 1
            continue

        md5_list.append(md5_value)

    return md5_list, invalid_count


def chunked(items: List[str], chunk_size: int):
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import MD5 values from JSON to Redis set")
    parser.add_argument("--json-path", default=DEFAULT_JSON_PATH, help="Path to md5 JSON file")
    parser.add_argument("--redis-host", default=DEFAULT_REDIS_HOST, help="Redis host")
    parser.add_argument("--redis-port", type=int, default=DEFAULT_REDIS_PORT, help="Redis port")
    parser.add_argument("--redis-db", type=int, default=DEFAULT_REDIS_DB, help="Redis DB index")
    parser.add_argument("--redis-key", default=DEFAULT_REDIS_KEY, help="Redis Set key")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Batch size for SADD")
    parser.add_argument("--dry-run", action="store_true", help="Show stats only, do not write Redis")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    json_path = Path(args.json_path)
    if not json_path.exists():
        print(f"[ERROR] File not found: {json_path}")
        return 1

    try:
        rows = load_json(json_path)
    except Exception as e:
        print(f"[ERROR] Failed to read JSON: {e}")
        return 1

    md5_values, invalid_count = extract_md5_values(rows)
    unique_md5_values = list(dict.fromkeys(md5_values))

    print(f"[INFO] Total rows: {len(rows)}")
    print(f"[INFO] Valid md5 rows: {len(md5_values)}")
    print(f"[INFO] Unique md5 in file: {len(unique_md5_values)}")
    print(f"[INFO] Invalid rows: {invalid_count}")

    if args.dry_run:
        print("[INFO] dry-run mode: no Redis writes")
        return 0

    if not unique_md5_values:
        print("[INFO] No md5 values to write")
        return 0

    try:
        rds = redis.Redis(
            host=args.redis_host,
            port=args.redis_port,
            db=args.redis_db,
            decode_responses=True,
        )
        rds.ping()
    except Exception as e:
        print(f"[ERROR] Redis connection failed: {e}")
        return 1

    added_count = 0
    try:
        for batch in chunked(unique_md5_values, max(1, args.chunk_size)):
            added_count += int(rds.sadd(args.redis_key, *batch))
    except Exception as e:
        print(f"[ERROR] Redis write failed: {e}")
        return 1

    existed_count = len(unique_md5_values) - added_count
    print(f"[OK] Redis Key: {args.redis_key}")
    print(f"[OK] Added: {added_count}")
    print(f"[OK] Already existed: {existed_count}")
    print(f"[OK] Final set cardinality: {rds.scard(args.redis_key)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
