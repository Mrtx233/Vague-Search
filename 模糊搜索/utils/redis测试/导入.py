import json
import os
from typing import Any, Dict

import redis

# Redis connection
REDIS_HOST = "10.229.32.166"
REDIS_PORT = 6379
REDIS_DB = 6

# Import directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
IMPORT_DIR = os.path.join(BASE_DIR, "redis_export")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def import_key_data(file_path: str) -> None:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    key = data["key"]
    key_type = data["type"]
    ttl = data.get("ttl", -1)
    value = data.get("value")

    print(f"[+] Importing: {key} (type: {key_type})")

    if key_type == "string":
        r.set(key, value, ex=ttl if ttl > 0 else None)
    elif key_type == "hash":
        r.delete(key)
        if value:
            r.hset(key, mapping=value)
        if ttl > 0:
            r.expire(key, ttl)
    elif key_type == "set":
        r.delete(key)
        if value:
            r.sadd(key, *value)
        if ttl > 0:
            r.expire(key, ttl)
    elif key_type == "list":
        r.delete(key)
        if value:
            r.lpush(key, *value)
        if ttl > 0:
            r.expire(key, ttl)
    elif key_type == "zset":
        r.delete(key)
        if value:
            for item in value:
                r.zadd(key, {item["member"]: item["score"]})
        if ttl > 0:
            r.expire(key, ttl)
    elif key_type == "stream":
        r.delete(key)
        if value:
            for item in value:
                r.xadd(key, item["fields"])
        if ttl > 0:
            r.expire(key, ttl)
    else:
        print(f"[!] Unsupported type: {key_type}")


def scan_json_files(directory: str) -> list:
    json_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".json"):
                json_files.append(os.path.join(root, file))
    return sorted(json_files)


def main() -> None:
    if not os.path.exists(IMPORT_DIR):
        print(f"[!] Import directory not found: {IMPORT_DIR}")
        return

    try:
        print(f"[+] Connecting {REDIS_HOST}:{REDIS_PORT} db={REDIS_DB} ...")
        print("[+] PING ->", r.ping())
    except Exception as exc:
        print("[!] Redis connect failed:", exc)
        return

    json_files = scan_json_files(IMPORT_DIR)
    if not json_files:
        print(f"[!] No JSON files found in: {IMPORT_DIR}")
        return

    print(f"[+] Found {len(json_files)} JSON files")

    success_count = 0
    error_count = 0

    for file_path in json_files:
        try:
            import_key_data(file_path)
            success_count += 1
        except Exception as exc:
            print(f"[!] Error importing {file_path}: {exc}")
            error_count += 1

    print("\n==================== SUMMARY ====================")
    print(f"Total files: {len(json_files)}")
    print(f"Success: {success_count}")
    print(f"Errors: {error_count}")
    print("[+] Done.")


if __name__ == "__main__":
    main()
