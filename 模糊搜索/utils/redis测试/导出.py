import base64
import json
import os
import time
from collections import Counter
from typing import Any, Dict, Iterable

import redis

# Redis connection
REDIS_HOST = "10.229.32.166"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PREFIX = "crawler"

# Export directory (default: sibling folder next to this script)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXPORT_DIR = os.path.join(BASE_DIR, "redis_export")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def scan_prefix_keys(prefix: str, count: int = 1000) -> Iterable[str]:
    """
    Scan DB safely and return all keys that belong to this prefix:
    - prefix
    - prefix:*
    """
    cursor = 0
    pattern = f"{prefix}*"
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=count)
        for key in keys:
            if key == prefix or key.startswith(f"{prefix}:"):
                yield key
        if cursor == 0:
            break


def key_to_safe_name(key: str) -> str:
    return key.replace(":", "__")


def get_subfolder(key: str) -> str:
    if ":keyword_finished:" in key or key.endswith(":keyword_finished"):
        return "keyword_finished"
    elif ":results:" in key or key.endswith(":results"):
        return "results"
    elif ":seen_url:" in key or key.endswith(":seen_url"):
        return "seen_url"
    elif ":seen_md5:" in key or key.endswith(":seen_md5"):
        return "seen_md5"
    else:
        return "other"


def export_key_data(key: str) -> Dict[str, Any]:
    key_type = r.type(key)
    ttl = r.ttl(key)
    payload: Dict[str, Any] = {"type": key_type, "ttl": ttl, "value": None}

    if key_type == "string":
        payload["value"] = r.get(key)
    elif key_type == "hash":
        payload["value"] = r.hgetall(key)
    elif key_type == "set":
        payload["value"] = sorted(r.smembers(key))
    elif key_type == "list":
        payload["value"] = r.lrange(key, 0, -1)
    elif key_type == "zset":
        payload["value"] = [
            {"member": member, "score": score}
            for member, score in r.zrange(key, 0, -1, withscores=True)
        ]
    elif key_type == "stream":
        payload["value"] = [
            {"id": item_id, "fields": fields}
            for item_id, fields in r.xrange(key, min="-", max="+")
        ]
    else:
        # Fallback for other Redis types.
        raw = r.dump(key)
        if raw is None:
            payload["value"] = None
        elif isinstance(raw, bytes):
            payload["value"] = base64.b64encode(raw).decode("ascii")
            payload["value_encoding"] = "base64_rdb_dump"
        else:
            payload["value"] = str(raw)
            payload["value_encoding"] = "stringified"

    return payload


def dump_key_file(export_root: str, key: str, data: Dict[str, Any]) -> None:
    subfolder = get_subfolder(key)
    export_dir = os.path.join(export_root, subfolder)
    ensure_dir(export_dir)
    out_file = os.path.join(export_dir, f"{key_to_safe_name(key)}.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({"key": key, **data}, f, ensure_ascii=False, indent=2)


def main() -> None:
    ensure_dir(EXPORT_DIR)

    try:
        print(f"[+] Connecting {REDIS_HOST}:{REDIS_PORT} db={REDIS_DB} ...")
        print("[+] PING ->", r.ping())
    except Exception as exc:
        print("[!] Redis connect failed:", exc)
        return

    keys = sorted(set(scan_prefix_keys(REDIS_PREFIX)))
    if not keys:
        print(f"[!] No keys matched prefix: {REDIS_PREFIX}")
        return

    print(f"[+] Matched keys: {len(keys)}")
    for key in keys:
        print(f"- {key}")

    type_counter: Counter[str] = Counter()
    for key in keys:
        data = export_key_data(key)
        type_counter[data["type"]] += 1
        dump_key_file(EXPORT_DIR, key, data)

    print("\n==================== SUMMARY ====================")
    for key_type, count in sorted(type_counter.items()):
        print(f"{key_type:>8}: {count}")

    print("\n==================== OUTPUT ====================")
    print(f"[+] Per-key JSON files -> {EXPORT_DIR}")
    print("[+] Done.")


if __name__ == "__main__":
    main()
