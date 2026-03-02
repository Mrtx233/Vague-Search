import argparse
import json
from pathlib import Path

# 来自 模糊搜索-google/google_1.py
REDIS_HOST = "10.229.32.166"
REDIS_PORT = 6379
REDIS_DB = 0


def dump_key(r, key: str) -> str:
    if not r.exists(key):
        return f"[不存在] {key}\n"

    key_type = r.type(key)
    lines = [f"[KEY] {key}", f"[TYPE] {key_type}"]

    if key_type == "string":
        value = r.get(key)
        lines.append(f"[VALUE] {value}")

    elif key_type == "list":
        total = r.llen(key)
        lines.append(f"[LEN] {total}")
        items = r.lrange(key, 0, -1)
        for i, item in enumerate(items, 1):
            lines.append(f"{i}. {item}")

    elif key_type == "set":
        members = sorted(list(r.smembers(key)))
        lines.append(f"[CARD] {len(members)}")
        for i, member in enumerate(members, 1):
            lines.append(f"{i}. {member}")

    elif key_type == "hash":
        data = r.hgetall(key)
        lines.append(f"[LEN] {len(data)}")
        for i, field in enumerate(sorted(data.keys()), 1):
            lines.append(f"{i}. {field} = {data[field]}")

    elif key_type == "zset":
        data = r.zrange(key, 0, -1, withscores=True)
        lines.append(f"[CARD] {len(data)}")
        for i, (member, score) in enumerate(data, 1):
            lines.append(f"{i}. {member} | score={score}")

    elif key_type == "stream":
        data = r.xrange(key, "-", "+")
        lines.append(f"[LEN] {len(data)}")
        for i, (entry_id, fields) in enumerate(data, 1):
            lines.append(f"{i}. id={entry_id} fields={json.dumps(fields, ensure_ascii=False)}")

    else:
        lines.append("[提示] 暂不支持该类型的详细展示")

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="查看单个 Redis key 的详细数据")
    parser.add_argument("--key", default="", help="要查询的完整 key 名称")
    parser.add_argument("--host", default=REDIS_HOST, help="Redis host")
    parser.add_argument("--port", type=int, default=REDIS_PORT, help="Redis port")
    parser.add_argument("--db", type=int, default=REDIS_DB, help="Redis db")
    parser.add_argument("--output", default="", help="输出目录；会写入详情文本文件")
    args = parser.parse_args()

    key = (args.key or "").strip()
    if not key:
        try:
            key = input("请输入要查询的完整 key: ").strip()
        except EOFError:
            key = ""
    if not key:
        print("未提供 key。请使用 --key 传入，或在提示时输入。")
        return

    try:
        import redis
    except ImportError:
        print("未检测到 redis 依赖，请先安装：pip install redis")
        return

    r = redis.Redis(host=args.host, port=args.port, db=args.db, decode_responses=True)
    print(f"[连接] redis://{args.host}:{args.port}/{args.db}")
    print(f"[PING] {r.ping()}")

    text = dump_key(r, key)
    print(text)

    if args.output:
        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_name = key.replace(":", "_")
        out_path = out_dir / f"redis_key_detail_{safe_name}.txt"
        out_path.write_text(text, encoding="utf-8")
        print(f"[写入] {out_path}")


if __name__ == "__main__":
    main()
