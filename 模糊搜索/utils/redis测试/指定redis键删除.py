import argparse

# 来自 模糊搜索-google/google_1.py
REDIS_HOST = "10.229.32.166"
REDIS_PORT = 6379
REDIS_DB = 0

DEFAULT_KEY = "crawler:keyword_finished:google:ba1c1924e4f244888d33f61c895e5d0a"


def main():
    parser = argparse.ArgumentParser(description="删除指定 Redis key")
    parser.add_argument("--host", default=REDIS_HOST, help="Redis host")
    parser.add_argument("--port", type=int, default=REDIS_PORT, help="Redis port")
    parser.add_argument("--db", type=int, default=REDIS_DB, help="Redis db")
    parser.add_argument("--key", default=DEFAULT_KEY, help="要删除的 key")
    parser.add_argument("--execute", action="store_true", help="实际删除；不传则仅预览")
    args = parser.parse_args()

    try:
        import redis
    except ImportError:
        print("未检测到 redis 依赖，请先安装：pip install redis")
        return

    r = redis.Redis(host=args.host, port=args.port, db=args.db, decode_responses=True)
    print(f"[连接] redis://{args.host}:{args.port}/{args.db}")
    print(f"[PING] {r.ping()}")
    print(f"[目标] {args.key}")

    if not r.exists(args.key):
        print("[结果] key 不存在，无需删除。")
        return

    print(f"[类型] {r.type(args.key)}")
    if not args.execute:
        print("[模式] DRY-RUN(仅预览)，未删除。")
        print("如需删除，请加参数: --execute")
        return

    deleted = r.delete(args.key)
    print(f"[结果] 已删除数量: {deleted}")


if __name__ == "__main__":
    main()
