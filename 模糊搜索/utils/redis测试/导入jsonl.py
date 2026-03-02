import os
import json
import time
import redis

# ========== 你要改的配置 ==========
JSONL_DIR = r"D:\数据采集\data\bing\jsonl文件"

REDIS_HOST = "10.229.32.166"
REDIS_PORT = 6379
REDIS_DB = 0

REDIS_PREFIX = "crawler"   # 必须与你爬虫里的 REDIS_PREFIX 一致
RESULTS_LIST_MAXLEN = 0    # 0=不裁剪；>0 则 LTRIM 保留最近 N 条
# =================================

CODE_TO_ZH = {
    "de": "德语",
    "en": "英语",
    "es": "西班牙语",
    "fr": "法语",
    "hi": "印地语",
    "id": "印度尼西亚语",
    "it": "意大利语",
    "ja": "日语",
    "ko": "韩语",
    "nl": "荷兰语",
    "pt": "葡萄牙语",
    "ru": "俄语",
    "th": "泰语",
    "vi": "越南语",
    "zh": "中文",
}

ZH_TO_CODE = {v: k for k, v in CODE_TO_ZH.items()}

KNOWN_CODES = set(CODE_TO_ZH.keys())

def norm_lang_to_code(lang_value) -> str:
    """把 extend.language 归一为英文 code：de/en/.../unknown"""
    if lang_value is None:
        return "unknown"
    s = str(lang_value).strip()
    if not s:
        return "unknown"

    low = s.lower()

    # 常见 unknown 形态
    if low in {"unknown", "unk", "none", "null", "未知"}:
        return "unknown"

    # 已经是 code
    if low in KNOWN_CODES:
        return low

    # 中文名 -> code
    if s in ZH_TO_CODE:
        return ZH_TO_CODE[s]

    # 有些数据可能是 "德语 " / " 德语"
    s2 = s.replace(" ", "")
    if s2 in ZH_TO_CODE:
        return ZH_TO_CODE[s2]

    # 不认识就统一 unknown，避免产生杂桶
    return "unknown"

def scan_jsonl_files(folder: str):
    for root, _, files in os.walk(folder):
        for fn in files:
            if fn.lower().endswith(".jsonl"):
                yield os.path.join(root, fn)

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    print(f"[+] Redis {REDIS_HOST}:{REDIS_PORT} db={REDIS_DB} ping -> {r.ping()}")
    print(f"[+] Import from: {JSONL_DIR}")
    print(f"[+] Redis prefix: {REDIS_PREFIX}")

    files = list(scan_jsonl_files(JSONL_DIR))
    if not files:
        print("[!] No .jsonl files found.")
        return

    total_lines = 0
    total_ok = 0
    bad_json = 0
    empty_lines = 0

    sadd_seen_1 = 0
    sadd_seen_0 = 0
    sadd_kw_added = 0
    rpush_count = 0

    t0 = time.time()

    for fp in files:
        print(f"\n--- File: {fp}")
        with open(fp, "r", encoding="utf-8") as f:
            for line in f:
                total_lines += 1
                raw = line.strip()
                if not raw:
                    empty_lines += 1
                    continue

                try:
                    obj = json.loads(raw)
                except Exception:
                    bad_json += 1
                    continue

                ext = obj.get("extend") if isinstance(obj, dict) else None
                lang_val = None
                keyword = None
                if isinstance(ext, dict):
                    lang_val = ext.get("language")
                    keyword = ext.get("keyword")

                lang_code = norm_lang_to_code(lang_val)

                src_url = obj.get("srcUrl") if isinstance(obj, dict) else None

                # 统一 key 结构：按 lang_code 分桶
                k_results = f"{REDIS_PREFIX}:results:{lang_code}"
                k_seen = f"{REDIS_PREFIX}:seen_url:{lang_code}"
                k_kw_finished = f"{REDIS_PREFIX}:keyword_finished:global"

                # 1) srcUrl 去重集合
                if src_url:
                    added = r.sadd(k_seen, src_url)
                    if added == 1:
                        sadd_seen_1 += 1
                    else:
                        sadd_seen_0 += 1

                # 2) results：原样写入（不改 jsonl 字段/格式）
                r.rpush(k_results, raw)
                rpush_count += 1

                # 3) 关键词完成集合（可选但建议）
                # if keyword:
                #     sadd_kw_added += r.sadd(k_kw_finished, keyword)

                # 4) 控制 results list 长度（可选）
                if RESULTS_LIST_MAXLEN and RESULTS_LIST_MAXLEN > 0:
                    r.ltrim(k_results, -RESULTS_LIST_MAXLEN, -1)

                total_ok += 1

    dt = time.time() - t0
    print("\n==================== 汇总 ====================")
    print(f"JSONL 文件数量            : {len(files)}")
    print(f"读取行数（总计）          : {total_lines}")
    print(f"空行数量                  : {empty_lines}")
    print(f"JSON 解析失败行数         : {bad_json}")
    print(f"成功导入记录数            : {total_ok}")
    print(f"写入 results（RPUSH）条数  : {rpush_count}")
    print(f"seen_url 新增（SADD=1）    : {sadd_seen_1}")
    print(f"seen_url 已存在（SADD=0）  : {sadd_seen_0}（重复，已存在）")
    # print(f"finished_keywords 新增数   : {sadd_kw_added}")
    print(f"耗时                      : {dt:.2f} 秒")
    print("完成。")


if __name__ == "__main__":
    main()
