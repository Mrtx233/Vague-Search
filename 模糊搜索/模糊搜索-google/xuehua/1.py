import json

with open(r"C:\数据采集\越南语\IT_翻译后.json", 'r', encoding='utf-8-sig') as f:
    data = json.load(f)
    print([item['外文'] for item in data if '外文' in item])