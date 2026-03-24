"""
fetch_history.py
从 CamelCamelCamel 爬取指定 ASIN 列表的历史价格数据，补充到 data/all_data.csv
"""

import csv
import os
import re
import time
import random
import json
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

# ── 配置 ──────────────────────────────────────────────────────────────────────
DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "all_data.csv")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

DELAY_MIN = 2.0
DELAY_MAX = 4.0

# 只拉取过去 180 天（约6个月）
HISTORY_DAYS = 180


# ── CSV 读写 ──────────────────────────────────────────────────────────────────

FIELDNAMES = ["rank", "asin", "title", "brand", "price", "rating", "review_count", "timestamp"]


def load_existing_data(path: str):
    """
    返回 (rows, existing_keys)
    rows: 原始行列表（保留写回）
    existing_keys: set of (asin, date_str)，date_str 取 timestamp 的前10字符（YYYY-MM-DD）
    """
    rows = []
    existing_keys = set()
    if not os.path.exists(path):
        return rows, existing_keys

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
            ts = row.get("timestamp", "")
            date_part = ts[:10] if ts else ""
            existing_keys.add((row["asin"], date_part))
    return rows, existing_keys


def extract_asins(rows) -> list:
    seen = set()
    asins = []
    for row in rows:
        a = row.get("asin", "").strip()
        if a and a not in seen:
            seen.add(a)
            asins.append(a)
    return asins


def get_title_for_asin(rows, asin: str) -> str:
    for row in rows:
        if row.get("asin") == asin and row.get("title"):
            return row["title"]
    return ""


def append_rows(path: str, new_rows: list):
    if not new_rows:
        return
    write_header = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)


# ── 网络请求 ──────────────────────────────────────────────────────────────────

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def fetch_page(asin: str, session: requests.Session) -> str | None:
    url = f"https://camelcamelcamel.com/product/{asin}"
    try:
        resp = session.get(url, headers=get_headers(), timeout=20)
        if resp.status_code == 200:
            return resp.text
        elif resp.status_code == 404:
            print(f"  [404] ASIN {asin} not found on CCC")
        elif resp.status_code == 429:
            print(f"  [429] Rate limited for ASIN {asin}, waiting 30s...")
            time.sleep(30)
        else:
            print(f"  [HTTP {resp.status_code}] ASIN {asin}")
    except requests.RequestException as e:
        print(f"  [ERROR] {asin}: {e}")
    return None


# ── 解析历史价格 ───────────────────────────────────────────────────────────────

# 截止时间戳（毫秒）
_cutoff_ms = (time.time() - HISTORY_DAYS * 86400) * 1000


def ms_to_date(ms: int) -> str:
    """Unix毫秒 → 'YYYY-MM-DD' 字符串"""
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def parse_highcharts_series(html: str) -> list[tuple[int, float]]:
    """
    策略1：从 Highcharts series data 里解析 [[timestamp_ms, price], ...]
    CCC 页面通常包含:
        series:[{name:"Amazon",data:[[1700000000000,29.99],[...]]},...]
    或者:
        data:[[timestamp,price],...]
    返回 [(timestamp_ms, price), ...]，只保留 cutoff 之后的数据
    """
    results = []

    # 匹配多种可能的 Highcharts series data 格式
    # 尝试找 series 数组整体
    series_pattern = re.compile(
        r'series\s*:\s*\[.*?\]',
        re.DOTALL
    )
    # 更宽泛：找所有 data:[...] 块（可能有多个 series）
    data_block_pattern = re.compile(
        r'data\s*:\s*(\[\s*\[[\d\s,.eE+\-]*?\]\s*(?:,\s*\[[\d\s,.eE+\-]*?\]\s*)*\])',
        re.DOTALL
    )

    for m in data_block_pattern.finditer(html):
        raw = m.group(1)
        # 提取所有 [number, number] 对
        pairs = re.findall(r'\[\s*(\d{10,13})\s*,\s*([\d.]+)\s*\]', raw)
        for ts_str, price_str in pairs:
            ts = int(ts_str)
            # 如果是秒级时间戳，转为毫秒
            if ts < 1e12:
                ts *= 1000
            if ts >= _cutoff_ms:
                try:
                    results.append((ts, float(price_str)))
                except ValueError:
                    pass

    return results


def parse_json_ld(html: str) -> list[tuple[int, float]]:
    """
    策略2：解析 JSON-LD 结构化数据中的 priceHistory 或 offers
    """
    results = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
            # 可能是 list
            if isinstance(data, list):
                for item in data:
                    results.extend(_extract_ld_prices(item))
            else:
                results.extend(_extract_ld_prices(data))
        except (json.JSONDecodeError, AttributeError):
            pass
    return results


def _extract_ld_prices(obj: dict) -> list[tuple[int, float]]:
    results = []
    if not isinstance(obj, dict):
        return results
    # 递归搜索 priceValidUntil / price 组合
    # 这里只做简单递归查找，实际 CCC 不太可能有这个
    for v in obj.values():
        if isinstance(v, (dict, list)):
            if isinstance(v, list):
                for item in v:
                    results.extend(_extract_ld_prices(item))
            else:
                results.extend(_extract_ld_prices(v))
    return results


def parse_inline_var(html: str) -> list[tuple[int, float]]:
    """
    策略3：查找页面中常见的 var data = [...] 或 priceData = [...] 格式
    """
    results = []
    patterns = [
        # var amazon_data = [[ts, price], ...]
        r'(?:amazon_data|third_data|new_data|price_data|priceData|history)\s*=\s*(\[\s*\[[\d\s,.eE+\-]*?\](?:\s*,\s*\[[\d\s,.eE+\-]*?\])*\s*\])',
        # 通用 var xxx = [[...]]
        r'var\s+\w+\s*=\s*(\[\s*\[[\d]{10,13}\s*,\s*[\d.]+\](?:\s*,\s*\[[\d]{10,13}\s*,\s*[\d.]+\])*\s*\])',
    ]
    for pat in patterns:
        for m in re.finditer(pat, html, re.DOTALL):
            pairs = re.findall(r'\[\s*(\d{10,13})\s*,\s*([\d.]+)\s*\]', m.group(1))
            for ts_str, price_str in pairs:
                ts = int(ts_str)
                if ts < 1e12:
                    ts *= 1000
                if ts >= _cutoff_ms:
                    try:
                        results.append((ts, float(price_str)))
                    except ValueError:
                        pass
    return results


def parse_camel_page(html: str) -> list[tuple[int, float]]:
    """
    综合三种策略，去重后返回 [(timestamp_ms, price), ...]
    """
    all_points = []
    all_points.extend(parse_highcharts_series(html))
    all_points.extend(parse_json_ld(html))
    all_points.extend(parse_inline_var(html))

    # 去重（同一天取最低价，模拟历史最低）
    by_date: dict[str, float] = {}
    for ts, price in all_points:
        d = ms_to_date(ts)
        if d not in by_date or price < by_date[d]:
            by_date[d] = price

    return [(int(datetime.strptime(d, "%Y-%m-%d")
                 .replace(tzinfo=timezone.utc).timestamp() * 1000), p)
            for d, p in sorted(by_date.items())]


# ── 主逻辑 ────────────────────────────────────────────────────────────────────

def main():
    print(f"读取现有数据：{DATA_FILE}")
    existing_rows, existing_keys = load_existing_data(DATA_FILE)
    print(f"已有记录：{len(existing_rows)} 条")

    asins = extract_asins(existing_rows)
    print(f"唯一 ASIN：{len(asins)} 个\n")

    session = requests.Session()
    session.headers.update({"Referer": "https://camelcamelcamel.com/"})

    success_count = 0
    fail_count = 0
    new_record_count = 0
    new_rows_buffer = []

    for i, asin in enumerate(asins, 1):
        print(f"[{i}/{len(asins)}] 处理 ASIN: {asin}")
        title = get_title_for_asin(existing_rows, asin)

        html = fetch_page(asin, session)
        if html is None:
            fail_count += 1
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            continue

        price_points = parse_camel_page(html)

        if not price_points:
            print(f"  未找到价格数据（可能页面结构变化或无历史数据）")
            fail_count += 1
        else:
            added = 0
            for ts_ms, price in price_points:
                date_str = ms_to_date(ts_ms)
                key = (asin, date_str)
                if key in existing_keys:
                    continue  # 跳过重复
                existing_keys.add(key)  # 防止本次批次内重复
                iso_ts = date_str + "T00:00:00Z"
                new_rows_buffer.append({
                    "rank": "",
                    "asin": asin,
                    "title": title,
                    "brand": "",
                    "price": f"{price:.2f}",
                    "rating": "",
                    "review_count": "",
                    "timestamp": iso_ts,
                })
                added += 1

            new_record_count += added
            success_count += 1
            print(f"  新增 {added} 条历史价格记录（共解析到 {len(price_points)} 个价格点）")

        # 每处理5个ASIN就批量写入一次，防止中途失败丢数据
        if len(new_rows_buffer) >= 50:
            append_rows(DATA_FILE, new_rows_buffer)
            new_rows_buffer.clear()
            print(f"  [已写入缓冲区到文件]")

        delay = random.uniform(DELAY_MIN, DELAY_MAX)
        time.sleep(delay)

    # 写入剩余缓冲区
    if new_rows_buffer:
        append_rows(DATA_FILE, new_rows_buffer)

    print("\n" + "=" * 50)
    print(f"完成！成功 {success_count} 个 ASIN，失败 {fail_count} 个，新增 {new_record_count} 条记录")
    print(f"数据文件：{DATA_FILE}")


if __name__ == "__main__":
    main()
