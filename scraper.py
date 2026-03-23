"""
Amazon Fish Oil Best Sellers Scraper
目标: https://www.amazon.com/gp/bestsellers/hpc/6943343011 (Essential Fatty Acids / Fish Oil & Omega-3)
采集字段: rank, asin, title, brand, price, rating, review_count, timestamp
"""

import os
import re
import csv
import time
import random
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────
# 配置
# ──────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TODAY = datetime.now().strftime("%Y-%m-%d")
DAILY_CSV = DATA_DIR / f"{TODAY}.csv"
ALL_CSV   = DATA_DIR / "all_data.csv"

TARGET_URLS = [
    "https://www.amazon.com/gp/bestsellers/hpc/6943343011",
    "https://www.amazon.com/gp/bestsellers/hpc/6943343011?pg=2",
]

FIELDNAMES = ["rank", "asin", "title", "brand", "price", "unit_price", "count", "rating", "review_count", "timestamp"]

DELAY_MIN = 3
DELAY_MAX = 5

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 OPR/106.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# HTTP 工具
# ──────────────────────────────────────────────

def make_headers() -> dict:
    ua = random.choice(USER_AGENTS)
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "DNT": "1",
    }


def fetch_with_requests(url: str, retries: int = 3) -> str | None:
    """用 requests 获取页面 HTML，失败返回 None"""
    session = requests.Session()
    for attempt in range(1, retries + 1):
        try:
            headers = make_headers()
            logger.info(f"[requests] 第{attempt}次请求: {url}")
            resp = session.get(url, headers=headers, timeout=20, allow_redirects=True)
            if resp.status_code == 200:
                # 简单检查是否被重定向到验证页
                if "Type the characters you see in this image" in resp.text or \
                   "Enter the characters you see below" in resp.text or \
                   "Sorry, we just need to make sure you" in resp.text:
                    logger.warning("检测到 CAPTCHA，requests 方案受阻")
                    return None
                logger.info(f"[requests] 成功，页面大小: {len(resp.text)} chars")
                return resp.text
            elif resp.status_code in (403, 429, 503):
                logger.warning(f"[requests] 状态码 {resp.status_code}，等待后重试...")
                time.sleep(random.uniform(5, 10))
            else:
                logger.warning(f"[requests] 状态码 {resp.status_code}")
        except requests.RequestException as e:
            logger.error(f"[requests] 请求异常: {e}")
        if attempt < retries:
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    return None


def fetch_with_playwright(url: str) -> str | None:
    """用 playwright 无头浏览器获取页面 HTML，失败返回 None"""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
    except ImportError:
        logger.warning("playwright 未安装，跳过备选方案。安装: pip install playwright && playwright install chromium")
        return None

    logger.info(f"[playwright] 启动无头浏览器访问: {url}")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                ]
            )
            context = browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                locale="en-US",
                viewport={"width": 1280, "height": 800},
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "DNT": "1",
                }
            )
            page = context.new_page()
            # 隐藏 webdriver 特征
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            """)
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            # 等待商品列表加载
            try:
                page.wait_for_selector(
                    "div.zg-grid-general-faceout, li.zg-item-immersion, .p13n-desktop-grid",
                    timeout=10000
                )
            except PWTimeoutError:
                logger.warning("[playwright] 等待选择器超时，尝试继续解析")
            html = page.content()
            browser.close()
            logger.info(f"[playwright] 成功，页面大小: {len(html)} chars")
            return html
    except Exception as e:
        logger.error(f"[playwright] 异常: {e}")
        return None


def get_page_html(url: str) -> str | None:
    """主方案 requests，失败自动降级到 playwright"""
    html = fetch_with_requests(url)
    if html is None:
        logger.info("requests 方案失败，尝试 playwright 备选方案...")
        html = fetch_with_playwright(url)
    return html


# ──────────────────────────────────────────────
# 解析工具
# ──────────────────────────────────────────────

def parse_price(text: str | None) -> float | None:
    """从字符串提取价格，如 '$29.99' -> 29.99"""
    if not text:
        return None
    text = text.strip()
    # 处理 "$29\n99" 格式（Amazon 有时分开显示整数和小数）
    text = text.replace("\n", ".")
    match = re.search(r"[\d,]+\.?\d*", text.replace(",", ""))
    if match:
        try:
            return float(match.group())
        except ValueError:
            return None
    return None


def parse_rating(text: str | None) -> float | None:
    """从 '4.7 out of 5 stars' 提取 4.7"""
    if not text:
        return None
    match = re.search(r"([\d.]+)\s+out of\s+5", text)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    # 尝试直接提取数字
    match = re.search(r"[\d.]+", text)
    if match:
        try:
            val = float(match.group())
            if 1.0 <= val <= 5.0:
                return val
        except ValueError:
            pass
    return None


def parse_review_count(text: str | None) -> int | None:
    """从 '1,234' 或'1234 ratings' 提取整数"""
    if not text:
        return None
    # 去掉逗号再提取数字
    text = text.replace(",", "")
    match = re.search(r"\d+", text)
    if match:
        try:
            return int(match.group())
        except ValueError:
            return None
    return None


def extract_asin_from_url(url: str) -> str | None:
    """从 URL 中提取 10 位 ASIN"""
    match = re.search(r"/dp/([A-Z0-9]{10})(?:/|$|\?)", url)
    if match:
        return match.group(1)
    match = re.search(r"(?:product|ASIN)/([A-Z0-9]{10})", url)
    if match:
        return match.group(1)
    return None


def extract_count(title: str):
    """从标题提取胶囊/片数量"""
    if not title:
        return None
    m = re.search(
        r'(\d+)\s*(?:soft\s?gels?|softgels?|capsules?|caps?|tablets?|count|ct\.?|pieces?|servings?)',
        title, re.IGNORECASE
    )
    if m:
        n = int(m.group(1))
        if 20 <= n <= 1000:
            return n
    return None


FISH_OIL_KEYWORDS = [
    "fish oil", "omega-3", "omega 3", "omega3", "dha", "epa", "cod liver oil",
    "krill oil", "algae oil", "algal oil", "flaxseed oil", "fatty acid",
    "fish oil supplement",
]

def is_fish_oil_product(title: str) -> bool:
    """过滤非鱼油/omega-3商品"""
    if not title:
        return False
    t = title.lower()
    return any(kw in t for kw in FISH_OIL_KEYWORDS)


def parse_brand_from_title(title: str) -> str | None:
    """从标题解析品牌，优先匹配已知品牌列表，匹配不到返回None"""
    if not title:
        return None
    known_brands = [
        "Nordic Naturals", "Nature Made", "Nature's Bounty", "NatureWise",
        "Carlson", "Carlyle", "Viva Naturals", "Sports Research", "Bronson",
        "NOW Foods", "Doctor's Best", "Dr. Tobias", "Life Extension", "Solgar",
        "Jarrow Formulas", "Nutrigold", "Nutricost", "Garden of Life",
        "New Chapter", "Pure Encapsulations", "MegaRed", "Kirkland Signature",
        "Kirkland", "WHC", "Wiley's Finest", "OmegaBrite", "Coromega",
        "BioSchwartz", "Performance Lab", "Thorne", "THORNE", "Renew Life",
        "Naturo Sciences", "Arazo Nutrition", "Horbäach", "Icelandic",
        "Qunol", "Purity Products", "Barlean's", "Metagenics", "Oceanblue",
        "Livingood", "Live Conscious", "Amazon Basics", "MAJU", "Zhou",
        "Freshfield", "InnoSupps", "Physician's CHOICE",
    ]
    t = title.strip()
    for brand in known_brands:
        if t.lower().startswith(brand.lower()):
            return brand
    return None  # 未知品牌返回None，不瞎猜


# ──────────────────────────────────────────────
# 核心解析：从 HTML 提取商品列表
# ──────────────────────────────────────────────

def parse_items(html: str, page_offset: int = 0) -> list[dict]:
    """
    解析 Best Sellers 页面 HTML，返回商品字典列表。
    page_offset: 第二页排名从 page_offset+1 开始（第一页50条则offset=50）
    """
    soup = BeautifulSoup(html, "lxml")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    items = []

    # ── 策略1: 新版 Grid 布局 ──
    # Amazon 新版 BSR 页面使用 div.zg-grid-general-faceout
    grid_items = soup.select("div.zg-grid-general-faceout")
    if grid_items:
        logger.info(f"策略1 (grid): 找到 {len(grid_items)} 个商品块")
        for idx, block in enumerate(grid_items, start=1):
            item = _parse_grid_item(block, rank=page_offset + idx, timestamp=timestamp)
            if item:
                items.append(item)
        return items

    # ── 策略2: 旧版 list 布局 ──
    # li.zg-item-immersion
    list_items = soup.select("li.zg-item-immersion")
    if list_items:
        logger.info(f"策略2 (list): 找到 {len(list_items)} 个商品块")
        for idx, block in enumerate(list_items, start=1):
            item = _parse_list_item(block, rank=page_offset + idx, timestamp=timestamp)
            if item:
                items.append(item)
        return items

    # ── 策略3: p13n-desktop-grid ──
    p13n_items = soup.select("div[id^='p13n-asin-index']")
    if p13n_items:
        logger.info(f"策略3 (p13n): 找到 {len(p13n_items)} 个商品块")
        for idx, block in enumerate(p13n_items, start=1):
            item = _parse_p13n_item(block, rank=page_offset + idx, timestamp=timestamp)
            if item:
                items.append(item)
        return items

    # ── 策略4: 通用 data-asin 属性 ──
    asin_blocks = soup.select("[data-asin]")
    asin_blocks = [b for b in asin_blocks if b.get("data-asin", "").strip()]
    if asin_blocks:
        logger.info(f"策略4 (data-asin): 找到 {len(asin_blocks)} 个带ASIN的块")
        seen_asins = set()
        rank_counter = page_offset
        for block in asin_blocks:
            asin = block.get("data-asin", "").strip()
            if not asin or asin in seen_asins:
                continue
            seen_asins.add(asin)
            rank_counter += 1
            item = _parse_generic_item(block, asin=asin, rank=rank_counter, timestamp=timestamp)
            if item:
                items.append(item)
        return items

    logger.warning("所有解析策略均未找到商品，请检查页面 HTML 结构是否已变更")
    # Debug: 保存 HTML 供排查
    debug_path = DATA_DIR / f"debug_{datetime.now().strftime('%H%M%S')}.html"
    debug_path.write_text(html, encoding="utf-8")
    logger.info(f"已保存调试 HTML 到: {debug_path}")
    return items


def _safe_text(element) -> str:
    """安全提取文本"""
    if element is None:
        return ""
    return element.get_text(strip=True)


def _parse_grid_item(block, rank: int, timestamp: str) -> dict | None:
    """解析新版 Grid 布局的单个商品块"""
    try:
        # ASIN
        asin = block.get("data-asin", "").strip()
        if not asin:
            # 尝试从内部链接提取
            link = block.select_one("a[href*='/dp/']")
            if link:
                asin = extract_asin_from_url(link.get("href", "")) or ""

        # Title
        title_el = (
            block.select_one("div._cDEzb_p13n-sc-css-line-clamp-3_g3dy1") or
            block.select_one("div._cDEzb_p13n-sc-css-line-clamp-4_2q2cc") or
            block.select_one("span.zg-text-center-align") or
            block.select_one("a.a-link-normal span") or
            block.select_one("div[class*='p13n-sc-line-clamp']") or
            block.select_one("._cDEzb_p13n-sc-css-line-clamp-1_60kD9") or
            block.select_one("a[title]")
        )
        if title_el is None:
            # 尝试取 a 标签的 title 属性
            a_tag = block.select_one("a[href*='/dp/']")
            title = a_tag.get("title", "").strip() if a_tag else None
        else:
            title = _safe_text(title_el) or None

        # Price
        price_el = (
            block.select_one("._cDEzb_p13n-sc-price_3mJ9Z") or
            block.select_one(".p13n-sc-price") or
            block.select_one("span[class*='p13n-sc-price']") or
            block.select_one(".a-color-price") or
            block.select_one("span.a-offscreen")
        )
        price = parse_price(_safe_text(price_el))

        # Rating
        rating_el = (
            block.select_one("span.a-icon-alt") or
            block.select_one("i[class*='a-star'] span.a-icon-alt")
        )
        rating = parse_rating(_safe_text(rating_el))

        # Review count
        review_el = (
            block.select_one("span.a-size-small") or
            block.select_one("a[href*='#customerReviews'] span") or
            block.select_one("span[aria-label*='stars']")
        )
        # a-size-small 有时包含很多无关元素，尝试找包含数字的
        review_count = None
        for candidate in block.select("span.a-size-small, span[class*='a-size-small']"):
            txt = _safe_text(candidate)
            if re.search(r"\d{2,}", txt.replace(",", "")):
                review_count = parse_review_count(txt)
                if review_count and review_count > 0:
                    break

        brand = parse_brand_from_title(title) if title else None

        return {
            "rank": rank,
            "asin": asin or None,
            "title": title,
            "brand": brand,
            "price": price,
            "count": extract_count(title),
            "unit_price": round(price / extract_count(title), 4) if price and extract_count(title) else None,
            "rating": rating,
            "review_count": review_count,
            "timestamp": timestamp,
        }
    except Exception as e:
        logger.debug(f"_parse_grid_item rank={rank} 异常: {e}")
        return None


def _parse_list_item(block, rank: int, timestamp: str) -> dict | None:
    """解析旧版 list 布局的单个商品块"""
    try:
        asin = block.get("data-asin", "").strip()
        if not asin:
            link = block.select_one("a.a-link-normal[href*='/dp/']")
            if link:
                asin = extract_asin_from_url(link.get("href", "")) or ""

        title_el = (
            block.select_one("a.a-link-normal div.p13n-sc-truncated") or
            block.select_one("a.a-link-normal span") or
            block.select_one("div.a-truncate-cut")
        )
        title = _safe_text(title_el) or None

        price_el = (
            block.select_one(".p13n-sc-price") or
            block.select_one("span.a-color-price")
        )
        price = parse_price(_safe_text(price_el))

        rating_el = block.select_one("span.a-icon-alt")
        rating = parse_rating(_safe_text(rating_el))

        review_count = None
        for candidate in block.select("a[href*='customerReviews'], span.a-size-small"):
            txt = _safe_text(candidate)
            cnt = parse_review_count(txt)
            if cnt and cnt > 0:
                review_count = cnt
                break

        brand = parse_brand_from_title(title) if title else None

        return {
            "rank": rank,
            "asin": asin or None,
            "title": title,
            "brand": brand,
            "price": price,
            "count": extract_count(title),
            "unit_price": round(price / extract_count(title), 4) if price and extract_count(title) else None,
            "rating": rating,
            "review_count": review_count,
            "timestamp": timestamp,
        }
    except Exception as e:
        logger.debug(f"_parse_list_item rank={rank} 异常: {e}")
        return None


def _parse_p13n_item(block, rank: int, timestamp: str) -> dict | None:
    """解析 p13n-asin-index 块"""
    try:
        asin = ""
        # id 格式: p13n-asin-index-N
        block_id = block.get("id", "")
        # 尝试从内部链接提取 ASIN
        link = block.select_one("a[href*='/dp/']")
        if link:
            asin = extract_asin_from_url(link.get("href", "")) or ""

        title_el = block.select_one("a[title]")
        title = title_el.get("title", "").strip() if title_el else (_safe_text(block.select_one("span.a-truncate-cut")) or None)

        price_el = block.select_one("span[class*='price']") or block.select_one(".a-color-price")
        price = parse_price(_safe_text(price_el))

        rating_el = block.select_one("span.a-icon-alt")
        rating = parse_rating(_safe_text(rating_el))

        review_count = None
        for candidate in block.select("span"):
            txt = _safe_text(candidate)
            if re.search(r"^\d[\d,]+$", txt.replace(",", "").strip()):
                review_count = parse_review_count(txt)
                if review_count and review_count > 0:
                    break

        brand = parse_brand_from_title(title) if title else None

        return {
            "rank": rank,
            "asin": asin or None,
            "title": title,
            "brand": brand,
            "price": price,
            "count": extract_count(title),
            "unit_price": round(price / extract_count(title), 4) if price and extract_count(title) else None,
            "rating": rating,
            "review_count": review_count,
            "timestamp": timestamp,
        }
    except Exception as e:
        logger.debug(f"_parse_p13n_item rank={rank} 异常: {e}")
        return None


def _parse_generic_item(block, asin: str, rank: int, timestamp: str) -> dict | None:
    """通用 fallback 解析"""
    try:
        title_el = (
            block.select_one("span.a-size-medium") or
            block.select_one("a[title]") or
            block.select_one("span[class*='title']") or
            block.select_one("a.a-link-normal span")
        )
        if title_el and title_el.name == "a":
            title = title_el.get("title", "").strip() or _safe_text(title_el)
        else:
            title = _safe_text(title_el) or None

        price_el = block.select_one("span[class*='price']") or block.select_one(".a-color-price")
        price = parse_price(_safe_text(price_el))

        rating_el = block.select_one("span.a-icon-alt")
        rating = parse_rating(_safe_text(rating_el))

        review_count = None
        for candidate in block.select("span.a-size-small"):
            cnt = parse_review_count(_safe_text(candidate))
            if cnt and cnt > 0:
                review_count = cnt
                break

        brand = parse_brand_from_title(title) if title else None

        return {
            "rank": rank,
            "asin": asin or None,
            "title": title,
            "brand": brand,
            "price": price,
            "count": extract_count(title),
            "unit_price": round(price / extract_count(title), 4) if price and extract_count(title) else None,
            "rating": rating,
            "review_count": review_count,
            "timestamp": timestamp,
        }
    except Exception as e:
        logger.debug(f"_parse_generic_item rank={rank} 异常: {e}")
        return None


# ──────────────────────────────────────────────
# CSV 存储
# ──────────────────────────────────────────────

def save_to_csv(items: list[dict]):
    """保存到每日文件 + all_data.csv（追加）"""
    if not items:
        logger.warning("没有数据可保存")
        return

    # 1. 每日文件（覆盖写入）
    daily_exists = DAILY_CSV.exists()
    write_header_daily = not daily_exists
    with open(DAILY_CSV, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header_daily:
            writer.writeheader()
        writer.writerows(items)
    logger.info(f"已写入每日文件: {DAILY_CSV}  ({len(items)} 条)")

    # 2. all_data.csv（追加）
    all_exists = ALL_CSV.exists()
    with open(ALL_CSV, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not all_exists:
            writer.writeheader()
        writer.writerows(items)
    logger.info(f"已追加到总文件: {ALL_CSV}")


# ──────────────────────────────────────────────
# 主流程
# ──────────────────────────────────────────────

def run():
    logger.info("=" * 60)
    logger.info("Amazon Fish Oil Best Sellers Scraper 启动")
    logger.info(f"目标页面数: {len(TARGET_URLS)}")
    logger.info("=" * 60)

    all_items = []
    success_count = 0
    fail_count = 0
    page_offset = 0

    for page_idx, url in enumerate(TARGET_URLS, start=1):
        logger.info(f"\n[Page {page_idx}/{len(TARGET_URLS)}] {url}")

        html = get_page_html(url)
        if html is None:
            logger.error(f"无法获取页面 {url}，跳过")
            fail_count += 50  # 估计一页约50条
            continue

        page_items = parse_items(html, page_offset=page_offset)
        valid = [it for it in page_items if it is not None and is_fish_oil_product(it.get("title", ""))]
        invalid = len(page_items) - len(valid)

        logger.info(f"本页解析: 成功 {len(valid)} 条，失败 {invalid} 条")
        all_items.extend(valid)
        success_count += len(valid)
        fail_count += invalid

        # 更新 offset，下一页从当前数量之后开始
        page_offset += len(valid) if valid else 50

        # 页间延迟（最后一页不需要等）
        if page_idx < len(TARGET_URLS):
            delay = random.uniform(DELAY_MIN, DELAY_MAX)
            logger.info(f"等待 {delay:.1f} 秒后抓取下一页...")
            time.sleep(delay)

    logger.info("\n" + "=" * 60)
    logger.info(f"采集完成: 成功 {success_count} 条，失败 {fail_count} 条")
    logger.info("=" * 60)

    if all_items:
        save_to_csv(all_items)
        # 打印前5条预览
        logger.info("\n--- 数据预览（前5条）---")
        for item in all_items[:5]:
            logger.info(
                f"  #{item['rank']:>3}  ASIN={item['asin']}  "
                f"Price=${item['price']}  Rating={item['rating']}  "
                f"Reviews={item['review_count']}"
            )
            logger.info(f"        Title: {(item['title'] or '')[:80]}")
    else:
        logger.error("未采集到任何数据，请检查网络或 HTML 结构是否变更")

    return all_items


if __name__ == "__main__":
    run()
