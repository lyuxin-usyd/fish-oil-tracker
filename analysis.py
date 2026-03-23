"""
亚马逊鱼油品类数据分析模块
CSV字段：rank, asin, title, brand, price, rating, review_count, timestamp

模块一：选品分析
模块二：价格监控
"""

import pandas as pd
import numpy as np


# ─────────────────────────────────────────────
# 通用辅助函数
# ─────────────────────────────────────────────

def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    """标准化字段类型，处理 NaN，解析时间戳。"""
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    # 数值字段
    for col in ["rank", "price", "rating", "review_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 时间戳
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # 字符串字段去空格
    for col in ["asin", "title", "brand"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


# ═════════════════════════════════════════════
# 模块一：选品分析函数
# ═════════════════════════════════════════════

def get_price_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    价格带分布分析。
    将商品按价格分为 4 个区间：<$10 / $10-20 / $20-30 / >$30。
    返回各区间的商品数量与平均评分。

    返回列：price_range, product_count, avg_rating
    """
    df = _prepare_df(df)
    if df.empty or "price" not in df.columns:
        return pd.DataFrame(columns=["price_range", "product_count", "avg_rating"])

    bins = [0, 10, 20, 30, float("inf")]
    labels = ["<$10", "$10-20", "$20-30", ">$30"]

    df = df.dropna(subset=["price"])
    df["price_range"] = pd.cut(df["price"], bins=bins, labels=labels, right=False)

    result = (
        df.groupby("price_range", observed=True)
        .agg(
            product_count=("asin", "count"),
            avg_rating=("rating", "mean"),
        )
        .reset_index()
    )
    result["avg_rating"] = result["avg_rating"].round(2)
    return result


def get_brand_concentration(df: pd.DataFrame) -> pd.DataFrame:
    """
    品牌集中度分析（饼图数据）。
    统计前 10 大品牌的商品数量及占比，其余归入"其他"。

    返回列：brand, product_count, percentage
    """
    df = _prepare_df(df)
    if df.empty or "brand" not in df.columns:
        return pd.DataFrame(columns=["brand", "product_count", "percentage"])

    brand_counts = (
        df["brand"]
        .replace("nan", pd.NA)
        .dropna()
        .value_counts()
        .reset_index()
    )
    brand_counts.columns = ["brand", "product_count"]

    top10 = brand_counts.head(10).copy()
    others_count = brand_counts.iloc[10:]["product_count"].sum()

    if others_count > 0:
        others_row = pd.DataFrame([{"brand": "其他", "product_count": others_count}])
        top10 = pd.concat([top10, others_row], ignore_index=True)

    total = top10["product_count"].sum()
    top10["percentage"] = (top10["product_count"] / total * 100).round(2) if total > 0 else 0.0
    return top10


def get_competition_scatter(df: pd.DataFrame) -> pd.DataFrame:
    """
    竞争热力图数据（rank vs rating 散点图）。
    同时附带品牌、价格、评价数信息，方便 Streamlit 悬浮提示。

    返回列：asin, title, brand, rank, rating, review_count, price
    """
    df = _prepare_df(df)
    needed = ["asin", "rank", "rating"]
    if df.empty or not all(c in df.columns for c in needed):
        return pd.DataFrame(columns=["asin", "title", "brand", "rank", "rating", "review_count", "price"])

    cols = [c for c in ["asin", "title", "brand", "rank", "rating", "review_count", "price"] if c in df.columns]
    result = df[cols].dropna(subset=["rank", "rating"]).copy()

    # 若有多条记录取最新一条（按 timestamp 降序）
    if "timestamp" in df.columns:
        df_with_ts = df[cols + ["timestamp"]].dropna(subset=["rank", "rating"])
        result = (
            df_with_ts.sort_values("timestamp", ascending=False)
            .drop_duplicates(subset=["asin"])
            [cols]
        )

    return result.reset_index(drop=True)


def get_recommended_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    选品推荐筛选器。
    条件：排名前 30%（rank 数值越小越靠前）+ 评分 ≥ 4.0 + 评价数 ≥ 500。
    若有多条时间戳记录，取每个 ASIN 最新一条。

    返回列：同原始 DataFrame，按 rank 升序排列
    """
    df = _prepare_df(df)
    needed = ["asin", "rank", "rating", "review_count"]
    if df.empty or not all(c in df.columns for c in needed):
        return pd.DataFrame()

    # 取每个 ASIN 最新记录
    if "timestamp" in df.columns:
        df = (
            df.sort_values("timestamp", ascending=False)
            .drop_duplicates(subset=["asin"])
        )

    df = df.dropna(subset=["rank", "rating", "review_count"])

    # 排名前 30%（rank 越小 = 排名越靠前）
    rank_threshold = df["rank"].quantile(0.30)
    mask = (
        (df["rank"] <= rank_threshold)
        & (df["rating"] >= 4.0)
        & (df["review_count"] >= 500)
    )
    result = df[mask].sort_values("rank").reset_index(drop=True)
    return result


def get_summary_metrics(df: pd.DataFrame) -> dict:
    """
    顶部数字卡片数据。
    返回：总追踪商品数、平均价格、平均评分。

    返回 dict：
        {
            "total_products": int,
            "avg_price": float,
            "avg_rating": float,
        }
    """
    empty = {"total_products": 0, "avg_price": 0.0, "avg_rating": 0.0}
    df = _prepare_df(df)
    if df.empty:
        return empty

    # 若有多条时间戳记录，取每个 ASIN 最新记录
    if "timestamp" in df.columns and "asin" in df.columns:
        df = (
            df.sort_values("timestamp", ascending=False)
            .drop_duplicates(subset=["asin"])
        )

    total = int(df["asin"].nunique()) if "asin" in df.columns else len(df)
    avg_price = round(float(df["price"].mean()), 2) if "price" in df.columns and df["price"].notna().any() else 0.0
    avg_rating = round(float(df["rating"].mean()), 2) if "rating" in df.columns and df["rating"].notna().any() else 0.0

    return {
        "total_products": total,
        "avg_price": avg_price,
        "avg_rating": avg_rating,
    }


# ═════════════════════════════════════════════
# 模块二：价格监控函数
# ═════════════════════════════════════════════

def get_price_trend(df: pd.DataFrame, asin: str) -> pd.DataFrame:
    """
    单品价格历史趋势（折线图数据）。
    指定 ASIN 的每日价格，若同一天有多条记录取均值。

    返回列：date, avg_price
    """
    df = _prepare_df(df)
    needed = ["asin", "price", "timestamp"]
    if df.empty or not all(c in df.columns for c in needed):
        return pd.DataFrame(columns=["date", "avg_price"])

    sub = df[df["asin"] == asin].dropna(subset=["price", "timestamp"]).copy()
    if sub.empty:
        return pd.DataFrame(columns=["date", "avg_price"])

    sub["date"] = sub["timestamp"].dt.date
    result = (
        sub.groupby("date")["price"]
        .mean()
        .round(2)
        .reset_index()
        .rename(columns={"price": "avg_price"})
        .sort_values("date")
    )
    return result.reset_index(drop=True)


def get_price_alerts(df: pd.DataFrame) -> pd.DataFrame:
    """
    价格波动预警。
    检测单品单日价格变动幅度 > 10% 的记录，供前端标红展示。
    变动幅度 = (当日价格 - 前一日价格) / 前一日价格 * 100

    返回列：asin, title, date, prev_price, curr_price, change_pct
    """
    df = _prepare_df(df)
    needed = ["asin", "price", "timestamp"]
    if df.empty or not all(c in df.columns for c in needed):
        return pd.DataFrame(columns=["asin", "title", "date", "prev_price", "curr_price", "change_pct"])

    df = df.dropna(subset=["price", "timestamp"]).copy()
    df["date"] = df["timestamp"].dt.date

    # 每个 ASIN 每天取均价
    daily = (
        df.groupby(["asin", "date"])
        .agg(
            avg_price=("price", "mean"),
            title=("title", "first") if "title" in df.columns else ("asin", "first"),
        )
        .reset_index()
        .sort_values(["asin", "date"])
    )

    daily["prev_price"] = daily.groupby("asin")["avg_price"].shift(1)
    daily = daily.dropna(subset=["prev_price"])
    daily["change_pct"] = ((daily["avg_price"] - daily["prev_price"]) / daily["prev_price"] * 100).round(2)

    alerts = daily[daily["change_pct"].abs() > 10].copy()
    alerts = alerts.rename(columns={"avg_price": "curr_price"})
    cols = ["asin"] + (["title"] if "title" in alerts.columns else []) + ["date", "prev_price", "curr_price", "change_pct"]
    cols = [c for c in cols if c in alerts.columns]

    return alerts[cols].sort_values("change_pct", key=abs, ascending=False).reset_index(drop=True)


def get_promotion_patterns(df: pd.DataFrame) -> dict:
    """
    促销规律分析。
    统计每周（周一=0...周日=6）和每月（1-12月）哪些时间段降价最频繁。
    降价判定：当日价格低于该 ASIN 7日滚动均价的 5%。

    返回 dict：
        {
            "weekly":  DataFrame(columns=[weekday, weekday_name, drop_count]),
            "monthly": DataFrame(columns=[month, drop_count]),
        }
    """
    empty = {
        "weekly": pd.DataFrame(columns=["weekday", "weekday_name", "drop_count"]),
        "monthly": pd.DataFrame(columns=["month", "drop_count"]),
    }
    df = _prepare_df(df)
    needed = ["asin", "price", "timestamp"]
    if df.empty or not all(c in df.columns for c in needed):
        return empty

    df = df.dropna(subset=["price", "timestamp"]).copy()
    df["date"] = df["timestamp"].dt.date

    daily = (
        df.groupby(["asin", "date"])["price"]
        .mean()
        .reset_index()
        .sort_values(["asin", "date"])
    )

    # 7日滚动均价（每个 ASIN 独立计算）
    daily["rolling_mean"] = (
        daily.groupby("asin")["price"]
        .transform(lambda x: x.rolling(7, min_periods=1).mean())
    )
    daily["is_promo"] = daily["price"] < daily["rolling_mean"] * 0.95

    promos = daily[daily["is_promo"]].copy()
    promos["date"] = pd.to_datetime(promos["date"])
    promos["weekday"] = promos["date"].dt.weekday
    promos["month"] = promos["date"].dt.month

    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    weekly = (
        promos.groupby("weekday")["is_promo"]
        .count()
        .reindex(range(7), fill_value=0)
        .reset_index()
        .rename(columns={"is_promo": "drop_count"})
    )
    weekly["weekday_name"] = weekly["weekday"].map(lambda x: weekday_names[x])

    monthly = (
        promos.groupby("month")["is_promo"]
        .count()
        .reindex(range(1, 13), fill_value=0)
        .reset_index()
        .rename(columns={"is_promo": "drop_count"})
    )

    return {"weekly": weekly, "monthly": monthly}


def get_market_price_trend(df: pd.DataFrame) -> pd.DataFrame:
    """
    市场整体价格走势。
    计算所有商品每日的平均价格，用于大盘折线图。

    返回列：date, avg_price, product_count
    """
    df = _prepare_df(df)
    needed = ["price", "timestamp"]
    if df.empty or not all(c in df.columns for c in needed):
        return pd.DataFrame(columns=["date", "avg_price", "product_count"])

    df = df.dropna(subset=["price", "timestamp"]).copy()
    df["date"] = df["timestamp"].dt.date

    result = (
        df.groupby("date")
        .agg(
            avg_price=("price", "mean"),
            product_count=("asin", "nunique") if "asin" in df.columns else ("price", "count"),
        )
        .reset_index()
        .sort_values("date")
    )
    result["avg_price"] = result["avg_price"].round(2)
    return result.reset_index(drop=True)


def get_top10_asins(df: pd.DataFrame) -> pd.DataFrame:
    """
    获取历史数据中排名最靠前的 10 个 ASIN（下拉选择器用）。
    评判依据：各 ASIN 的历史最佳（最小）rank 值。

    返回列：asin, title, best_rank
    """
    df = _prepare_df(df)
    needed = ["asin", "rank"]
    if df.empty or not all(c in df.columns for c in needed):
        return pd.DataFrame(columns=["asin", "title", "best_rank"])

    df = df.dropna(subset=["asin", "rank"])

    agg: dict = {"rank": "min"}
    if "title" in df.columns:
        agg["title"] = "first"

    result = (
        df.groupby("asin")
        .agg(agg)
        .reset_index()
        .rename(columns={"rank": "best_rank"})
        .sort_values("best_rank")
        .head(10)
        .reset_index(drop=True)
    )

    if "title" not in result.columns:
        result["title"] = result["asin"]

    return result[["asin", "title", "best_rank"]]


# ── 函数名别名（兼容 app.py 的 import 名称）─────────────────────────────────
get_price_band_distribution = get_price_distribution
get_rank_rating_data = get_competition_scatter
get_price_history = get_price_trend
get_all_price_trends = get_market_price_trend
get_promotion_events = get_promotion_patterns
get_summary_stats = get_summary_metrics
