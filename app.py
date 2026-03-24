"""
鱼油品类竞品分析平台 - Streamlit 前端
"""

import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date

# ── 页面配置（必须是第一个 Streamlit 调用）──────────────────────────────────
st.set_page_config(
    layout="wide",
    page_title="鱼油品类竞品分析平台",
    page_icon="🐟",
)

# ── 导入 analysis.py 中的分析函数 ────────────────────────────────────────────
try:
    from analysis import (
        get_price_band_distribution,
        get_brand_concentration,
        get_rank_rating_data,
        get_recommended_products,
        get_price_history,
        get_all_price_trends,
        get_promotion_events,
        get_summary_stats,
    )
    ANALYSIS_IMPORTED = True
except ImportError:
    ANALYSIS_IMPORTED = False


# ── 常量 ─────────────────────────────────────────────────────────────────────
DATA_DIR = "data"
PLOTLY_TEMPLATE = "plotly_dark"
PRIMARY_COLOR = "#1f77b4"

BRANDS = [
    "Nordic Naturals",
    "Nature Made",
    "Viva Naturals",
    "Carlson",
    "Sports Research",
    "NOW Foods",
    "Kirkland Signature",
    "Solgar",
    "Garden of Life",
    "Dr. Tobias",
]


# ── Mock 数据生成 ─────────────────────────────────────────────────────────────

def _make_mock_products(seed: int = 42) -> pd.DataFrame:
    """生成50条假鱼油商品数据"""
    rng = np.random.default_rng(seed)
    n = 50

    asins = [f"B{rng.integers(10**8, 10**9):09d}" for _ in range(n)]
    brands = rng.choice(BRANDS, size=n)
    prices = np.round(rng.uniform(8, 45, size=n), 2)
    ratings = np.round(rng.uniform(3.5, 5.0, size=n), 1)
    review_counts = rng.integers(100, 50001, size=n)
    ranks = rng.integers(1, 5001, size=n)

    titles = [
        f"{brand} Omega-3 Fish Oil {rng.integers(500, 3001)}mg ({rng.integers(30, 365)} softgels)"
        for brand in brands
    ]

    df = pd.DataFrame(
        {
            "asin": asins,
            "title": titles,
            "brand": brands,
            "price": prices,
            "rating": ratings,
            "review_count": review_counts,
            "rank": ranks,
            "date": date.today().isoformat(),
        }
    )
    return df.sort_values("rank").reset_index(drop=True)


def _make_mock_price_history(asin: str, days: int = 60) -> pd.DataFrame:
    """生成单品60天价格历史"""
    rng = np.random.default_rng(abs(hash(asin)) % (2**32))
    base_price = rng.uniform(10, 40)
    dates = [date.today() - timedelta(days=i) for i in range(days, -1, -1)]
    prices = [base_price]
    for _ in range(days):
        change = rng.normal(0, 0.5)
        # 偶发大幅降价
        if rng.random() < 0.05:
            change -= rng.uniform(2, 6)
        new_price = max(5.0, round(prices[-1] + change, 2))
        prices.append(new_price)

    df = pd.DataFrame({"date": dates, "price": prices, "asin": asin})
    df["date"] = pd.to_datetime(df["date"])
    return df


def _make_mock_all_trends(products: pd.DataFrame, days: int = 60) -> pd.DataFrame:
    """生成所有商品整体价格趋势（每日均价）"""
    rng = np.random.default_rng(0)
    dates = [date.today() - timedelta(days=i) for i in range(days, -1, -1)]
    avg_prices = np.round(np.cumsum(rng.normal(0, 0.3, len(dates))) + 22.0, 2)
    return pd.DataFrame({"date": pd.to_datetime(dates), "avg_price": avg_prices})


def _make_promotion_events(history_df: pd.DataFrame) -> pd.DataFrame:
    """从价格历史中提取波动>10%的记录"""
    df = history_df.copy().sort_values("date").reset_index(drop=True)
    df["prev_price"] = df["price"].shift(1)
    df["change_pct"] = (df["price"] - df["prev_price"]) / df["prev_price"] * 100
    events = df[df["change_pct"].abs() > 10].copy()
    events["event_type"] = events["change_pct"].apply(
        lambda x: "📉 降价" if x < 0 else "📈 涨价"
    )
    events["change_pct"] = events["change_pct"].round(2)
    return events[["date", "price", "prev_price", "change_pct", "event_type"]].rename(
        columns={
            "date": "日期",
            "price": "当前价格($)",
            "prev_price": "前日价格($)",
            "change_pct": "变化幅度(%)",
            "event_type": "事件类型",
        }
    )


# ── 数据加载 ──────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_latest_data() -> pd.DataFrame:
    """加载最新一天的数据，如果没有则生成 mock 数据"""
    if not ANALYSIS_IMPORTED:
        return _make_mock_products()

    # 尝试从 data/ 目录读取最新文件
    try:
        import re as _re_loader
        files = sorted(
            [f for f in os.listdir(DATA_DIR) if _re_loader.match(r'^\d{4}-\d{2}-\d{2}\.csv$', f) and f != "all_data.csv"],
            reverse=True,
        )
        if files:
            df = pd.read_csv(os.path.join(DATA_DIR, files[0]), encoding='utf-8')
            if "date" not in df.columns and "timestamp" in df.columns:
                df["date"] = pd.to_datetime(df["timestamp"], errors="coerce").dt.date.astype(str)
            return df
    except Exception:
        pass
    return _make_mock_products()


@st.cache_data(ttl=300)
def load_all_data() -> pd.DataFrame:
    """加载 all_data.csv 历史数据，如果没有则生成 mock 数据"""
    if not ANALYSIS_IMPORTED:
        # 拼接多天 mock 数据
        frames = []
        for d in range(60, -1, -1):
            df = _make_mock_products(seed=d)
            df["date"] = (date.today() - timedelta(days=d)).isoformat()
            frames.append(df)
        return pd.concat(frames, ignore_index=True)

    try:
        path = os.path.join(DATA_DIR, "all_data.csv")
        if os.path.exists(path):
            return pd.read_csv(path, encoding='utf-8')
    except Exception:
        pass

    frames = []
    for d in range(60, -1, -1):
        df = _make_mock_products(seed=d)
        df["date"] = (date.today() - timedelta(days=d)).isoformat()
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


# ── 分析函数（优先用 analysis.py，否则用本地 mock 实现）────────────────────

def safe_get_price_band_distribution(df: pd.DataFrame) -> pd.DataFrame:
    if ANALYSIS_IMPORTED:
        try:
            return get_price_band_distribution(df)
        except Exception:
            pass
    bins = [0, 10, 20, 30, 40, 100]
    labels = ["<$10", "$10-20", "$20-30", "$30-40", ">$40"]
    df = df.copy()
    df["price_band"] = pd.cut(df["price"], bins=bins, labels=labels, right=False)
    return df.groupby("price_band", observed=True).size().reset_index(name="count")


def safe_get_brand_concentration(df: pd.DataFrame) -> pd.DataFrame:
    if ANALYSIS_IMPORTED:
        try:
            return get_brand_concentration(df)
        except Exception:
            pass
    return df.groupby("brand").size().reset_index(name="count")


def safe_get_rank_rating_data(df: pd.DataFrame) -> pd.DataFrame:
    if ANALYSIS_IMPORTED:
        try:
            return get_rank_rating_data(df)
        except Exception:
            pass
    return df[["asin", "title", "brand", "rank", "rating", "review_count", "price"]].copy()


def safe_get_recommended_products(df: pd.DataFrame) -> pd.DataFrame:
    if ANALYSIS_IMPORTED:
        try:
            return get_recommended_products(df)
        except Exception:
            pass
    # 修改推荐逻辑：排名<=200 + 评分>=4.5 + 评价数>=10000
    mask = (df["rating"] >= 4.5) & (df["review_count"] >= 10000)
    if "rank" in df.columns:
        mask = mask & (df["rank"] <= 200)
    rec = df[mask].copy()
    rec = rec.sort_values("rank", ascending=True).head(20)
    cols = ["asin", "brand", "title", "price", "rating", "review_count", "rank"]
    if "unit_price" in rec.columns:
        cols.append("unit_price")
    return rec[[c for c in cols if c in rec.columns]]


def safe_get_price_history(asin: str, all_df: pd.DataFrame) -> pd.DataFrame:
    if ANALYSIS_IMPORTED:
        try:
            result = get_price_history(all_df, asin)
            if not result.empty and "avg_price" in result.columns and "price" not in result.columns:
                result = result.rename(columns={"avg_price": "price"})
            if not result.empty:
                return result
        except Exception:
            pass
    date_col = "date" if "date" in all_df.columns else "timestamp"
    hist = all_df[all_df["asin"] == asin][[date_col, "price"]].copy()
    hist = hist.rename(columns={date_col: "date"})
    if hist.empty:
        return _make_mock_price_history(asin)
    hist["date"] = pd.to_datetime(hist["date"])
    hist["asin"] = asin
    return hist.sort_values("date").reset_index(drop=True)


def safe_get_all_price_trends(all_df: pd.DataFrame) -> pd.DataFrame:
    if ANALYSIS_IMPORTED:
        try:
            return get_all_price_trends(all_df)
        except Exception:
            pass
    all_df = all_df.copy()
    all_df["date"] = pd.to_datetime(all_df["date"])
    return all_df.groupby("date")["price"].mean().reset_index(name="avg_price")


def safe_get_promotion_events(history_df: pd.DataFrame) -> pd.DataFrame:
    return _make_promotion_events(history_df)


def safe_get_summary_stats(df: pd.DataFrame) -> dict:
    if ANALYSIS_IMPORTED:
        try:
            return get_summary_stats(df)
        except Exception:
            pass
    return {
        "total_products": len(df),
        "avg_price": round(df["price"].mean(), 2),
        "avg_rating": round(df["rating"].mean(), 2),
    }


# ── 图表绘制函数 ──────────────────────────────────────────────────────────────

def chart_price_band(df: pd.DataFrame) -> go.Figure:
    band_df = safe_get_price_band_distribution(df)
    y_col = "product_count" if "product_count" in band_df.columns else (band_df.columns[1] if len(band_df.columns) > 1 else band_df.columns[0])
    x_col = "price_range" if "price_range" in band_df.columns else band_df.columns[0]
    fig = px.bar(
        band_df,
        x=x_col,
        y=y_col,
        title="价格带分布",
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=[PRIMARY_COLOR],
        labels={x_col: "价格区间", y_col: "商品数量"},
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        title_font_size=15,
        margin=dict(t=40, b=10, l=10, r=10),
    )
    return fig


def chart_brand_influence(df: pd.DataFrame) -> go.Figure:
    """品牌BSR影响力 = Σ(1/rank)，rank越小分越高，按影响力降序"""
    tmp = df.dropna(subset=["brand", "rank"]).copy()
    tmp = tmp[tmp["rank"] > 0]

    brand_influence = (
        tmp.groupby("brand")
        .agg(influence=("rank", lambda x: (1.0 / x).sum()), count=("rank", "count"))
        .reset_index()
        .sort_values("influence", ascending=True)  # ascending=True → plotly horizontal bar shows highest at top
    )

    fig = px.bar(
        brand_influence,
        x="influence",
        y="brand",
        orientation="h",
        title="品牌BSR影响力排行（Σ 1/rank）",
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=["#1f77b4"],
        text=brand_influence["count"].apply(lambda x: f"{int(x)}款"),
        labels={"influence": "BSR影响力得分", "brand": "品牌"},
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title_font_size=15,
        margin=dict(t=40, b=10, l=10, r=80),
        showlegend=False,
        height=max(300, len(brand_influence) * 28),
    )
    return fig


def chart_price_rank(df: pd.DataFrame) -> go.Figure:
    """单粒价排行：y轴用商品名(前25字符)，每ASIN唯一，按单粒价升序，前20条"""
    import re as _re

    def _extract_count(t):
        if not t:
            return None
        m = _re.search(r'(\d+)\s*(?:soft\s?gels?|softgels?|capsules?|caps?|tablets?|count|ct\.?)', str(t), _re.IGNORECASE)
        if m:
            n = int(m.group(1))
            return n if 20 <= n <= 1000 else None
        return None

    tmp = df.dropna(subset=["price", "brand"]).copy()
    if "unit_price" not in tmp.columns or tmp["unit_price"].isna().all():
        tmp["unit_price"] = tmp.apply(
            lambda r: (r["price"] / c) if (c := _extract_count(r.get("title"))) else None,
            axis=1,
        )
    tmp["sort_price"] = tmp["unit_price"].fillna(tmp["price"] / 100)
    # 每个品牌只保留单粒价最低的一款
    tmp = tmp.sort_values("sort_price").drop_duplicates(subset=["brand"], keep="first")
    tmp = tmp.head(20)
    tmp["display_price"] = tmp.apply(
        lambda r: f"¢{r['unit_price']*100:.1f}/粒" if pd.notna(r.get("unit_price")) else f"${r['price']:.2f}",
        axis=1,
    )
    tmp["label"] = tmp["brand"].apply(lambda x: str(x)[:20])

    fig = px.bar(
        tmp, x="sort_price", y="label", orientation="h",
        title="单粒价排行（低→高，前20）",
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=[PRIMARY_COLOR],
        text=tmp["display_price"],
        labels={"sort_price": "单粒价（$）", "label": ""},
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        title_font_size=15, margin=dict(t=40, b=10, l=10, r=80),
        height=max(300, len(tmp) * 30), yaxis=dict(tickfont=dict(size=11)),
    )
    return fig


def chart_rating_rank(df: pd.DataFrame) -> go.Figure:
    """评分排行：评分降序，同分按评价数降序，显示评分+评价数"""
    tmp = (df.dropna(subset=["rating", "review_count"])
             .sort_values(["rating", "review_count"], ascending=[False, False])
             .drop_duplicates(subset=["asin"] if "asin" in df.columns else ["title"])
             .head(20).copy())
    tmp["label"] = tmp["brand"].fillna("?").apply(lambda x: x[:16])
    tmp["text_label"] = tmp["rating"].apply(lambda x: f"{x:.1f}") + "  (" + tmp["review_count"].apply(lambda x: f"{int(x):,}评") + ")"

    fig = px.bar(
        tmp, x="rating", y="label", orientation="h",
        title="评分排行（同分按评价数排）",
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=["#2ecc71"],
        text=tmp["text_label"],
        labels={"rating": "评分", "label": ""},
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        title_font_size=15, margin=dict(t=40, b=10, l=10, r=100),
        height=max(300, len(tmp) * 30), xaxis=dict(range=[4.0, 5.3]),
        yaxis=dict(tickfont=dict(size=11)),
    )
    return fig


def chart_price_line(history_df: pd.DataFrame, asin: str, title_name: str) -> go.Figure:
    history_df = history_df.sort_values("date").copy()
    history_df["prev_price"] = history_df["price"].shift(1)
    history_df["drop"] = (
        (history_df["price"] - history_df["prev_price"]) / history_df["prev_price"] * 100 < -10
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=history_df["date"],
            y=history_df["price"],
            mode="lines",
            name="价格",
            line=dict(color=PRIMARY_COLOR, width=2),
        )
    )

    # 标注降价节点（红点）
    drops = history_df[history_df["drop"] == True]
    if not drops.empty:
        fig.add_trace(
            go.Scatter(
                x=drops["date"],
                y=drops["price"],
                mode="markers",
                name="降价节点",
                marker=dict(color="#e74c3c", size=10, symbol="circle"),
            )
        )

    fig.update_layout(
        title=f"价格走势：{title_name[:40]}",
        template=PLOTLY_TEMPLATE,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        title_font_size=15,
        margin=dict(t=40, b=10, l=10, r=10),
        xaxis_title="日期",
        yaxis_title="价格($)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def chart_all_trends(trend_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=trend_df["date"],
            y=trend_df["avg_price"],
            mode="lines+markers",
            name="全品类均价",
            line=dict(color="#2ecc71", width=2),
            marker=dict(size=4),
        )
    )
    fig.update_layout(
        title="全品类整体价格走势（日均价）",
        template=PLOTLY_TEMPLATE,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        title_font_size=15,
        margin=dict(t=40, b=10, l=10, r=10),
        xaxis_title="日期",
        yaxis_title="均价($)",
    )
    return fig


# ── 侧边栏 ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚙️ 控制面板")
    st.markdown("---")

    module = st.selectbox(
        "📊 选择模块",
        options=["选品分析", "价格监控"],
        index=0,
    )

    st.markdown("---")
    st.markdown("### 📅 日期筛选")

    # 先加载数据检查有几个不同日期（用 all_data 或 latest_data 的 date 列）
    _check_df = load_latest_data()
    _unique_dates = []
    if "date" in _check_df.columns:
        _unique_dates = sorted(_check_df["date"].dropna().unique().tolist())

    if len(_unique_dates) <= 1:
        _date_str = _unique_dates[0] if _unique_dates else date.today().isoformat()
        st.info(f"📅 当前仅有1天数据（{_date_str}），日期筛选在积累多日数据后生效")
        selected_date = date.today()
    else:
        selected_date = st.date_input(
            "分析日期",
            value=date.today(),
            max_value=date.today(),
        )

    st.markdown("---")
    st.markdown("### 💲 价格区间筛选")
    price_range = st.slider(
        "价格范围($)",
        min_value=0,
        max_value=100,
        value=(0, 100),
        step=1,
    )

    st.markdown("---")
    st.markdown("### 🏷️ 品牌筛选")
    _raw_brands = latest_df["brand"].dropna().unique().tolist() if "brand" in latest_df.columns else []
    _all_brands = sorted([b for b in _raw_brands if b and str(b) not in ("nan", "None", "")])
    selected_brands = st.multiselect("选择品牌（不选=全部）", options=_all_brands, default=[])

    st.markdown("---")
    if ANALYSIS_IMPORTED:
        st.success("✅ analysis.py 已加载")
    else:
        st.warning("⚠️ 使用 Mock 数据演示\n（未找到 analysis.py）")


# ── 页头 ──────────────────────────────────────────────────────────────────────

st.markdown(
    """
    <h1 style='text-align:center; color:#1f77b4; margin-bottom:0;'>
        🐟 鱼油品类竞品分析平台
    </h1>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    f"<p style='text-align:center; color:#888; font-size:13px;'>数据更新时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>",
    unsafe_allow_html=True,
)
st.markdown("---")

# ── 加载并筛选数据 ────────────────────────────────────────────────────────────

latest_df = load_latest_data()
all_df = load_all_data()

# 价格筛选
filtered_df = latest_df[
    (latest_df["price"] >= price_range[0]) & (latest_df["price"] <= price_range[1])
].copy()

# 品牌筛选
if selected_brands:
    filtered_df = filtered_df[filtered_df["brand"].isin(selected_brands)].copy()

if filtered_df.empty:
    st.warning("当前价格区间内没有商品数据，请调整筛选条件。")
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# 模块 1：选品分析
# ══════════════════════════════════════════════════════════════════════════════

if module == "选品分析":

    # ── 第一行：指标卡片 ────────────────────────────────────────────────────
    stats = safe_get_summary_stats(filtered_df)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            label="📦 在追踪商品数",
            value=f"{stats['total_products']} 款",
            delta=f"筛选后（原 {len(latest_df)} 款）" if len(filtered_df) < len(latest_df) else None,
        )
    with col2:
        st.metric(
            label="💲 平均价格",
            value=f"${stats['avg_price']:.2f}",
        )
    with col3:
        st.metric(
            label="⭐ 平均评分",
            value=f"{stats['avg_rating']:.2f}",
        )

    st.markdown("---")

    # ── 第二行：价格带分布 + 品牌BSR影响力 ─────────────────────────────────
    col_left, col_right = st.columns(2)
    with col_left:
        st.plotly_chart(
            chart_price_band(filtered_df),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with col_right:
        st.plotly_chart(
            chart_brand_influence(filtered_df),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    st.markdown("---")

    # ── 第三行：售价排行 + 评分排行 ─────────────────────────────────────────
    col_price, col_rating = st.columns(2)
    with col_price:
        st.plotly_chart(
            chart_price_rank(filtered_df),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with col_rating:
        st.plotly_chart(
            chart_rating_rank(filtered_df),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    st.markdown("---")

    # ── 第四行：推荐商品列表 ────────────────────────────────────────────────
    st.subheader("🏆 推荐商品列表（高性价比）")
    st.caption("筛选条件：排名 ≤ 200 · 评分 ≥ 4.5 · 评价数 ≥ 10,000 · 按排名升序")

    rec_df = safe_get_recommended_products(filtered_df)

    if rec_df.empty:
        st.info("当前筛选范围内暂无符合推荐条件的商品。")
    else:
        # 如果有 unit_price 列，转换为分并加入展示
        display_cols = {
            "asin": "ASIN",
            "brand": "品牌",
            "title": "商品名称",
            "price": "价格($)",
            "rating": "评分",
            "review_count": "评价数",
            "rank": "BSR排名↑（越小越好）",
        }
        show_df = rec_df[[c for c in display_cols if c in rec_df.columns]].rename(
            columns=display_cols
        )
        # 加入单粒价列（如果存在）
        if "unit_price" in rec_df.columns:
            show_df["单粒价(¢)"] = (rec_df["unit_price"].values * 100).round(2)

        st.dataframe(
            show_df,
            use_container_width=True,
            height=350,
        )


# ══════════════════════════════════════════════════════════════════════════════
# 模块 2：价格监控
# ══════════════════════════════════════════════════════════════════════════════

elif module == "价格监控":

    # ── 第一行：商品选择 ────────────────────────────────────────────────────
    st.subheader("🔍 商品价格监控")

    product_options = dict(zip(filtered_df["title"], filtered_df["asin"]))
    if not product_options:
        st.warning("无可选商品，请调整筛选条件。")
        st.stop()

    selected_title = st.selectbox(
        "选择商品",
        options=list(product_options.keys()),
        format_func=lambda x: x[:80] + "..." if len(x) > 80 else x,
    )
    selected_asin = product_options[selected_title]

    # 选中商品简要信息
    selected_row = filtered_df[filtered_df["asin"] == selected_asin].iloc[0]
    info_col1, info_col2, info_col3, info_col4 = st.columns(4)
    info_col1.metric("当前价格", f"${selected_row['price']:.2f}")
    info_col2.metric("评分", f"{selected_row['rating']:.1f} ⭐")
    info_col3.metric("评价数", f"{int(selected_row['review_count']):,}")
    info_col4.metric("品牌", selected_row["brand"])

    st.markdown("---")

    # ── 第二行：单品价格折线图 ──────────────────────────────────────────────
    history_df = safe_get_price_history(selected_asin, all_df)

    st.plotly_chart(
        chart_price_line(history_df, selected_asin, selected_title),
        use_container_width=True,
        config={"displayModeBar": False},
    )

    st.markdown("---")

    # ── 第三行：全品类价格走势 ──────────────────────────────────────────────
    trend_df = safe_get_all_price_trends(all_df)

    st.plotly_chart(
        chart_all_trends(trend_df),
        use_container_width=True,
        config={"displayModeBar": False},
    )

    st.markdown("---")

    # ── 第四行：促销事件记录表（需要14天以上数据才显示）──────────────────────
    days_of_data = history_df["date"].nunique() if (not history_df.empty and "date" in history_df.columns) else 0
    if days_of_data >= 14:
        st.subheader("📋 促销事件记录（价格波动 >10%）")
        promo_df = safe_get_promotion_events(history_df)
        if not promo_df.empty:
            st.dataframe(promo_df, use_container_width=True, height=300)
        else:
            st.info("该商品近期无明显价格波动事件（波动幅度均在10%以内）。")

    # 底部提示
    st.markdown("---")
    st.caption(
        f"ASIN：{selected_asin} · 数据来源：{'analysis.py' if ANALYSIS_IMPORTED else 'Mock Data'} · "
        f"分析日期：{selected_date.isoformat()}"
    )
