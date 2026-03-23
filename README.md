# 🐟 鱼油品类电商竞品分析平台

## 项目背景

持续追踪亚马逊美国站 Fish Oil / Omega-3 品类 Best Sellers 数据，辅助电商采销做选品决策和定价策略。

## 功能模块

### 选品分析
- 价格带分布（$10以下 / $10-20 / $20-30 / $30+）
- 排名 vs 评分散点图（气泡大小 = 评价数量）
- 品牌集中度饼图（前10品牌）
- 综合推荐商品列表（排名前30% + 评分≥4.0 + 评价≥500）

### 价格监控
- 单品价格历史折线图（标注降价节点）
- 单日价格波动 >10% 预警
- 促销规律识别（周/月维度）
- 市场整体均价走势

## 技术栈

| 模块 | 技术 |
|------|------|
| 数据采集 | Python + requests + BeautifulSoup |
| 定时任务 | GitHub Actions（每日 08:00 北京时间） |
| 数据存储 | GitHub 仓库内 CSV 文件 |
| 前端展示 | Streamlit + Plotly |
| 部署 | Streamlit Cloud |

## 在线演示

[点击查看]（部署后更新链接）

## 数据说明

- 数据来源：亚马逊美国站 Essential Fatty Acids Best Sellers（节点 6943343011）
- 采集频率：每天早上 8 点（北京时间）自动更新
- 每次采集约 60 条商品数据
- 字段：ASIN、商品名称、品牌、BSR 排名、价格、评分、评价数量、采集时间

## 本地运行

```bash
git clone https://github.com/lyuxin-usyd/fish-oil-tracker.git
cd fish-oil-tracker
pip install -r requirements.txt
streamlit run app.py
```

手动触发爬虫：

```bash
python scraper.py
```
