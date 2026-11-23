# -*- coding: utf-8 -*-
"""沪深交易所宽基ETF全量份额采集与系列聚合。
份额单位统一为亿份；原始缓存保留万份。分类依据交易所披露的基金简称，
无法可靠识别的ETF不强行归类，并在数据状态中提示。
"""
import json, os, re, time
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
CACHE_PATH = os.path.join(DATA_DIR, 'broad_etf_cache.json')
START_DATE = '2024-06-03'
SSE_URL = 'https://query.sse.com.cn/commonQuery.do'
SZSE_URL = 'https://www.szse.cn/api/report/ShowReport/data'
HEADERS = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36',
           'Referer':'https://www.sse.com.cn/'}
GROUPS = [
 {'id':'50','name':'上证50','color':'#f08c00'},
 {'id':'300','name':'沪深300','color':'#1098ad'},
 {'id':'500','name':'中证500','color':'#7950f2'},
 {'id':'1000','name':'中证1000','color':'#40a02b'},
 {'id':'A500','name':'中证A500','color':'#d6336c'},
 {'id':'STAR','name':'科创宽基','color':'#1c7ed6'},
 {'id':'CHINEXT','name':'创业板','color':'#e03131'},
]


# 行业主题关键词：命中即排除
INDUSTRY_KEYWORDS = (
    '芯片', '半导体', '新材料', '人工智能', 'AI',
    '算力', '软件', '电池', '新能源', '金融科技',
    '创新药', '医疗', '医药', '证券', '银行',
    '消费', '酒', '食品', '军工', '机器人',
    '光伏', '稀土', '有色', '煤炭', '钢铁',
    '通信', '传媒', '云计算', '数据中心',
    '生物科技', '生物医药'
)

# 名称不完整但属于用户提供的宽基候选清单的产品
CODE_OVERRIDES = {
    '159678': '500',    # 500增强ETF博时
    '561780': '1000',   # 1000增强ETF博时
    '159685': '1000',   # 1000增强ETF天弘
    '159676': 'CHINEXT' # 创业板增强ETF富国
}


def classify(name, code=None):
    """
    七大宽基ETF分类。
    允许增强、红利、价值、成长、低波、现金流、ESG等策略产品。
    排除行业主题、跨市场双创、创业板综指及创业板200。
    """
    n = (name or '').replace(' ', '').replace('（', '(').replace('）', ')')
    code = str(code or '').zfill(6)

    if not n:
        return None

    # 1. 行业主题ETF一律排除
    if any(word in n.upper() for word in INDUSTRY_KEYWORDS):
        return None

    # 2. 排除跨市场双创产品
    if any(word in n for word in ('科创创业', '双创50', '双创ETF')):
        return None

    # 3. 科创宽基：只保留50、100、200、综指
    if any(word in n for word in (
        '科创50', '科创100', '科创200',
        '科创综指', '科创板50', '科创板100',
        '科创板200', '科创板综合'
    )):
        return 'STAR'

    # 科创成长等其他非指定宽基不纳入
    if '科创' in n:
        return None

    # 4. 创业板：仅创业板指和创业板50
    if '创业板' in n:
        if any(word in n for word in ('创业板综', '创业板200', '创业板综合')):
            return None
        if '创业板50' in n or '创业板指' in n or '创业板指数' in n:
            return 'CHINEXT'
        if '创业板ETF' in n or '创业板增强ETF' in n:
            return 'CHINEXT'
        return None

    # 5. 上证50
    if '上证50' in n:
        return '50'

    # 6. 沪深300
    if '沪深300' in n:
        return '300'

    # 7. 中证A500
    if 'A500' in n.upper():
        return 'A500'

    # 8. 中证1000
    if '中证1000' in n or '1000增强ETF' in n:
        return '1000'

    # 9. 中证500
    if '中证500' in n or '500增强ETF' in n:
        return '500'

    # 10. 名称不完整的特殊代码
    return CODE_OVERRIDES.get(code)
