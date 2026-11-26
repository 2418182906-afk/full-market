# -*- coding: utf-8 -*-
"""沪深交易所指定宽基ETF份额采集与系列聚合。

仅追踪固定代码白名单中的35只ETF。
份额单位统一为亿份；原始缓存保留万份。
分类完全依据ETF代码，不依赖基金简称或关键词。
"""

import json
import os
import time
import io
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
CACHE_PATH = os.path.join(DATA_DIR, 'broad_etf_cache.json')

START_DATE = '2024-06-03'

# 白名单更新后递增版本号，触发历史数据重新采集
CLASSIFICATION_VERSION = 5

SSE_URL = 'https://query.sse.com.cn/commonQuery.do'
SZSE_URL = 'https://www.szse.cn/api/report/ShowReport'

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 Chrome/120 Safari/537.36'
    ),
    'Referer': 'https://www.sse.com.cn/'
}

GROUPS = [
    {'id': '50', 'name': '上证50', 'color': '#f08c00'},
    {'id': '300', 'name': '沪深300', 'color': '#1098ad'},
    {'id': '500', 'name': '中证500', 'color': '#7950f2'},
    {'id': '1000', 'name': '中证1000', 'color': '#40a02b'},
    {'id': 'A500', 'name': '中证A500', 'color': '#d6336c'},
    {'id': 'STAR', 'name': '科创宽基', 'color': '#1c7ed6'},
    {'id': 'CHINEXT', 'name': '创业板', 'color': '#e03131'},
]

GROUP_IDS = {g['id'] for g in GROUPS}


# ============================================================
# 一、固定代码白名单：共35只
# ============================================================

CODE_LISTS = {

    # 上证50：3只
    '50': [
        '510100',
        '510050',
        '530000',
    ],

    # 沪深300：6只
    '300': [
        '510300',
        '510330',
        '159919',
        '510310',
        '515330',
        '510360',
    ],

    # 中证500：3只
    '500': [
        '510500',
        '159922',
        '512500',
    ],

    # 中证1000：5只
    '1000': [
        '512100',
        '159845',
        '560010',
        '159629',
        '159633',
    ],

    # 中证A500：8只
    'A500': [
        '563360',
        '159352',
        '159361',
        '159338',
        '512050',
        '563800',
        '159351',
        '563220',
    ],

    # 科创宽基：5只
    'STAR': [
        '588080',
        '588050',
        '588000',
        '588060',
        '588220',
    ],

    # 创业板：5只
    'CHINEXT': [
        '159915',
        '159949',
        '159952',
        '159977',
        '159682',
    ],
}


# 各系列预期数量
EXPECTED_COUNTS = {
    '50': 3,
