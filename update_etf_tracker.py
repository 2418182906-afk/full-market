#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宽基 ETF 份额追踪 & 资金底背离监测系统 (QTYX V3.6.6 网页复刻增强版)
数据源：
1. 五大宽基指数日线：腾讯财经日K接口 (sh000688, sh000852, sh000905, sh000016, sh000300)
2. 八大核心宽基ETF每日份额：上海证券交易所官方ETF规模接口 (COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L)

用法：
- 更新数据并生成独立HTML看板：
  python3 update_etf_tracker.py
- 启动本地交互Web服务（支持网页端一键同步最新上交所数据）：
  python3 update_etf_tracker.py --serve --port 8765
"""

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import requests

from broad_market import sync_broad_market, build_broad_payload

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CACHE_PATH = os.path.join(DATA_DIR, "etf_share_cache.json")
HTML_OUT_PATH = os.path.join(BASE_DIR, "etf_share_tracker.html")

START_DATE = "2024-06-03"

INDICES_META = [
    {"code": "sh000688", "name": "科创50指数", "group": "科创50", "color": "#2b6cb0"},
    {"code": "sh000852", "name": "中证1000指数", "group": "中证1000", "color": "#82c91e"},
    {"code": "sh000905", "name": "中证500指数", "group": "中证500", "color": "#495057"},
    {"code": "sh000016", "name": "上证50指数", "group": "上证50", "color": "#fd7e14"},
    {"code": "sh000300", "name": "沪深300指数", "group": "沪深300", "color": "#15aabf"},
]

ETFS_META = [
    {
        "code": "510300",
        "name": "沪深300华泰",
        "full_name": "华泰柏瑞沪深300ETF",
        "group": "沪深300",
        "index_code": "sh000300",
        "index_name": "沪深300指数",
        "color": "#e6b800",
        "single_thresh": 20.0,
        "five_day_thresh": 50.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "510330",
        "name": "沪深300华夏",
        "full_name": "华夏沪深300ETF",
        "group": "沪深300",
        "index_code": "sh000300",
        "index_name": "沪深300指数",
        "color": "#e64980",
        "single_thresh": 14.0,
        "five_day_thresh": 35.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "510500",
        "name": "中证500南方",
        "full_name": "南方中证500ETF",
        "group": "中证500",
        "index_code": "sh000905",
        "index_name": "中证500指数",
        "color": "#7950f2",
        "single_thresh": 10.0,
        "five_day_thresh": 25.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "512500",
        "name": "中证500华夏",
        "full_name": "华夏中证500ETF",
        "group": "中证500",
        "index_code": "sh000905",
        "index_name": "中证500指数",
        "color": "#20c997",
        "single_thresh": 4.0,
        "five_day_thresh": 10.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "512100",
        "name": "中证1000南方",
        "full_name": "南方中证1000ETF",
        "group": "中证1000",
        "index_code": "sh000852",
        "index_name": "中证1000指数",
        "color": "#4c6ef5",
        "single_thresh": 7.0,
        "five_day_thresh": 30.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "560010",
        "name": "中证1000广发",
        "full_name": "广发中证1000ETF",
        "group": "中证1000",
        "index_code": "sh000852",
        "index_name": "中证1000指数",
        "color": "#94d82d",
        "single_thresh": 5.0,
        "five_day_thresh": 10.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "510050",
        "name": "上证50华夏",
        "full_name": "华夏上证50ETF",
        "group": "上证50",
        "index_code": "sh000016",
        "index_name": "上证50指数",
        "color": "#495057",
        "single_thresh": 15.0,
        "five_day_thresh": 35.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "588000",
        "name": "科创50华夏",
        "full_name": "华夏上证科创板50ETF",
        "group": "科创50",
        "index_code": "sh000688",
        "index_name": "科创50指数",
        "color": "#f76707",
        "single_thresh": 6.0,
        "five_day_thresh": 30.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
]


def sync_data(force_full: bool = False) -> dict:
    """增量同步五大宽基指数日线与上交所8只宽基ETF份额数据"""
    os.makedirs(DATA_DIR, exist_ok=True)
    cache = {}
    if os.path.exists(CACHE_PATH) and not force_full:
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                cache = json.load(f)
        except Exception:
            cache = {}

    etf_by_date = cache.get("etf_by_date", {})
    indices_data = cache.get("indices_data", {})

    session = requests.Session()
    all_dates_set = set()

    for item in INDICES_META:
        code = item["code"]
        url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},day,{START_DATE},2029-12-31,1200,"
        resp = session.get(url, timeout=15).json()
        days = resp.get("data", {}).get(code, {}).get("day", [])
        series = indices_data.get(code, {})
        for d in days:
            if d[0] >= START_DATE:
                series[d[0]] = round(float(d[2]), 2)
                all_dates_set.add(d[0])
        indices_data[code] = series

    all_trading_dates = sorted(list(all_dates_set))
    target_codes = {e["code"] for e in ETFS_META}

    # Check which dates need fetching from SSE (always re-check the latest 3 trading days in case today's data just arrived)
    recent_recheck = set(all_trading_dates[-3:]) if all_trading_dates else set()
    missing_dates = [
        d for d in all_trading_dates if (d not in etf_by_date) or (len(etf_by_date.get(d, {})) < len(target_codes)) or (d in recent_recheck)
    ]

    sse_url = "https://query.sse.com.cn/commonQuery.do"
    headers = {"Referer": "https://www.sse.com.cn/", "User-Agent": "Mozilla/5.0"}

    def fetch_sse_date(d: str):
        params = {
            "isPagination": "true",
            "pageHelp.pageSize": "10000",
            "pageHelp.pageNo": "1",
            "sqlId": "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L",
            "STAT_DATE": d,
        }
        for attempt in range(3):
            try:
                r = requests.get(sse_url, params=params, headers=headers, timeout=12)
                js = r.json()
                out = {}
                for x in js.get("result", []):
                    sc = x.get("SEC_CODE")
                    if sc in target_codes:
                        out[sc] = round(float(x["TOT_VOL"]) / 10000.0, 4)
                return d, out
            except Exception:
                time.sleep(0.25 * (attempt + 1))
        return d, {}

    if missing_dates:
        print(f"[Sync] 正在从上交所同步 {len(missing_dates)} 个交易日的ETF规模份额数据...")
        with ThreadPoolExecutor(max_workers=16) as ex:
            for d, res in ex.map(fetch_sse_date, missing_dates):
                if res:
                    etf_by_date[d] = res

    # Keep only dates that have both index and ETF share data
    valid_dates = sorted([d for d in etf_by_date.keys() if etf_by_date[d]])

    broad_cache = sync_broad_market(force_full=force_full)
    broad_payload = build_broad_payload(broad_cache)

    new_cache = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "start_date": valid_dates[0] if valid_dates else START_DATE,
        "end_date": valid_dates[-1] if valid_dates else "",
        "total_days": len(valid_dates),
        "indices_meta": INDICES_META,
        "etfs_meta": ETFS_META,
        "dates": valid_dates,
        "indices_data": indices_data,
        "etf_by_date": {d: etf_by_date[d] for d in valid_dates},
        "broad_market": broad_payload,
    }

    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(new_cache, f, ensure_ascii=False, indent=2)

    print(
        f"[Sync] 数据同步完成: {new_cache['start_date']} ~ {new_cache['end_date']} "
        f"(共 {new_cache['total_days']} 个交易日)"
    )
    return new_cache


def build_frontend_payload(cache: dict) -> dict:
    """将缓存数据整理为前端高密度列式JSON结构"""
    dates = cache["dates"]
    indices_series = {}
    for idx_meta in INDICES_META:
        c = idx_meta["code"]
        raw_map = cache["indices_data"].get(c, {})
        vals = []
        last_val = None
        for d in dates:
            v = raw_map.get(d, last_val)
            if v is not None:
                last_val = v
            vals.append(v)
        indices_series[c] = vals

    etf_series = {}
    for etf_meta in ETFS_META:
        c = etf_meta["code"]
        vals = []
        last_val = None
        for d in dates:
            day_map = cache["etf_by_date"].get(d, {})
            v = day_map.get(c, last_val)
            if v is not None:
                last_val = v
            vals.append(round(v, 4) if v is not None else None)
        etf_series[c] = vals

    return {
        "updated_at": cache["updated_at"],
        "start_date": cache["start_date"],
        "end_date": cache["end_date"],
        "total_days": cache["total_days"],
        "dates": dates,
        "indices_meta": INDICES_META,
        "etfs_meta": ETFS_META,
        "indices_series": indices_series,
        "etf_series": etf_series,
        "broad_market": cache.get("broad_market", {}),
    }


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>宽基ETF份额追踪 · 三大板块监测市场底 (QTYX-V3.6.6 Web版)</title>
  <script src="./echarts.min.js"></script>
  <script>
    if (typeof window.echarts === 'undefined') {
      document.write('<script src="https://registry.npmmirror.com/echarts/5.5.1/files/dist/echarts.min.js"><\/script>');
    }
  </script>
  <style>
    :root {
      --bg-page: #f4f6fa;
      --bg-card: #ffffff;
      --bg-subtle: #f8fafd;
      --bg-hover: #eef3fb;
      --text-primary: #141b2d;
      --text-secondary: #4a5568;
      --text-muted: #7b879c;
      --border-color: #e2e8f0;
      --border-strong: #cbd5e1;
      --accent: #1d4ed8;
      --accent-soft: rgba(29, 78, 216, 0.08);
      --up-red: #dc2626;
      --up-red-bg: rgba(220, 38, 38, 0.09);
      --down-green: #16a34a;
      --down-green-bg: rgba(22, 163, 74, 0.09);
      --warn-amber: #d97706;
      --warn-amber-bg: rgba(217, 119, 6, 0.11);
      --purple-fire: #7c3aed;
      --purple-fire-bg: rgba(124, 58, 237, 0.10);
      --shadow-sm: 0 1px 3px rgba(15, 23, 42, 0.05), 0 1px 2px rgba(15, 23, 42, 0.03);
      --shadow-md: 0 8px 24px rgba(15, 23, 42, 0.06);
      --radius-lg: 14px;
      --radius-md: 10px;
      --font-sans: "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", -apple-system, BlinkMacSystemFont, sans-serif;
      --font-mono: "JetBrains Mono", "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
    }

    [data-theme="dark"] {
      --bg-page: #0b1120;
      --bg-card: #111827;
      --bg-subtle: #1e293b;
      --bg-hover: #263449;
      --text-primary: #f1f5f9;
      --text-secondary: #cbd5e1;
      --text-muted: #94a3b8;
      --border-color: #1f2937;
      --border-strong: #334155;
      --accent: #3b82f6;
      --accent-soft: rgba(59, 130, 246, 0.15);
      --up-red: #f87171;
      --up-red-bg: rgba(248, 113, 113, 0.14);
      --down-green: #4ade80;
      --down-green-bg: rgba(74, 222, 128, 0.14);
      --warn-amber: #fbbf24;
      --warn-amber-bg: rgba(251, 191, 36, 0.14);
      --purple-fire: #a78bfa;
      --purple-fire-bg: rgba(167, 139, 250, 0.15);
      --shadow-sm: 0 2px 6px rgba(0, 0, 0, 0.3);
      --shadow-md: 0 10px 30px rgba(0, 0, 0, 0.45);
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: var(--font-sans);
      background-color: var(--bg-page);
      color: var(--text-primary);
      line-height: 1.5;
      padding-bottom: 48px;
      transition: background-color 0.2s ease, color 0.2s ease;
    }

    /* Top Navigation / Header */
    .top-header {
      background: var(--bg-card);
      border-bottom: 1px solid var(--border-color);
      position: sticky;
      top: 0;
      z-index: 100;
      box-shadow: var(--shadow-sm);
    }
    .header-inner {
      max-width: 1560px;
      margin: 0 auto;
      padding: 14px 24px;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
    }
    .brand-group {
      display: flex;
      align-items: center;
      gap: 14px;
    }
    .brand-badge {
      background: linear-gradient(135deg, #1e40af, #2563eb);
      color: #fff;
      font-weight: 700;
      font-size: 13px;
      padding: 6px 11px;
      border-radius: 8px;
      letter-spacing: 0.5px;
      box-shadow: 0 2px 8px rgba(37, 99, 235, 0.28);
    }
    .brand-title h1 {
      font-size: 19px;
      font-weight: 700;
      letter-spacing: -0.2px;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .brand-subtitle {
      font-size: 12.5px;
      color: var(--text-muted);
      margin-top: 2px;
    }

    .nav-actions {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 8px;
    }
    .nav-jump {
      display: flex;
      background: var(--bg-subtle);
      padding: 3px;
      border-radius: 9px;
      border: 1px solid var(--border-color);
      margin-right: 6px;
    }
    .nav-jump a {
      text-decoration: none;
      color: var(--text-secondary);
      font-size: 12.5px;
      font-weight: 600;
      padding: 5px 11px;
      border-radius: 6px;
      transition: all 0.15s;
    }
    .nav-jump a:hover {
      color: var(--accent);
      background: var(--bg-card);
    }

    .btn {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 12.5px;
      font-weight: 600;
      padding: 7px 13px;
      border-radius: 8px;
      border: 1px solid var(--border-color);
      background: var(--bg-card);
      color: var(--text-primary);
      cursor: pointer;
      transition: all 0.15s ease;
      font-family: inherit;
    }
    .btn:hover {
      background: var(--bg-hover);
      border-color: var(--border-strong);
    }
    .btn-primary {
      background: var(--accent);
      color: #ffffff;
      border-color: var(--accent);
    }
    .btn-primary:hover {
      background: #1e40af;
      border-color: #1e40af;
    }

    .container {
      max-width: 1560px;
      margin: 0 auto;
      padding: 18px 24px 0;
    }

    /* Guide / Risk Banner */
    .guide-banner {
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-left: 4px solid var(--accent);
      border-radius: var(--radius-md);
      padding: 12px 18px;
      margin-bottom: 18px;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      box-shadow: var(--shadow-sm);
    }
    .guide-rules {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 16px;
      font-size: 13px;
      color: var(--text-secondary);
    }
    .rule-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: var(--bg-subtle);
      padding: 4px 10px;
      border-radius: 6px;
      border: 1px solid var(--border-color);
      font-weight: 500;
    }
    .guide-warn {
      font-size: 12px;
      color: var(--warn-amber);
      background: var(--warn-amber-bg);
      padding: 4px 10px;
      border-radius: 6px;
      font-weight: 600;
    }

    /* 8 ETF Snapshot Cards */
    .etf-grid {
      display: grid;
      grid-template-columns: repeat(8, 1fr);
      gap: 12px;
      margin-bottom: 20px;
    }
    @media (max-width: 1400px) {
      .etf-grid {
        grid-template-columns: repeat(4, 1fr);
      }
    }
    @media (max-width: 768px) {
      .etf-grid {
        grid-template-columns: repeat(2, 1fr);
      }
    }
    .etf-card {
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
      padding: 12px 13px;
      box-shadow: var(--shadow-sm);
      cursor: pointer;
      transition: transform 0.15s ease, box-shadow 0.15s ease, border-color 0.15s ease;
      position: relative;
      overflow: hidden;
    }
    .etf-card:hover {
      transform: translateY(-2px);
      box-shadow: var(--shadow-md);
      border-color: var(--accent);
    }
    .etf-card-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 6px;
    }
    .etf-card-name {
      font-weight: 700;
      font-size: 13px;
      color: var(--text-primary);
      display: flex;
      align-items: center;
      gap: 5px;
      white-space: nowrap;
    }
    .color-dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      display: inline-block;
      flex-shrink: 0;
    }
    .etf-card-code {
      font-family: var(--font-mono);
      font-size: 11.5px;
      color: var(--text-muted);
      background: var(--bg-subtle);
      padding: 1px 6px;
      border-radius: 4px;
    }
    .etf-card-share {
      font-family: var(--font-mono);
      font-size: 19px;
      font-weight: 700;
      margin: 4px 0 6px;
      letter-spacing: -0.5px;
    }
    .etf-card-share span {
      font-size: 11.5px;
      font-weight: 500;
      color: var(--text-muted);
      margin-left: 3px;
    }
    .etf-card-metrics {
      display: flex;
      justify-content: space-between;
      font-size: 11.5px;
      color: var(--text-secondary);
      margin-bottom: 7px;
      font-family: var(--font-mono);
    }
    .pct-bar-wrap {
      height: 5px;
      background: var(--bg-subtle);
      border-radius: 999px;
      overflow: hidden;
      margin-bottom: 5px;
    }
    .pct-bar-fill {
      height: 100%;
      border-radius: 999px;
      transition: width 0.3s ease;
    }
    .etf-card-foot {
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 11px;
      color: var(--text-muted);
    }

    /* Section Card */
    .section-card {
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-lg);
      padding: 20px 22px;
      margin-bottom: 22px;
      box-shadow: var(--shadow-sm);
    }
    .section-header {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding-bottom: 14px;
      border-bottom: 1px solid var(--border-color);
      margin-bottom: 16px;
    }
    .section-title-group {
      display: flex;
      flex-direction: column;
      gap: 3px;
    }
    .section-title {
      font-size: 17px;
      font-weight: 700;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .section-num {
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
      padding: 2px 8px;
      border-radius: 6px;
    }
    .section-desc {
      font-size: 12.5px;
      color: var(--text-muted);
    }

    /* Filter Pills & Toolbar */
    .toolbar-row {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 12px;
    }
    .pill-group {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 6px;
    }
    .pill-label {
      font-size: 12px;
      font-weight: 600;
      color: var(--text-muted);
      margin-right: 4px;
    }
    .pill-btn {
      font-size: 12px;
      font-weight: 600;
      padding: 5px 11px;
      border-radius: 7px;
      border: 1px solid var(--border-color);
      background: var(--bg-subtle);
      color: var(--text-secondary);
      cursor: pointer;
      transition: all 0.15s ease;
    }
    .pill-btn:hover {
      border-color: var(--accent);
      color: var(--accent);
    }
    .pill-btn.active {
      background: var(--accent);
      color: #ffffff;
      border-color: var(--accent);
    }

    .checkbox-label {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 12.5px;
      font-weight: 600;
      color: var(--text-secondary);
      cursor: pointer;
      user-select: none;
      background: var(--bg-subtle);
      padding: 5px 11px;
      border-radius: 7px;
      border: 1px solid var(--border-color);
    }

    /* Chart containers */
    .chart-box-overview {
      width: 100%;
      height: 580px;
    }
    .chart-box-single {
      width: 100%;
      height: 540px;
    }
    .stacked-charts-grid {
      display: flex;
      flex-direction: column;
      gap: 24px;
    }
    .stacked-chart-item {
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
      padding: 14px 16px;
      background: var(--bg-subtle);
    }

    /* Style Selector Tabs for Section 2 */
    .style-tabs {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .style-tab {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      padding: 7px 15px;
      border-radius: 8px;
      font-size: 13px;
      font-weight: 700;
      border: 1px solid var(--border-color);
      background: var(--bg-subtle);
      color: var(--text-secondary);
      cursor: pointer;
      transition: all 0.15s;
    }
    .style-tab:hover {
      border-color: var(--accent);
      color: var(--text-primary);
    }
    .style-tab.active {
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
      box-shadow: 0 2px 8px rgba(29, 78, 216, 0.25);
    }

    /* Single ETF Insight Bar */
    .insight-strip {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
      padding: 11px 14px;
      background: var(--bg-subtle);
      border-radius: var(--radius-md);
      border: 1px solid var(--border-color);
    }
    .insight-item {
      display: flex;
      flex-direction: column;
      gap: 2px;
    }
    .insight-label {
      font-size: 11.5px;
      color: var(--text-muted);
    }
    .insight-val {
      font-size: 14.5px;
      font-weight: 700;
      font-family: var(--font-mono);
    }

    /* Section 3: Alert Signals Table & Summary Counters */
    .signal-counters {
      display: grid;
      grid-template-columns: repeat(6, 1fr);
      gap: 10px;
      margin-bottom: 16px;
    }
    @media (max-width: 1100px) {
      .signal-counters {
        grid-template-columns: repeat(3, 1fr);
      }
    }
    .signal-counter-card {
      background: var(--bg-subtle);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
      padding: 10px 14px;
      cursor: pointer;
      transition: all 0.15s;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .signal-counter-card:hover,
    .signal-counter-card.active {
      border-color: var(--accent);
      background: var(--accent-soft);
    }
    .sc-title {
      font-size: 12.5px;
      font-weight: 700;
      display: flex;
      flex-direction: column;
      gap: 2px;
    }
    .sc-sub {
      font-size: 11px;
      font-weight: 400;
      color: var(--text-muted);
    }
    .sc-count {
      font-family: var(--font-mono);
      font-size: 20px;
      font-weight: 800;
    }

    .filter-controls {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 10px;
      margin-bottom: 14px;
    }
    .select-input, .text-input {
      font-family: inherit;
      font-size: 12.5px;
      padding: 6px 11px;
      border-radius: 7px;
      border: 1px solid var(--border-color);
      background: var(--bg-subtle);
      color: var(--text-primary);
      outline: none;
    }
    .select-input:focus, .text-input:focus {
      border-color: var(--accent);
    }

    .table-wrap {
      overflow-x: auto;
      max-height: 620px;
      overflow-y: auto;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
    }
    table.signal-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      text-align: left;
    }
    table.signal-table th {
      position: sticky;
      top: 0;
      background: var(--bg-subtle);
      color: var(--text-secondary);
      font-weight: 700;
      padding: 10px 14px;
      border-bottom: 2px solid var(--border-color);
      border-right: 1px solid var(--border-color);
      white-space: nowrap;
      z-index: 2;
    }
    table.signal-table td {
      padding: 8.5px 14px;
      border-bottom: 1px solid var(--border-color);
      border-right: 1px solid var(--border-color);
      white-space: nowrap;
    }
    table.signal-table tr:hover td {
      background: var(--bg-hover);
    }
    .mono-cell {
      font-family: var(--font-mono);
    }
    .val-pos {
      color: var(--up-red);
      font-weight: 700;
    }
    .val-neg {
      color: var(--down-green);
      font-weight: 700;
    }

    /* Signal Badges */
    .sig-badge {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      font-size: 12px;
      font-weight: 700;
      padding: 2.5px 9px;
      border-radius: 6px;
      margin-right: 4px;
    }
    .sig-buy-single {
      background: var(--up-red-bg);
      color: var(--up-red);
    }
    .sig-sell-single {
      background: var(--warn-amber-bg);
      color: var(--warn-amber);
    }
    .sig-buy-5d {
      background: var(--purple-fire-bg);
      color: var(--purple-fire);
    }
    .sig-bottom-acc {
      background: var(--down-green-bg);
      color: var(--down-green);
    }
    .sig-top-dist {
      background: rgba(225, 29, 72, 0.12);
      color: #e11d48;
    }

    /* Threshold Config Modal */
    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(15, 23, 42, 0.55);
      backdrop-filter: blur(3px);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 999;
      padding: 20px;
    }
    .modal-backdrop.open {
      display: flex;
    }
    .modal-card {
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-lg);
      width: 100%;
      max-width: 820px;
      padding: 22px 24px;
      box-shadow: var(--shadow-md);
    }
    .modal-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding-bottom: 12px;
      border-bottom: 1px solid var(--border-color);
      margin-bottom: 14px;
    }
    .modal-head h3 {
      font-size: 16px;
      font-weight: 700;
    }
    .thresh-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12.5px;
      margin-bottom: 16px;
    }
    .thresh-table th, .thresh-table td {
      padding: 8px 10px;
      border-bottom: 1px solid var(--border-color);
      text-align: left;
    }
    .thresh-input {
      width: 82px;
      padding: 4px 8px;
      border: 1px solid var(--border-strong);
      border-radius: 6px;
      background: var(--bg-subtle);
      color: var(--text-primary);
      font-family: var(--font-mono);
      font-size: 12.5px;
    }
    /* Full-market broad ETF aggregation panel */
    .broad-market-panel { background:var(--bg-card); border:1px solid var(--border-color); border-radius:var(--radius-lg); padding:18px; margin:18px 0 22px; box-shadow:var(--shadow-sm); }
    .broad-market-head { display:flex; align-items:flex-start; justify-content:space-between; flex-wrap:wrap; gap:12px; margin-bottom:14px; }
    .broad-market-head h2 { font-size:18px; font-weight:750; }
    .broad-market-note { color:var(--text-muted); font-size:12px; margin-top:5px; }
    .broad-kpis { display:grid; grid-template-columns:repeat(4,minmax(130px,1fr)); gap:10px; margin:14px 0; }
    .broad-kpi { border:1px solid var(--border-color); border-radius:10px; padding:12px; background:var(--bg-subtle); }
    .broad-kpi .k-label { color:var(--text-muted); font-size:12px; }
    .broad-kpi .k-value { font:700 21px var(--font-mono); margin-top:5px; }
    .broad-table-wrap { overflow:auto; }
    .broad-table { border-collapse:collapse; width:100%; min-width:720px; font-size:13px; }
    .broad-table th,.broad-table td { padding:10px 9px; border-bottom:1px solid var(--border-color); text-align:right; white-space:nowrap; }
    .broad-table th:first-child,.broad-table td:first-child { text-align:left; }
    .broad-table th { color:var(--text-muted); font-weight:650; background:var(--bg-subtle); }
    .broad-expand { border:0; background:none; color:var(--accent); cursor:pointer; font:inherit; font-weight:700; }
    .broad-detail { display:none; background:var(--bg-subtle); }
    .broad-detail.open { display:table-row; }
    .broad-detail td { text-align:left; white-space:normal; }
    .broad-chip { display:inline-block; border:1px solid var(--border-color); border-radius:999px; padding:2px 8px; margin:2px 5px 2px 0; font-size:11px; color:var(--text-secondary); }
    .broad-status { font-size:12px; color:var(--text-muted); margin-top:10px; }
    .broad-daily-change-head { display:flex; align-items:baseline; justify-content:space-between; flex-wrap:wrap; gap:6px; margin:4px 2px 0; color:var(--text-primary); font-size:13px; }
    .broad-daily-change-head span { color:var(--text-muted); font-size:11px; font-weight:400; }
    @media(max-width:850px) { .broad-kpis {grid-template-columns:repeat(2,minmax(0,1fr));} }

    .toast {
      position: fixed;
      bottom: 24px;
      right: 24px;
      background: #1e293b;
      color: #fff;
      padding: 10px 16px;
      border-radius: 9px;
      font-size: 13px;
      font-weight: 600;
      box-shadow: 0 10px 25px rgba(0, 0, 0, 0.25);
      z-index: 1200;
      display: none;
    }
  </style>
</head>
<body>
  <!-- Sticky Top Navigation -->
  <header class="top-header">
    <div class="header-inner">
      <div class="brand-group">
        <span class="brand-badge">QTYX V3.6.6</span>
        <div class="brand-title">
          <h1>宽基 ETF 份额追踪 · 市场底背离监测系统</h1>
          <div class="brand-subtitle" id="metaSubtitle">加载中...</div>
        </div>
      </div>

      <div class="nav-actions">
        <nav class="nav-jump">
          <a href="#sec-overview">1. 全市场总览图</a>
          <a href="#sec-single">2. 单ETF资金共振</a>
          <a href="#sec-alerts">3. 预警信号清单</a>
        </nav>
        <button class="btn" id="btnConfigThresh" title="配置每只ETF独立异动阈值">⚙️ 独立阈值配置</button>
        <button class="btn" id="btnExportCsv" title="导出当前预警清单为CSV">📥 导出预警CSV</button>
        <button class="btn" id="btnThemeToggle" title="切换研报浅色/量化深色主题">🌓 切换主题</button>
        <button class="btn btn-primary" id="btnSyncLatest" title="数据由 GitHub Actions 每个交易日自动更新">⏱ 每日自动更新</button>
      </div>
    </div>
  </header>

  <main class="container">
    <!-- Reading Guide & Risk Reminder -->
    <div class="guide-banner">
      <div class="guide-rules">
        <span class="rule-pill">📏 <strong>看图统一规则</strong>：虚线 = 宽基指数走势，实线 = ETF 份额（起点归一到 100）</span>
        <span class="rule-pill">📉 <strong>同步创新低</strong>：指数跌 + 份额跌 → 资金出逃，抛压未结束</span>
        <span class="rule-pill">📈 <strong>资金底背离</strong>：指数持续回落 + 份额止跌拐头向上 → 大资金在低位主动申购承接</span>
      </div>
      <div class="guide-warn">⚠️ 提示：资金底先行 ≠ 价格底立刻到来，二者常有磨底时间差，仅供宏观观测辅助</div>
    </div>

    <!-- Full-market aggregation: all recognized ETFs across both exchanges -->
    <section class="broad-market-panel" id="sec-broad-market">
      <div class="broad-market-head">
        <div><h2>沪深两市宽基 ETF 全系列份额汇总</h2>
          <div class="broad-market-note">上证50 · 沪深300 · 中证500 · 中证1000 · 中证A500 · 科创宽基 · 创业板（创业板指+创业板50）</div>
        </div>
        <span class="rule-pill">按基金代码去重 · 单位：亿份</span>
      </div>
      <div class="broad-kpis" id="broadMarketKpis"></div>
      <div id="broadMarketChart" style="height:330px;width:100%;"></div>
      <div class="broad-daily-change-head">
        <strong>七大系列合计每日份额变化</strong>
        <span>较前一有效披露日 · 单位：亿份</span>
      </div>
      <div id="broadMarketDailyChangeChart" style="height:210px;width:100%;"></div>
      <div class="broad-table-wrap"><table class="broad-table"><thead><tr><th>系列</th><th>纳入ETF数量</th><th>最新总份额（亿份）</th><th>较前一披露日变化（亿份）</th><th>明细</th></tr></thead><tbody id="broadMarketRows"></tbody></table></div>
      <div class="broad-status" id="broadMarketStatus"></div>
    </section>

    <!-- 8 Core Broad-Based ETF Snapshot Cards -->
    <section class="etf-grid" id="etfSnapshotGrid"></section>

    <!-- SECTION 1: All-Market Normalized Comparison Chart -->
    <section class="section-card" id="sec-overview">
      <div class="section-header">
        <div class="section-title-group">
          <div class="section-title">
            <span class="section-num">板块一</span>
            主力资金动向：全市场宽基指数 &amp; ETF 份额归一对比
          </div>
          <div class="section-desc">
            五大宽基指数（虚线）+ 八大旗舰宽基 ETF 份额（实线）横向同框对比，一眼看清不同风格主力资金强弱与跨风格共振
          </div>
        </div>
        <div class="pill-group" id="overviewPeriodGroup">
          <span class="pill-label">区间快捷缩放：</span>
          <button class="pill-btn active" data-range="ALL">全部区间</button>
          <button class="pill-btn" data-range="YTD">2026年以来</button>
          <button class="pill-btn" data-range="6M">近6个月</button>
          <button class="pill-btn" data-range="3M">近3个月</button>
          <button class="pill-btn" data-range="1M">近1个月</button>
        </div>
      </div>

      <div class="toolbar-row">
        <div class="pill-group" id="overviewPresetGroup">
          <span class="pill-label">线条组合预设：</span>
          <button class="pill-btn active" data-preset="ALL">📊 全部显示 (5指数+8ETF)</button>
          <button class="pill-btn" data-preset="ONLY_ETF">📈 仅看8只ETF份额 (实线)</button>
          <button class="pill-btn" data-preset="ONLY_IDX">📉 仅看5大宽基指数 (虚线)</button>
          <button class="pill-btn" data-preset="STAR50">🚀 科创50底背离典型</button>
          <button class="pill-btn" data-preset="LARGE_CAP">🏛️ 大盘宽基 (沪深300+上证50)</button>
          <button class="pill-btn" data-preset="SMID_CAP">⚡ 中小盘成长 (中证500+1000)</button>
        </div>
        <label class="checkbox-label" title="开启后，拖动底部时间滑块时，以当前视野左端起点动态重新归一为 100">
          <input type="checkbox" id="chkDynamicRebase" />
          缩放区间起点动态重算基准=100
        </label>
      </div>

      <div id="chartOverview" class="chart-box-overview"></div>
    </section>

    <!-- SECTION 2: Single Broad-Based / Single ETF Resonance Chart -->
    <section class="section-card" id="sec-single">
      <div class="section-header">
        <div class="section-title-group">
          <div class="section-title">
            <span class="section-num">板块二</span>
            单宽基 / 单 ETF 资金共振独立观测图
          </div>
          <div class="section-desc">
            上图：左轴指数点位 + 右轴 ETF 真实份额（亿份）；下图：每日份额申赎变化柱状图（红柱净申购 / 绿柱净赎回）+ 5日累计流向
          </div>
        </div>
        <div class="pill-group">
          <button class="btn" id="btnToggleExpandMode">📜 切换为：五大宽基全部展开视图</button>
        </div>
      </div>

      <div class="toolbar-row" id="singleTabToolbar">
        <div class="style-tabs" id="styleTabsContainer"></div>
        <div class="pill-group" id="singleEtfSubSelect"></div>
      </div>

      <div class="insight-strip" id="singleInsightStrip"></div>

      <!-- Tab view container -->
      <div id="chartSingle" class="chart-box-single"></div>

      <!-- Stacked all-5-groups view container (hidden by default) -->
      <div id="stackedChartsContainer" class="stacked-charts-grid" style="display: none;"></div>
    </section>

    <!-- SECTION 3: ETF Alert Signal Table -->
    <section class="section-card" id="sec-alerts">
      <div class="section-header">
        <div class="section-title-group">
          <div class="section-title">
            <span class="section-num">板块三</span>
            🚨 全 ETF 预警信号清单
          </div>
          <div class="section-desc">
            自动扫描全历史周期，按日期倒序汇总触发异动的记录；每只 ETF 按规模体量独立配置阈值，点击任意行可定位至上方共振图对应日期
          </div>
        </div>
        <div class="pill-group">
          <label class="checkbox-label" title="开启后同一天若同时满足底部吸筹与单日大额申购，将同时展示多个标签">
            <input type="checkbox" id="chkShowMultiTags" />
            同日显示全部触发标签
          </label>
        </div>
      </div>

      <!-- 5 Signal Category Summary Cards -->
      <div class="signal-counters" id="signalCountersGrid"></div>

      <!-- Filter Bar -->
      <div class="filter-controls">
        <select class="select-input" id="selFilterEtf">
          <option value="ALL">全部 8 只宽基 ETF</option>
        </select>
        <select class="select-input" id="selFilterGroup">
          <option value="ALL">全部五大宽基板块</option>
          <option value="科创50">科创50</option>
          <option value="沪深300">沪深300</option>
          <option value="上证50">上证50</option>
          <option value="中证500">中证500</option>
          <option value="中证1000">中证1000</option>
        </select>
        <select class="select-input" id="selFilterWindow">
          <option value="60">最近 60 个交易日</option>
          <option value="120">最近 120 个交易日</option>
          <option value="250">最近 1 年 (250交易日)</option>
          <option value="ALL" selected>全部历史记录 (2024-06至今)</option>
        </select>
        <input type="text" class="text-input" id="inpSearchSignal" placeholder="搜索日期 / 代码 / 名称 / 预警类型..." style="min-width: 230px;" />
        <span id="signalCountSummary" style="margin-left: auto; font-size: 12.5px; color: var(--text-muted); font-weight: 600;"></span>
      </div>

      <div class="table-wrap">
        <table class="signal-table">
          <thead>
            <tr>
              <th>日期</th>
              <th>ETF代码</th>
              <th>ETF名称</th>
              <th>所属宽基</th>
              <th>预警类型</th>
              <th>当日变化(亿)</th>
              <th>5日累计(亿)</th>
              <th>份额分位数</th>
              <th>当日份额(亿)</th>
              <th>对应指数点位</th>
              <th>图表定位</th>
            </tr>
          </thead>
          <tbody id="signalTableBody"></tbody>
        </table>
      </div>
    </section>
  </main>

  <!-- Per-ETF Threshold Configuration Modal -->
  <div class="modal-backdrop" id="modalThresh">
    <div class="modal-card">
      <div class="modal-head">
        <h3>⚙️ 每只宽基 ETF 独立异动阈值配置 (单位：亿份 / 分位数%)</h3>
        <button class="btn" id="btnCloseModal">✕ 关闭</button>
      </div>
      <p style="font-size: 12.5px; color: var(--text-muted); margin-bottom: 12px;">
        不同宽基 ETF 规模体量差异较大（如科创50华夏、沪深300华泰规模数百上千亿，中证1000广发、中证500华夏体量较小），独立配置阈值可避免一刀切。修改后将立即重新扫描全部历史数据。
      </p>
      <div style="overflow-x: auto;">
        <table class="thresh-table">
          <thead>
            <tr>
              <th>ETF代码</th>
              <th>ETF名称</th>
              <th>单日大额申赎阈值(亿)</th>
              <th>5日持续建仓阈值(亿)</th>
              <th>底部吸筹分位上限(%)</th>
              <th>高位派发分位下限(%)</th>
            </tr>
          </thead>
          <tbody id="threshModalBody"></tbody>
        </table>
      </div>
      <div style="display: flex; justify-content: flex-end; gap: 10px;">
        <button class="btn" id="btnResetThresh">恢复默认阈值</button>
        <button class="btn btn-primary" id="btnSaveThresh">保存并重新扫描信号</button>
      </div>
    </div>
  </div>

  <div class="toast" id="toastMsg"></div>

  <script>
    window.ETF_TRACKER_DATA = __PAYLOAD_JSON__;
  </script>
  <script>
    (function () {
      const DATA = window.ETF_TRACKER_DATA;
      const dates = DATA.dates;
      const N = dates.length;

      // Full-market series aggregation (SSE + SZSE official share disclosures)
      function renderBroadMarket() {
        const B = DATA.broad_market || {};
        const groups = B.groups || [];
        const fmt = v => v == null ? "—" : Number(v).toLocaleString("zh-CN", {maximumFractionDigits:2});
        const sign = v => v == null ? "—" : (v > 0 ? "+" : "") + fmt(v);
        const total = B.total_latest;
        const totalChange = B.total_change1;
        const kpis = document.getElementById("broadMarketKpis");
        kpis.innerHTML = `
          <div class="broad-kpi"><div class="k-label">七大系列合计份额</div><div class="k-value">${fmt(total)} <small>亿份</small></div></div>
          <div class="broad-kpi"><div class="k-label">较前一披露日变化</div><div class="k-value" style="color:${totalChange>0?'var(--up-red)':totalChange<0?'var(--down-green)':'inherit'}">${sign(totalChange)}</div></div>
          <div class="broad-kpi"><div class="k-label">纳入统计的ETF数量</div><div class="k-value">${groups.reduce((a,g)=>a+(g.count||0),0)} <small>只</small></div></div>
          <div class="broad-kpi"><div class="k-label">份额数据区间</div><div class="k-value" style="font-size:14px">${B.start_date||'—'} 至 ${B.end_date||'—'}</div></div>`;
        const tbody = document.getElementById("broadMarketRows");
        tbody.innerHTML = groups.map((g,i)=>{
          const detail = (g.details||[]).map(x=>`<span class="broad-chip">${x.code} ${x.name} · ${fmt(x.shares)}亿份 · ${x.exchange}</span>`).join("") || "暂无可用明细";
          return `<tr><td><strong>${g.name}</strong></td><td>${g.count||0}</td><td>${fmt(g.latest)}</td><td style="color:${g.change1>0?'var(--up-red)':g.change1<0?'var(--down-green)':'inherit'}">${sign(g.change1)}</td><td><button class="broad-expand" data-expand="${i}">展开 ${g.count||0} 只ETF ▾</button></td></tr><tr class="broad-detail" id="broadDetail${i}"><td colspan="5">${detail}</td></tr>`;
        }).join("") + `<tr><td><strong>七大系列合计</strong></td><td>${groups.reduce((a,g)=>a+(g.count||0),0)}</td><td><strong>${fmt(total)}</strong></td><td>${sign(totalChange)}</td><td>按代码去重汇总</td></tr>`;
        tbody.querySelectorAll("[data-expand]").forEach(btn=>btn.addEventListener("click",()=>{
          const row=document.getElementById("broadDetail"+btn.dataset.expand); row.classList.toggle("open");
          btn.textContent=row.classList.contains("open")?"收起明细 ▴":`展开 ${groups[Number(btn.dataset.expand)].count||0} 只ETF ▾`;
        }));
        const status = document.getElementById("broadMarketStatus");
        const partial = B.partial_dates || [];
        status.textContent = `数据更新时间：${B.updated_at||'暂无'}；数据源：${B.source||'沪深交易所'}。${B.note||''}` +
          (partial.length ? ` ⚠️ ${partial.length} 个自然日存在交易所接口采集失败/缺口，相关日期需结合采集日志核验：${partial.slice(-8).join('、')}${partial.length>8?'…':''}` : " 当前采集未记录接口失败日期。") +
          ((B.errors||[]).length ? ` 采集异常 ${B.errors.length} 项；缺失数据不以前值填充。` : "");
        const el=document.getElementById("broadMarketChart");
        const dailyEl=document.getElementById("broadMarketDailyChangeChart");
        if (window.echarts && el && B.dates && B.dates.length) {
          const chart=echarts.init(el);
          const series=(groups||[]).map(g=>({name:g.name,type:'line',showSymbol:false,smooth:false,data:g.series||[],connectNulls:false}));
          series.push({name:'七大系列合计',type:'line',showSymbol:false,smooth:false,lineStyle:{width:3,type:'solid'},data:B.total_series||[],connectNulls:false});
          chart.setOption({tooltip:{trigger:'axis'},legend:{type:'scroll'},grid:{left:55,right:25,top:45,bottom:55},xAxis:{type:'category',data:B.dates,axisLabel:{hideOverlap:true}},yAxis:{type:'value',name:'亿份'},dataZoom:[{type:'inside'},{type:'slider'}],series});

          // 每日变化按前后两个有效披露日计算；缺失日保留 null，不当作 0。
          const totalSeries = B.total_series || [];
          let previousValid = null;
          const dailyChanges = totalSeries.map(value => {
            if (value == null || !Number.isFinite(Number(value))) return null;
            const current = Number(value);
            const change = previousValid == null ? null : Number((current - previousValid).toFixed(4));
            previousValid = current;
            return change;
          });
          if (dailyEl) {
            const dailyChart = echarts.init(dailyEl);
            dailyChart.setOption({
              animation: false,
              tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'shadow' },
                formatter: params => {
                  const p = params && params[0];
                  if (!p || p.value == null) return '';
                  const v = Number(p.value);
                  return `${p.axisValue}<br/>七大系列合计变化：<strong>${v > 0 ? '+' : ''}${v.toFixed(2)} 亿份</strong>`;
                }
              },
              grid: { left: 58, right: 24, top: 16, bottom: 48 },
              xAxis: { type: 'category', data: B.dates, axisLabel: { hideOverlap: true }, axisTick: { alignWithLabel: true } },
              yAxis: { type: 'value', name: '亿份', scale: true, axisLabel: { formatter: value => Number(value).toFixed(0) } },
              dataZoom: [{ type: 'inside' }, { type: 'slider', height: 18, bottom: 4 }],
              series: [{
                name: '每日份额变化', type: 'bar', data: dailyChanges, barMaxWidth: 12,
                itemStyle: { color: params => params.value >= 0 ? '#ef4444' : '#22c55e', borderRadius: [2, 2, 0, 0] },
                emphasis: { focus: 'series' }
              }]
            });
            window.addEventListener('resize', () => dailyChart.resize());
          }
          window.addEventListener('resize',()=>chart.resize());
        } else {
          if (el) el.innerHTML='<div style="padding:40px;text-align:center;color:var(--text-muted)">等待首次全市场数据采集</div>';
          if (dailyEl) dailyEl.innerHTML='<div style="padding:24px;text-align:center;color:var(--text-muted)">暂无每日变化数据</div>';
        }
      }
      renderBroadMarket();

      // Clone configurable thresholds into state (persisted in localStorage)
      const STORAGE_KEY = "qtyx_etf_thresholds_v366";
      let thresholds = {};
      function initThresholds(reset = false) {
        if (!reset) {
          try {
            const saved = localStorage.getItem(STORAGE_KEY);
            if (saved) thresholds = JSON.parse(saved);
          } catch (e) {}
        } else {
          thresholds = {};
          localStorage.removeItem(STORAGE_KEY);
        }
        DATA.etfs_meta.forEach((m) => {
          if (!thresholds[m.code]) {
            thresholds[m.code] = {
              single_thresh: m.single_thresh,
              five_day_thresh: m.five_day_thresh,
              low_pct: m.low_pct,
              high_pct: m.high_pct,
            };
          }
        });
      }
      initThresholds(false);

      // Compute derived metrics for each ETF across all N dates:
      // - d1: 1-day share diff (亿)
      // - d5: 5-day rolling share diff (亿)
      // - pct: historical rank percentile (0~100%)
      const derived = {};
      function computeDerived() {
        DATA.etfs_meta.forEach((m) => {
          const code = m.code;
          const arr = DATA.etf_series[code] || [];
          const d1 = new Array(N).fill(0);
          const d5 = new Array(N).fill(0);
          const pct = new Array(N).fill(0);

          for (let i = 0; i < N; i++) {
            d1[i] = i >= 1 && arr[i] != null && arr[i - 1] != null ? +(arr[i] - arr[i - 1]).toFixed(2) : 0;
            d5[i] = i >= 5 && arr[i] != null && arr[i - 5] != null ? +(arr[i] - arr[i - 5]).toFixed(2) : 0;
          }

          // Compute rank(pct=True) * 100 across the window (matching pandas rank(pct=True))
          const validVals = arr.filter((v) => v != null);
          const sortedVals = [...validVals].sort((a, b) => a - b);
          const totalValid = sortedVals.length || 1;
          for (let i = 0; i < N; i++) {
            const v = arr[i];
            if (v == null) {
              pct[i] = 0;
              continue;
            }
            // Average rank for ties (1-based)
            let firstIdx = -1;
            let lastIdx = -1;
            for (let j = 0; j < totalValid; j++) {
              if (sortedVals[j] === v) {
                if (firstIdx === -1) firstIdx = j;
                lastIdx = j;
              }
            }
            const avgRank = (firstIdx + lastIdx) / 2 + 1;
            pct[i] = +((avgRank / totalValid) * 100).toFixed(1);
          }

          derived[code] = { share: arr, d1, d5, pct };
        });
      }
      computeDerived();

      // Classify 5 Alert Signal Types
      // 1. 🚨单日大额申购: d1 >= single_thresh
      // 2. ⚠️单日大额赎回: d1 <= -single_thresh
      // 3. 🔥5日持续建仓: d5 >= five_day_thresh && d1 > 0
      // 4. 🟢底部吸筹: pct <= low_pct && d1 >= Math.max(2.0, single_thresh * 0.4)
      // 5. 🔴高位派发: pct >= high_pct && d1 <= -Math.max(2.0, single_thresh * 0.4)
      const SIGNAL_META = {
        SINGLE_BUY: { key: "SINGLE_BUY", label: "🚨单日大额申购", cls: "sig-buy-single", sub: "单日增量超阈值" },
        SINGLE_SELL: { key: "SINGLE_SELL", label: "⚠️单日大额赎回", cls: "sig-sell-single", sub: "单日减量超阈值" },
        FIVE_DAY_ACC: { key: "FIVE_DAY_ACC", label: "🔥5日持续建仓", cls: "sig-buy-5d", sub: "5日累计申购达标" },
        BOTTOM_ACC: { key: "BOTTOM_ACC", label: "🟢底部吸筹", cls: "sig-bottom-acc", sub: "低分位区间净流入" },
        TOP_DIST: { key: "TOP_DIST", label: "🔴高位派发", cls: "sig-top-dist", sub: "高分位区间净流出" },
      };

      let allSignals = [];
      function scanAllSignals() {
        const list = [];
        for (let i = N - 1; i >= 1; i--) {
          const dt = dates[i];
          DATA.etfs_meta.forEach((m) => {
            const code = m.code;
            const t = thresholds[code];
            const d1 = derived[code].d1[i];
            const d5 = derived[code].d5[i];
            const pct = derived[code].pct[i];
            const share = derived[code].share[i];
            const idxVal = (DATA.indices_series[m.index_code] || [])[i];

            const tags = [];
            const isSingleBuy = d1 >= t.single_thresh;
            const isSingleSell = d1 <= -t.single_thresh;
            const isFiveDay = d5 >= t.five_day_thresh && d1 >= Math.min(3.0, t.single_thresh * 0.5);
            const isBottomAcc = pct <= t.low_pct && d1 >= Math.max(2.5, t.single_thresh * 0.45);
            const isTopDist = pct >= t.high_pct && d1 <= -Math.max(2.5, t.single_thresh * 0.45);

            // Determine primary signal matching QTYX table behavior, while also storing all matched tags
            if (isSingleBuy) tags.push(SIGNAL_META.SINGLE_BUY);
            if (isSingleSell) tags.push(SIGNAL_META.SINGLE_SELL);
            if (isFiveDay) tags.push(SIGNAL_META.FIVE_DAY_ACC);
            if (isBottomAcc) tags.push(SIGNAL_META.BOTTOM_ACC);
            if (isTopDist) tags.push(SIGNAL_META.TOP_DIST);

            if (tags.length > 0) {
              // QTYX primary label priority: if 5-day accumulation triggered and d1 is moderate/initial breakout, or single-day big creation
              let primary = tags[0];
              if (isFiveDay && (!isSingleBuy || (pct < 35 && d5 >= t.five_day_thresh && d1 < t.single_thresh * 1.45))) {
                primary = SIGNAL_META.FIVE_DAY_ACC;
              }
              list.push({
                dateIdx: i,
                date: dt,
                code,
                name: m.name,
                group: m.group,
                index_code: m.index_code,
                index_name: m.index_name,
                primary,
                tags,
                d1,
                d5,
                pct,
                share,
                idxVal,
              });
            }
          });
        }
        allSignals = list;
      }
      scanAllSignals();

      // Update Header Subtitle
      document.getElementById("metaSubtitle").textContent =
        `数据区间：${DATA.start_date} 至 ${DATA.end_date}（共 ${DATA.total_days} 个交易日） · 最近同步：${DATA.updated_at} · 数据源：沪深交易所ETF份额 & 腾讯指数日线`;

      // Render 8 ETF Snapshot Cards
      function renderSnapshotCards() {
        const container = document.getElementById("etfSnapshotGrid");
        const lastIdx = N - 1;
        // Find latest signal for each ETF
        const latestSigMap = {};
        allSignals.forEach((s) => {
          if (!latestSigMap[s.code]) latestSigMap[s.code] = s;
        });

        container.innerHTML = DATA.etfs_meta
          .map((m) => {
            const c = m.code;
            const share = derived[c].share[lastIdx] || 0;
            const d1 = derived[c].d1[lastIdx] || 0;
            const d5 = derived[c].d5[lastIdx] || 0;
            const pct = derived[c].pct[lastIdx] || 0;
            const d1Str = (d1 >= 0 ? "+" : "") + d1.toFixed(2);
            const d5Str = (d5 >= 0 ? "+" : "") + d5.toFixed(2);
            const d1Cls = d1 > 0 ? "val-pos" : d1 < 0 ? "val-neg" : "";
            const d5Cls = d5 > 0 ? "val-pos" : d5 < 0 ? "val-neg" : "";
            const sig = latestSigMap[c];
            const sigHtml = sig
              ? `<span class="sig-badge ${sig.primary.cls}" style="font-size:10.5px;padding:1px 6px;">${sig.primary.label} (${sig.date.slice(5)})</span>`
              : `<span style="color:var(--text-muted)">近期平稳</span>`;

            return `
              <div class="etf-card" data-group="${m.group}" data-code="${c}" title="点击查看 ${m.name} (${c}) 独立资金共振图">
                <div class="etf-card-top">
                  <div class="etf-card-name">
                    <span class="color-dot" style="background:${m.color}"></span>
                    ${m.name}
                  </div>
                  <span class="etf-card-code">${c}</span>
                </div>
                <div class="etf-card-share">
                  ${share.toFixed(2)}<span>亿份</span>
                </div>
                <div class="etf-card-metrics">
                  <span>当日 <strong class="${d1Cls}">${d1Str}</strong></span>
                  <span>5日 <strong class="${d5Cls}">${d5Str}</strong></span>
                </div>
                <div class="pct-bar-wrap" title="历史份额分位数：${pct.toFixed(1)}%">
                  <div class="pct-bar-fill" style="width:${Math.min(100, Math.max(4, pct))}%;background:${m.color};"></div>
                </div>
                <div class="etf-card-foot">
                  <span>分位 <strong>${pct.toFixed(1)}%</strong></span>
                  ${sigHtml}
                </div>
              </div>
            `;
          })
          .join("");

        container.querySelectorAll(".etf-card").forEach((card) => {
          card.addEventListener("click", () => {
            const grp = card.getAttribute("data-group");
            const code = card.getAttribute("data-code");
            selectSingleGroup(grp, code);
            document.getElementById("sec-single").scrollIntoView({ behavior: "smooth", block: "start" });
          });
        });
      }
      renderSnapshotCards();

      // =========================================================================
      // SECTION 1: ALL-MARKET NORMALIZED OVERVIEW CHART
      // =========================================================================
      const chartOverview = echarts.init(document.getElementById("chartOverview"));
      let overviewZoomStart = 0;
      let overviewZoomEnd = 100;

      function getThemeColors() {
        const isDark = document.documentElement.getAttribute("data-theme") === "dark";
        return {
          isDark,
          text: isDark ? "#e2e8f0" : "#1e293b",
          subText: isDark ? "#94a3b8" : "#64748b",
          gridLine: isDark ? "rgba(148, 163, 184, 0.13)" : "#f1f5f9",
          axisLine: isDark ? "#334155" : "#cbd5e1",
          tooltipBg: isDark ? "rgba(15, 23, 42, 0.94)" : "rgba(255, 255, 255, 0.96)",
        };
      }

      function buildNormalizedSeries(baseIdx = 0) {
        const series = [];
        // 1. 5 Indices (dashed lines)
        DATA.indices_meta.forEach((m) => {
          const raw = DATA.indices_series[m.code] || [];
          const baseVal = raw[baseIdx] || raw.find((v) => v != null) || 1;
          const normData = raw.map((v) => (v != null ? +((v / baseVal) * 100).toFixed(2) : null));
          series.push({
            name: m.name,
            type: "line",
            data: normData,
            rawValues: raw,
            unit: "点",
            isIndex: true,
            group: m.group,
            symbol: "none",
            smooth: false,
            lineStyle: {
              width: 1.8,
              type: "dashed",
              color: m.color,
            },
            itemStyle: { color: m.color },
            emphasis: { focus: "series", lineStyle: { width: 3 } },
          });
        });

        // 2. 8 ETFs (solid lines)
        DATA.etfs_meta.forEach((m) => {
          const raw = DATA.etf_series[m.code] || [];
          const baseVal = raw[baseIdx] || raw.find((v) => v != null) || 1;
          const normData = raw.map((v) => (v != null ? +((v / baseVal) * 100).toFixed(2) : null));
          series.push({
            name: m.name,
            type: "line",
            data: normData,
            rawValues: raw,
            unit: "亿份",
            isIndex: false,
            group: m.group,
            symbol: "none",
            smooth: false,
            lineStyle: {
              width: 2.2,
              type: "solid",
              color: m.color,
            },
            itemStyle: { color: m.color },
            emphasis: { focus: "series", lineStyle: { width: 3.5 } },
          });
        });

        return series;
      }

      let currentLegendSelected = {};
      function initLegendSelected(preset = "ALL") {
        const sel = {};
        DATA.indices_meta.forEach((m) => {
          if (preset === "ALL" || preset === "ONLY_IDX") sel[m.name] = true;
          else if (preset === "STAR50") sel[m.name] = m.group === "科创50";
          else if (preset === "LARGE_CAP") sel[m.name] = m.group === "沪深300" || m.group === "上证50";
          else if (preset === "SMID_CAP") sel[m.name] = m.group === "中证500" || m.group === "中证1000";
          else sel[m.name] = false;
        });
        DATA.etfs_meta.forEach((m) => {
          if (preset === "ALL" || preset === "ONLY_ETF") sel[m.name] = true;
          else if (preset === "STAR50") sel[m.name] = m.group === "科创50";
          else if (preset === "LARGE_CAP") sel[m.name] = m.group === "沪深300" || m.group === "上证50";
          else if (preset === "SMID_CAP") sel[m.name] = m.group === "中证500" || m.group === "中证1000";
          else sel[m.name] = false;
        });
        currentLegendSelected = sel;
      }
      initLegendSelected("ALL");

      function renderOverviewChart(baseIdx = 0) {
        const tc = getThemeColors();
        const series = buildNormalizedSeries(baseIdx);

        const option = {
          backgroundColor: "transparent",
          title: {
            text: "主力资金动向：全市场宽基 & ETF份额对比",
            subtext: `线型区分：虚线=指数走势，实线=ETF份额；所有线条独立配色（当前基准=100 起点：${dates[baseIdx]}）`,
            left: "center",
            top: 4,
            textStyle: { color: tc.text, fontSize: 16, fontWeight: 700 },
            subtextStyle: { color: tc.subText, fontSize: 12 },
          },
          tooltip: {
            trigger: "axis",
            axisPointer: { type: "cross", label: { backgroundColor: "#3b5bdb" } },
            backgroundColor: tc.tooltipBg,
            borderColor: tc.axisLine,
            textStyle: { color: tc.text, fontSize: 12 },
            formatter: function (params) {
              if (!params || !params.length) return "";
              const dt = params[0].axisValue;
              const idx = params[0].dataIndex;
              let html = `<div style="font-weight:700;margin-bottom:6px;border-bottom:1px solid ${tc.axisLine};padding-bottom:4px;">📅 ${dt}（基准起点 ${dates[baseIdx]}=100）</div>`;
              params.forEach((p) => {
                if (p.value == null) return;
                const sObj = series[p.seriesIndex];
                const rawVal = sObj && sObj.rawValues ? sObj.rawValues[idx] : "";
                const rawStr = rawVal != null ? ` <span style="opacity:0.75">(${rawVal.toFixed(2)}${sObj.unit})</span>` : "";
                const lineTag = sObj && sObj.isIndex ? "[指数·虚]" : "[份额·实]";
                html += `<div style="display:flex;justify-content:space-between;gap:16px;line-height:1.65;">
                  <span>${p.marker} <span style="opacity:0.7;font-size:11px;">${lineTag}</span> ${p.seriesName}</span>
                  <span style="font-family:monospace;font-weight:700;">${p.value.toFixed(2)}${rawStr}</span>
                </div>`;
              });
              return html;
            },
          },
          legend: {
            top: 52,
            left: "center",
            type: "scroll",
            selected: currentLegendSelected,
            textStyle: { color: tc.text, fontSize: 12 },
            pageTextStyle: { color: tc.subText },
          },
          grid: {
            left: 58,
            right: 32,
            top: 112,
            bottom: 75,
          },
          xAxis: {
            type: "category",
            data: dates,
            boundaryGap: false,
            axisLine: { lineStyle: { color: tc.axisLine } },
            axisLabel: { color: tc.subText, fontSize: 11, rotate: 30 },
            splitLine: { show: true, lineStyle: { color: tc.gridLine } },
          },
          yAxis: {
            type: "value",
            name: "相对起点涨跌幅 (基准=100)",
            nameTextStyle: { color: tc.subText, fontSize: 12, padding: [0, 0, 0, 24] },
            axisLine: { show: false },
            axisLabel: { color: tc.subText, fontSize: 11.5 },
            splitLine: { lineStyle: { color: tc.gridLine } },
          },
          dataZoom: [
            {
              type: "inside",
              start: overviewZoomStart,
              end: overviewZoomEnd,
            },
            {
              type: "slider",
              bottom: 10,
              height: 26,
              start: overviewZoomStart,
              end: overviewZoomEnd,
              borderColor: tc.axisLine,
              textStyle: { color: tc.subText },
            },
          ],
          series,
        };

        chartOverview.setOption(option, true);
      }
      renderOverviewChart(0);

      chartOverview.on("legendselectchanged", function (params) {
        currentLegendSelected = params.selected;
      });

      chartOverview.on("datazoom", function () {
        const opt = chartOverview.getOption();
        if (opt.dataZoom && opt.dataZoom[0]) {
          overviewZoomStart = opt.dataZoom[0].start;
          overviewZoomEnd = opt.dataZoom[0].end;
          if (document.getElementById("chkDynamicRebase").checked) {
            const startIdx = Math.max(0, Math.min(N - 1, Math.floor((overviewZoomStart / 100) * (N - 1))));
            renderOverviewChart(startIdx);
          }
        }
      });

      document.getElementById("chkDynamicRebase").addEventListener("change", function () {
        const startIdx = this.checked
          ? Math.max(0, Math.min(N - 1, Math.floor((overviewZoomStart / 100) * (N - 1))))
          : 0;
        renderOverviewChart(startIdx);
      });

      // Preset Filter Buttons for Section 1
      document.querySelectorAll("#overviewPresetGroup .pill-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
          document.querySelectorAll("#overviewPresetGroup .pill-btn").forEach((b) => b.classList.remove("active"));
          btn.classList.add("active");
          const preset = btn.getAttribute("data-preset");
          initLegendSelected(preset);
          const startIdx = document.getElementById("chkDynamicRebase").checked
            ? Math.max(0, Math.min(N - 1, Math.floor((overviewZoomStart / 100) * (N - 1))))
            : 0;
          renderOverviewChart(startIdx);
        });
      });

      // Period Range Buttons for Section 1
      document.querySelectorAll("#overviewPeriodGroup .pill-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
          document.querySelectorAll("#overviewPeriodGroup .pill-btn").forEach((b) => b.classList.remove("active"));
          btn.classList.add("active");
          const range = btn.getAttribute("data-range");
          let startIdx = 0;
          if (range === "1M") startIdx = Math.max(0, N - 22);
          else if (range === "3M") startIdx = Math.max(0, N - 65);
          else if (range === "6M") startIdx = Math.max(0, N - 130);
          else if (range === "YTD") {
            const ytdIdx = dates.findIndex((d) => d >= "2026-01-01");
            startIdx = ytdIdx >= 0 ? ytdIdx : 0;
          }
          overviewZoomStart = +((startIdx / Math.max(1, N - 1)) * 100).toFixed(2);
          overviewZoomEnd = 100;
          const baseIdx = document.getElementById("chkDynamicRebase").checked ? startIdx : 0;
          renderOverviewChart(baseIdx);
        });
      });

      // =========================================================================
      // SECTION 2: SINGLE BROAD-BASED / SINGLE ETF RESONANCE CHART
      // =========================================================================
      const GROUPS = [
        { group: "科创50", index_code: "sh000688", index_name: "科创50指数", desc: "高弹性科技创新成长赛道" },
        { group: "沪深300", index_code: "sh000300", index_name: "沪深300指数", desc: "A股核心蓝筹与汇金护盘主力" },
        { group: "中证1000", index_code: "sh000852", index_name: "中证1000指数", desc: "中小盘高弹性宽基代表" },
        { group: "中证500", index_code: "sh000905", index_name: "中证500指数", desc: "中盘成长核心宽基代表" },
        { group: "上证50", index_code: "sh000016", index_name: "上证50指数", desc: "超大盘权重蓝筹风向标" },
      ];

      let activeGroup = "科创50";
      let activeEtfCode = "588000"; // Or "SUM" for group total when group has 2 ETFs
      let isAllExpanded = false;
      const chartSingle = echarts.init(document.getElementById("chartSingle"));
      let stackedChartInstances = [];

      function renderStyleTabs() {
        const tabContainer = document.getElementById("styleTabsContainer");
        tabContainer.innerHTML = GROUPS.map((g) => {
          const etfs = DATA.etfs_meta.filter((e) => e.group === g.group);
          const names = etfs.map((e) => e.name).join(" / ");
          return `
            <button class="style-tab ${g.group === activeGroup ? "active" : ""}" data-group="${g.group}">
              <span>${g.group} 资金共振</span>
              <span style="font-size:11px;opacity:0.8;font-weight:500;">(${names})</span>
            </button>
          `;
        }).join("");

        tabContainer.querySelectorAll(".style-tab").forEach((btn) => {
          btn.addEventListener("click", () => {
            const grp = btn.getAttribute("data-group");
            const etfs = DATA.etfs_meta.filter((e) => e.group === grp);
            selectSingleGroup(grp, etfs[0].code);
          });
        });
      }

      function renderSubEtfPills() {
        const subContainer = document.getElementById("singleEtfSubSelect");
        const etfs = DATA.etfs_meta.filter((e) => e.group === activeGroup);
        if (etfs.length <= 1) {
          subContainer.innerHTML = `<span class="pill-label">当前观测标的：<strong>${etfs[0].name} (${etfs[0].code})</strong></span>`;
          return;
        }
        let html = `<span class="pill-label">标的选择：</span>`;
        etfs.forEach((e) => {
          html += `<button class="pill-btn ${activeEtfCode === e.code ? "active" : ""}" data-code="${e.code}">${e.name} (${e.code})</button>`;
        });
        html += `<button class="pill-btn ${activeEtfCode === "SUM" ? "active" : ""}" data-code="SUM">🔗 ${activeGroup}两只ETF合计份额</button>`;
        subContainer.innerHTML = html;

        subContainer.querySelectorAll(".pill-btn").forEach((btn) => {
          btn.addEventListener("click", () => {
            activeEtfCode = btn.getAttribute("data-code");
            renderSubEtfPills();
            renderSingleInsightAndChart();
          });
        });
      }

      function getResonanceData(groupName, etfCodeOrSum) {
        const gMeta = GROUPS.find((g) => g.group === groupName);
        const idxSeries = DATA.indices_series[gMeta.index_code] || [];
        const etfs = DATA.etfs_meta.filter((e) => e.group === groupName);

        let shareSeries = [];
        let d1Series = [];
        let d5Series = [];
        let pctSeries = [];
        let titleLabel = "";
        let threshSingle = 10;

        if (etfCodeOrSum === "SUM" && etfs.length > 1) {
          titleLabel = `${groupName}合计 (${etfs.map((e) => e.name).join("+")})`;
          threshSingle = etfs.reduce((acc, e) => acc + thresholds[e.code].single_thresh, 0);
          for (let i = 0; i < N; i++) {
            const sumVal = etfs.reduce((acc, e) => acc + (derived[e.code].share[i] || 0), 0);
            shareSeries.push(+sumVal.toFixed(2));
          }
          for (let i = 0; i < N; i++) {
            d1Series.push(i >= 1 ? +(shareSeries[i] - shareSeries[i - 1]).toFixed(2) : 0);
            d5Series.push(i >= 5 ? +(shareSeries[i] - shareSeries[i - 5]).toFixed(2) : 0);
          }
          const sorted = [...shareSeries].sort((a, b) => a - b);
          pctSeries = shareSeries.map((v) => +(((sorted.indexOf(v) + 1) / N) * 100).toFixed(1));
        } else {
          const targetEtf = etfs.find((e) => e.code === etfCodeOrSum) || etfs[0];
          titleLabel = `${targetEtf.name} (${targetEtf.code})`;
          threshSingle = thresholds[targetEtf.code].single_thresh;
          shareSeries = derived[targetEtf.code].share;
          d1Series = derived[targetEtf.code].d1;
          d5Series = derived[targetEtf.code].d5;
          pctSeries = derived[targetEtf.code].pct;
        }

        return {
          gMeta,
          etfs,
          idxSeries,
          shareSeries,
          d1Series,
          d5Series,
          pctSeries,
          titleLabel,
          threshSingle,
        };
      }

      function buildResonanceOption(groupName, etfCodeOrSum, highlightDateIdx = null) {
        const tc = getThemeColors();
        const { gMeta, idxSeries, shareSeries, d1Series, d5Series, pctSeries, titleLabel, threshSingle } =
          getResonanceData(groupName, etfCodeOrSum);

        const markPointData = [];
        if (highlightDateIdx != null && highlightDateIdx >= 0 && highlightDateIdx < N) {
          markPointData.push({
            name: "定位日期",
            coord: [dates[highlightDateIdx], shareSeries[highlightDateIdx]],
            value: `${dates[highlightDateIdx]}\n${shareSeries[highlightDateIdx].toFixed(2)}亿`,
            itemStyle: { color: "#dc2626" },
          });
        }

        return {
          backgroundColor: "transparent",
          title: {
            text: `${groupName} 资金共振总览`,
            subtext: `${gMeta.index_name}走势 vs ${titleLabel} 份额 & 每日申赎净变化（单日异动阈值：±${threshSingle}亿份）`,
            left: "center",
            top: 2,
            textStyle: { color: tc.text, fontSize: 15.5, fontWeight: 700 },
            subtextStyle: { color: tc.subText, fontSize: 12 },
          },
          tooltip: {
            trigger: "axis",
            axisPointer: { type: "cross", link: [{ xAxisIndex: "all" }] },
            backgroundColor: tc.tooltipBg,
            borderColor: tc.axisLine,
            textStyle: { color: tc.text, fontSize: 12 },
            formatter: function (params) {
              if (!params || !params.length) return "";
              const idx = params[0].dataIndex;
              const dt = dates[idx];
              const idxVal = idxSeries[idx];
              const shVal = shareSeries[idx];
              const d1 = d1Series[idx];
              const d5 = d5Series[idx];
              const pct = pctSeries[idx];
              const d1Color = d1 > 0 ? "#dc2626" : d1 < 0 ? "#16a34a" : tc.text;
              const d5Color = d5 > 0 ? "#dc2626" : d5 < 0 ? "#16a34a" : tc.text;
              return `
                <div style="font-weight:700;margin-bottom:6px;border-bottom:1px solid ${tc.axisLine};padding-bottom:4px;">
                  📅 ${dt} · ${groupName} (${titleLabel})
                </div>
                <div style="display:flex;justify-content:space-between;gap:18px;line-height:1.7;">
                  <span>📉 ${gMeta.index_name}：</span>
                  <strong style="font-family:monospace;">${idxVal != null ? idxVal.toFixed(2) : "-"} 点</strong>
                </div>
                <div style="display:flex;justify-content:space-between;gap:18px;line-height:1.7;">
                  <span>🏦 ETF总份额：</span>
                  <strong style="font-family:monospace;">${shVal != null ? shVal.toFixed(2) : "-"} 亿份 (分位 ${pct}%)</strong>
                </div>
                <div style="display:flex;justify-content:space-between;gap:18px;line-height:1.7;">
                  <span>📊 当日份额变化：</span>
                  <strong style="font-family:monospace;color:${d1Color};">${d1 >= 0 ? "+" : ""}${d1.toFixed(2)} 亿份</strong>
                </div>
                <div style="display:flex;justify-content:space-between;gap:18px;line-height:1.7;">
                  <span>🔥 5日累计变化：</span>
                  <strong style="font-family:monospace;color:${d5Color};">${d5 >= 0 ? "+" : ""}${d5.toFixed(2)} 亿份</strong>
                </div>
              `;
            },
          },
          legend: {
            top: 48,
            left: "center",
            textStyle: { color: tc.text, fontSize: 12 },
          },
          axisPointer: {
            link: [{ xAxisIndex: "all" }],
          },
          grid: [
            { left: 64, right: 72, top: 86, height: "50%" },
            { left: 64, right: 72, top: "64%", height: "22%" },
          ],
          xAxis: [
            {
              type: "category",
              data: dates,
              gridIndex: 0,
              boundaryGap: false,
              axisLine: { lineStyle: { color: tc.axisLine } },
              axisLabel: { show: false },
              axisTick: { show: false },
              splitLine: { show: true, lineStyle: { color: tc.gridLine } },
            },
            {
              type: "category",
              data: dates,
              gridIndex: 1,
              boundaryGap: true,
              axisLine: { lineStyle: { color: tc.axisLine } },
              axisLabel: { color: tc.subText, fontSize: 11 },
              axisTick: { show: true },
              splitLine: { show: true, lineStyle: { color: tc.gridLine } },
            },
          ],
          yAxis: [
            {
              type: "value",
              name: "指数点位",
              gridIndex: 0,
              scale: true,
              nameTextStyle: { color: tc.subText, fontSize: 11.5 },
              axisLabel: { color: tc.subText, fontSize: 11 },
              splitLine: { lineStyle: { color: tc.gridLine } },
            },
            {
              type: "value",
              name: "ETF份额(亿)",
              gridIndex: 0,
              scale: true,
              nameTextStyle: { color: tc.subText, fontSize: 11.5 },
              axisLabel: {
                color: tc.subText,
                fontSize: 11,
                formatter: "{value} 亿",
              },
              splitLine: { show: false },
            },
            {
              type: "value",
              name: "每日变化(亿)",
              gridIndex: 1,
              nameTextStyle: { color: tc.subText, fontSize: 11 },
              axisLabel: { color: tc.subText, fontSize: 10.5 },
              splitLine: { lineStyle: { color: tc.gridLine } },
            },
          ],
          dataZoom: [
            { type: "inside", xAxisIndex: [0, 1], start: 0, end: 100 },
            {
              type: "slider",
              xAxisIndex: [0, 1],
              bottom: 6,
              height: 22,
              start: 0,
              end: 100,
              borderColor: tc.axisLine,
              textStyle: { color: tc.subText },
            },
          ],
          series: [
            {
              name: gMeta.index_name,
              type: "line",
              xAxisIndex: 0,
              yAxisIndex: 0,
              data: idxSeries,
              symbol: "none",
              lineStyle: { width: 2, color: "#94a3b8" },
              itemStyle: { color: "#94a3b8" },
            },
            {
              name: `${titleLabel}份额`,
              type: "line",
              xAxisIndex: 0,
              yAxisIndex: 1,
              data: shareSeries,
              symbol: "none",
              lineStyle: { width: 2.3, color: tc.isDark ? "#60a5fa" : "#1e293b" },
              itemStyle: { color: tc.isDark ? "#60a5fa" : "#1e293b" },
              markPoint: markPointData.length ? { data: markPointData, symbolSize: 58 } : undefined,
            },
            {
              name: `${titleLabel}每日变化`,
              type: "bar",
              xAxisIndex: 1,
              yAxisIndex: 2,
              data: d1Series,
              itemStyle: {
                color: function (p) {
                  return p.value >= 0 ? "#ef4444" : "#22c55e";
                },
              },
            },
          ],
        };
      }

      function renderSingleInsightAndChart(highlightDateIdx = null) {
        const { gMeta, idxSeries, shareSeries, d1Series, d5Series, pctSeries, titleLabel, threshSingle } =
          getResonanceData(activeGroup, activeEtfCode);

        const last = N - 1;
        // Compare 20-day index return vs 20-day share change to diagnose divergence
        const lookback = Math.min(20, last);
        const idxChg20 =
          idxSeries[last - lookback] > 0
            ? (((idxSeries[last] - idxSeries[last - lookback]) / idxSeries[last - lookback]) * 100).toFixed(2)
            : "0.00";
        const shareDiff20 = (shareSeries[last] - shareSeries[last - lookback]).toFixed(2);

        let patternText = "📊 震荡观察期";
        if (+idxChg20 < -1.5 && +shareDiff20 > threshSingle * 0.6) {
          patternText = "🟢 指数回落 + 份额逆势扩张（典型资金底背离/低位承接）";
        } else if (+idxChg20 > 1.5 && +shareDiff20 > threshSingle * 0.6) {
          patternText = "🔥 量价齐升（指数与ETF份额同步走强共振）";
        } else if (+idxChg20 < -1.5 && +shareDiff20 < -threshSingle * 0.6) {
          patternText = "⚠️ 指数与份额同步回落（资金暂未大规模承接）";
        } else if (+idxChg20 > 2.0 && +shareDiff20 < -threshSingle * 0.6) {
          patternText = "🔴 指数反弹 + 份额净流出（逢高止盈赎回）";
        }

        const strip = document.getElementById("singleInsightStrip");
        strip.innerHTML = `
          <div class="insight-item">
            <span class="insight-label">当前观测标的</span>
            <span class="insight-val">${titleLabel}</span>
          </div>
          <div class="insight-item">
            <span class="insight-label">最新ETF份额 / 历史分位</span>
            <span class="insight-val">${shareSeries[last].toFixed(2)} 亿份 (${pctSeries[last]}%)</span>
          </div>
          <div class="insight-item">
            <span class="insight-label">最新单日 / 5日累计变化</span>
            <span class="insight-val">
              <span class="${d1Series[last] >= 0 ? "val-pos" : "val-neg"}">${d1Series[last] >= 0 ? "+" : ""}${d1Series[last].toFixed(2)}亿</span>
              /
              <span class="${d5Series[last] >= 0 ? "val-pos" : "val-neg"}">${d5Series[last] >= 0 ? "+" : ""}${d5Series[last].toFixed(2)}亿</span>
            </span>
          </div>
          <div class="insight-item">
            <span class="insight-label">近20交易日指数涨跌 / 份额增减</span>
            <span class="insight-val">${idxChg20 >= 0 ? "+" : ""}${idxChg20}% / ${shareDiff20 >= 0 ? "+" : ""}${shareDiff20}亿</span>
          </div>
          <div class="insight-item" style="grid-column: span 2;">
            <span class="insight-label">近20日资金-价格结构判定</span>
            <span class="insight-val" style="font-family:var(--font-sans);font-size:13.5px;">${patternText}</span>
          </div>
        `;

        const opt = buildResonanceOption(activeGroup, activeEtfCode, highlightDateIdx);
        chartSingle.setOption(opt, true);
      }

      function renderAllStackedCharts() {
        const container = document.getElementById("stackedChartsContainer");
        stackedChartInstances.forEach((inst) => inst.dispose());
        stackedChartInstances = [];

        container.innerHTML = GROUPS.map((g, idx) => {
          return `<div class="stacked-chart-item"><div id="stackedChart_${idx}" class="chart-box-single"></div></div>`;
        }).join("");

        GROUPS.forEach((g, idx) => {
          const dom = document.getElementById(`stackedChart_${idx}`);
          const inst = echarts.init(dom);
          const etfs = DATA.etfs_meta.filter((e) => e.group === g.group);
          const opt = buildResonanceOption(g.group, etfs[0].code);
          inst.setOption(opt, true);
          stackedChartInstances.push(inst);
        });
      }

      function selectSingleGroup(groupName, etfCode) {
        activeGroup = groupName;
        const etfs = DATA.etfs_meta.filter((e) => e.group === groupName);
        activeEtfCode = etfCode || etfs[0].code;
        if (isAllExpanded) {
          toggleExpandMode(false);
        }
        renderStyleTabs();
        renderSubEtfPills();
        renderSingleInsightAndChart();
      }

      function toggleExpandMode(forceState) {
        isAllExpanded = typeof forceState === "boolean" ? forceState : !isAllExpanded;
        const btn = document.getElementById("btnToggleExpandMode");
        const tabToolbar = document.getElementById("singleTabToolbar");
        const insightStrip = document.getElementById("singleInsightStrip");
        const singleDom = document.getElementById("chartSingle");
        const stackedDom = document.getElementById("stackedChartsContainer");

        if (isAllExpanded) {
          btn.textContent = "🗂️ 切换为：单宽基标签页视图";
          tabToolbar.style.display = "none";
          insightStrip.style.display = "none";
          singleDom.style.display = "none";
          stackedDom.style.display = "flex";
          renderAllStackedCharts();
        } else {
          btn.textContent = "📜 切换为：五大宽基全部展开视图";
          tabToolbar.style.display = "flex";
          insightStrip.style.display = "grid";
          singleDom.style.display = "block";
          stackedDom.style.display = "none";
          chartSingle.resize();
          renderSingleInsightAndChart();
        }
      }

      document.getElementById("btnToggleExpandMode").addEventListener("click", () => toggleExpandMode());

      renderStyleTabs();
      renderSubEtfPills();
      renderSingleInsightAndChart();

      // =========================================================================
      // SECTION 3: ALERT SIGNALS TABLE & FILTERS
      // =========================================================================
      let activeSignalTypeFilter = "ALL";

      // Populate ETF filter dropdown
      const selFilterEtf = document.getElementById("selFilterEtf");
      DATA.etfs_meta.forEach((m) => {
        const opt = document.createElement("option");
        opt.value = m.code;
        opt.textContent = `${m.code} - ${m.name}`;
        selFilterEtf.appendChild(opt);
      });

      function getFilteredSignals() {
        const etfVal = document.getElementById("selFilterEtf").value;
        const grpVal = document.getElementById("selFilterGroup").value;
        const winVal = document.getElementById("selFilterWindow").value;
        const q = document.getElementById("inpSearchSignal").value.trim().toLowerCase();
        const showMulti = document.getElementById("chkShowMultiTags").checked;

        const minDateIdx = winVal === "ALL" ? 0 : Math.max(0, N - parseInt(winVal, 10));

        return allSignals.filter((s) => {
          if (s.dateIdx < minDateIdx) return false;
          if (etfVal !== "ALL" && s.code !== etfVal) return false;
          if (grpVal !== "ALL" && s.group !== grpVal) return false;
          if (activeSignalTypeFilter !== "ALL") {
            if (showMulti) {
              if (!s.tags.some((t) => t.key === activeSignalTypeFilter)) return false;
            } else {
              if (s.primary.key !== activeSignalTypeFilter) return false;
            }
          }
          if (q) {
            const hay = `${s.date} ${s.code} ${s.name} ${s.group} ${s.primary.label}`.toLowerCase();
            if (!hay.includes(q)) return false;
          }
          return true;
        });
      }

      function renderSignalCounters() {
        const winVal = document.getElementById("selFilterWindow").value;
        const minDateIdx = winVal === "ALL" ? 0 : Math.max(0, N - parseInt(winVal, 10));
        const showMulti = document.getElementById("chkShowMultiTags").checked;

        const windowSignals = allSignals.filter((s) => s.dateIdx >= minDateIdx);
        const counts = {
          ALL: windowSignals.length,
          SINGLE_BUY: 0,
          SINGLE_SELL: 0,
          FIVE_DAY_ACC: 0,
          BOTTOM_ACC: 0,
          TOP_DIST: 0,
        };
        windowSignals.forEach((s) => {
          if (showMulti) {
            s.tags.forEach((t) => (counts[t.key] = (counts[t.key] || 0) + 1));
          } else {
            counts[s.primary.key] = (counts[s.primary.key] || 0) + 1;
          }
        });

        const cards = [
          { key: "ALL", label: "📊 全部异动信号", sub: "点击筛选各类型", count: counts.ALL, color: "var(--accent)" },
          {
            key: "BOTTOM_ACC",
            label: SIGNAL_META.BOTTOM_ACC.label,
            sub: SIGNAL_META.BOTTOM_ACC.sub,
            count: counts.BOTTOM_ACC,
            color: "var(--down-green)",
          },
          {
            key: "TOP_DIST",
            label: SIGNAL_META.TOP_DIST.label,
            sub: SIGNAL_META.TOP_DIST.sub,
            count: counts.TOP_DIST,
            color: "#e11d48",
          },
          {
            key: "SINGLE_BUY",
            label: SIGNAL_META.SINGLE_BUY.label,
            sub: SIGNAL_META.SINGLE_BUY.sub,
            count: counts.SINGLE_BUY,
            color: "var(--up-red)",
          },
          {
            key: "SINGLE_SELL",
            label: SIGNAL_META.SINGLE_SELL.label,
            sub: SIGNAL_META.SINGLE_SELL.sub,
            count: counts.SINGLE_SELL,
            color: "var(--warn-amber)",
          },
          {
            key: "FIVE_DAY_ACC",
            label: SIGNAL_META.FIVE_DAY_ACC.label,
            sub: SIGNAL_META.FIVE_DAY_ACC.sub,
            count: counts.FIVE_DAY_ACC,
            color: "var(--purple-fire)",
          },
        ];

        const grid = document.getElementById("signalCountersGrid");
        grid.innerHTML = cards
          .map(
            (c) => `
          <div class="signal-counter-card ${activeSignalTypeFilter === c.key ? "active" : ""}" data-key="${c.key}">
            <div class="sc-title">
              <span>${c.label}</span>
              <span class="sc-sub">${c.sub}</span>
            </div>
            <span class="sc-count" style="color:${c.color}">${c.count}</span>
          </div>
        `
          )
          .join("");

        grid.querySelectorAll(".signal-counter-card").forEach((card) => {
          card.addEventListener("click", () => {
            activeSignalTypeFilter = card.getAttribute("data-key");
            renderSignalCounters();
            renderSignalTable();
          });
        });
      }

      function renderSignalTable() {
        const rows = getFilteredSignals();
        const showMulti = document.getElementById("chkShowMultiTags").checked;
        document.getElementById("signalCountSummary").textContent = `当前筛选共 ${rows.length} 条异动记录`;

        const tbody = document.getElementById("signalTableBody");
        if (!rows.length) {
          tbody.innerHTML = `<tr><td colspan="11" style="text-align:center;padding:28px;color:var(--text-muted);">未匹配到符合条件的预警信号</td></tr>`;
          return;
        }

        // Render up to 350 rows for fast DOM performance
        const displayRows = rows.slice(0, 350);
        tbody.innerHTML = displayRows
          .map((s) => {
            const tagsHtml = showMulti
              ? s.tags.map((t) => `<span class="sig-badge ${t.cls}">${t.label}</span>`).join("")
              : `<span class="sig-badge ${s.primary.cls}">${s.primary.label}</span>`;

            const d1Cls = s.d1 > 0 ? "val-pos" : s.d1 < 0 ? "val-neg" : "";
            const d5Cls = s.d5 > 0 ? "val-pos" : s.d5 < 0 ? "val-neg" : "";

            return `
              <tr data-group="${s.group}" data-code="${s.code}" data-idx="${s.dateIdx}" style="cursor:pointer;">
                <td class="mono-cell">${s.date}</td>
                <td class="mono-cell">${s.code}</td>
                <td style="font-weight:600;">${s.name}</td>
                <td>${s.group}</td>
                <td>${tagsHtml}</td>
                <td class="mono-cell ${d1Cls}">${s.d1 >= 0 ? "" : ""}${s.d1.toFixed(2)}</td>
                <td class="mono-cell ${d5Cls}">${s.d5 >= 0 ? "" : ""}${s.d5.toFixed(2)}</td>
                <td class="mono-cell">${s.pct.toFixed(1)}%</td>
                <td class="mono-cell">${s.share != null ? s.share.toFixed(2) : "-"}</td>
                <td class="mono-cell">${s.idxVal != null ? s.idxVal.toFixed(2) : "-"}</td>
                <td><button class="pill-btn" style="padding:2px 8px;font-size:11.5px;">📍 图表定位</button></td>
              </tr>
            `;
          })
          .join("");

        tbody.querySelectorAll("tr[data-code]").forEach((tr) => {
          tr.addEventListener("click", () => {
            const grp = tr.getAttribute("data-group");
            const code = tr.getAttribute("data-code");
            const dateIdx = parseInt(tr.getAttribute("data-idx"), 10);
            activeGroup = grp;
            activeEtfCode = code;
            if (isAllExpanded) toggleExpandMode(false);
            renderStyleTabs();
            renderSubEtfPills();
            renderSingleInsightAndChart(dateIdx);
            document.getElementById("sec-single").scrollIntoView({ behavior: "smooth", block: "start" });
          });
        });
      }

      renderSignalCounters();
      renderSignalTable();

      ["selFilterEtf", "selFilterGroup", "selFilterWindow", "chkShowMultiTags"].forEach((id) => {
        document.getElementById(id).addEventListener("change", () => {
          renderSignalCounters();
          renderSignalTable();
        });
      });
      document.getElementById("inpSearchSignal").addEventListener("input", renderSignalTable);

      // =========================================================================
      // THRESHOLD MODAL & CSV EXPORT & THEME & LIVE SYNC
      // =========================================================================
      function showToast(msg) {
        const el = document.getElementById("toastMsg");
        el.textContent = msg;
        el.style.display = "block";
        clearTimeout(el._timer);
        el._timer = setTimeout(() => {
          el.style.display = "none";
        }, 3200);
      }

      const modalThresh = document.getElementById("modalThresh");
      function openThreshModal() {
        const tbody = document.getElementById("threshModalBody");
        tbody.innerHTML = DATA.etfs_meta
          .map((m) => {
            const t = thresholds[m.code];
            return `
            <tr data-code="${m.code}">
              <td class="mono-cell">${m.code}</td>
              <td style="font-weight:600;">${m.name}</td>
              <td><input type="number" step="0.5" class="thresh-input inp-s1" value="${t.single_thresh}" /></td>
              <td><input type="number" step="1" class="thresh-input inp-s5" value="${t.five_day_thresh}" /></td>
              <td><input type="number" step="1" class="thresh-input inp-low" value="${t.low_pct}" /></td>
              <td><input type="number" step="1" class="thresh-input inp-high" value="${t.high_pct}" /></td>
            </tr>
          `;
          })
          .join("");
        modalThresh.classList.add("open");
      }

      document.getElementById("btnConfigThresh").addEventListener("click", openThreshModal);
      document.getElementById("btnCloseModal").addEventListener("click", () => modalThresh.classList.remove("open"));
      modalThresh.addEventListener("click", (e) => {
        if (e.target === modalThresh) modalThresh.classList.remove("open");
      });

      document.getElementById("btnResetThresh").addEventListener("click", () => {
        initThresholds(true);
        openThreshModal();
        scanAllSignals();
        renderSnapshotCards();
        renderSingleInsightAndChart();
        renderSignalCounters();
        renderSignalTable();
        showToast("已恢复默认阈值并重新扫描全部信号");
      });

      document.getElementById("btnSaveThresh").addEventListener("click", () => {
        document.querySelectorAll("#threshModalBody tr[data-code]").forEach((tr) => {
          const c = tr.getAttribute("data-code");
          thresholds[c] = {
            single_thresh: parseFloat(tr.querySelector(".inp-s1").value) || 5.0,
            five_day_thresh: parseFloat(tr.querySelector(".inp-s5").value) || 15.0,
            low_pct: parseFloat(tr.querySelector(".inp-low").value) || 20.0,
            high_pct: parseFloat(tr.querySelector(".inp-high").value) || 80.0,
          };
        });
        try {
          localStorage.setItem(STORAGE_KEY, JSON.stringify(thresholds));
        } catch (e) {}
        modalThresh.classList.remove("open");
        scanAllSignals();
        renderSnapshotCards();
        renderSingleInsightAndChart();
        renderSignalCounters();
        renderSignalTable();
        showToast("新阈值已保存，预警信号清单已更新！");
      });

      // CSV Export
      document.getElementById("btnExportCsv").addEventListener("click", () => {
        const rows = getFilteredSignals();
        const header = ["日期", "ETF代码", "ETF名称", "所属宽基", "预警类型", "当日变化(亿)", "5日累计(亿)", "份额分位数(%)", "当日份额(亿)", "对应指数点位"];
        const csvLines = [header.join(",")];
        rows.forEach((s) => {
          csvLines.push(
            [
              s.date,
              s.code,
              s.name,
              s.group,
              s.primary.label,
              s.d1.toFixed(2),
              s.d5.toFixed(2),
              s.pct.toFixed(1) + "%",
              s.share != null ? s.share.toFixed(2) : "",
              s.idxVal != null ? s.idxVal.toFixed(2) : "",
            ].join(",")
          );
        });
        const blob = new Blob(["\uFEFF" + csvLines.join("\n")], { type: "text/csv;charset=utf-8;" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `宽基ETF份额预警信号_${DATA.end_date}.csv`;
        a.click();
        URL.revokeObjectURL(url);
        showToast(`已导出 ${rows.length} 条预警记录到 CSV`);
      });

      // Theme Toggle
      document.getElementById("btnThemeToggle").addEventListener("click", () => {
        const cur = document.documentElement.getAttribute("data-theme");
        const next = cur === "dark" ? "light" : "dark";
        document.documentElement.setAttribute("data-theme", next);
        const startIdx = document.getElementById("chkDynamicRebase").checked
          ? Math.max(0, Math.min(N - 1, Math.floor((overviewZoomStart / 100) * (N - 1))))
          : 0;
        renderOverviewChart(startIdx);
        if (isAllExpanded) renderAllStackedCharts();
        else renderSingleInsightAndChart();
      });

      // Static-site deployment: data is refreshed by the scheduled GitHub Actions workflow.
      document.getElementById("btnSyncLatest").addEventListener("click", () => {
        showToast("本网页由 GitHub Actions 每个交易日自动更新。如需立即更新，请到 GitHub 仓库的 Actions 页面手动运行工作流。");
      });

      window.addEventListener("resize", () => {
        chartOverview.resize();
        chartSingle.resize();
        stackedChartInstances.forEach((inst) => inst.resize());
      });
    })();
  </script>
</body>
</html>
"""


def generate_html(cache: dict) -> str:
    payload = build_frontend_payload(cache)
    payload_json = json.dumps(payload, ensure_ascii=False)
    html_content = HTML_TEMPLATE.replace("__PAYLOAD_JSON__", payload_json)
    with open(HTML_OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"[Build] 已生成交互式网页看板: {HTML_OUT_PATH}")
    return HTML_OUT_PATH


class TrackerRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE_DIR, **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            self.path = "/etf_share_tracker.html"
        return super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/sync":
            try:
                cache = sync_data(force_full=False)
                generate_html(cache)
                body = json.dumps(
                    {
                        "ok": True,
                        "start_date": cache["start_date"],
                        "end_date": cache["end_date"],
                        "total_days": cache["total_days"],
                        "updated_at": cache["updated_at"],
                    },
                    ensure_ascii=False,
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                body = json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()


def main():
    parser = argparse.ArgumentParser(description="宽基ETF份额追踪与资金底背离监测系统")
    parser.add_argument("--serve", action="store_true", help="启动本地Web服务器")
    parser.add_argument("--port", type=int, default=8765, help="本地Web服务器端口 (默认: 8765)")
    parser.add_argument("--force-full", action="store_true", help="强制重新拉取全量历史数据")
    args = parser.parse_args()

    cache = sync_data(force_full=args.force_full)
    generate_html(cache)

    if args.serve:
        addr = ("127.0.0.1", args.port)
        httpd = ThreadingHTTPServer(addr, TrackerRequestHandler)
        print(f"[Server] 本地交互服务已启动: http://127.0.0.1:{args.port}")
        httpd.serve_forever()


if __name__ == "__main__":
    main()
