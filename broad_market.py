# -*- coding: utf-8 -*-
"""沪深交易所指定宽基ETF份额采集与系列聚合。

仅追踪固定代码白名单中的35只ETF。
份额单位统一为亿份；原始缓存保留万份。
分类完全依据ETF代码，不依赖基金简称或关键词。
"""

import io
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
import zipfile

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CACHE_PATH = os.path.join(DATA_DIR, "broad_etf_cache.json")

START_DATE = "2024-06-03"

# 白名单或口径更新后递增版本号，触发历史数据重新采集
CLASSIFICATION_VERSION = 6

SSE_URL = "https://query.sse.com.cn/commonQuery.do"
SZSE_URL = "https://www.szse.cn/api/report/ShowReport"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.sse.com.cn/",
}

GROUPS = [
    {"id": "50", "name": "上证50", "color": "#f08c00"},
    {"id": "300", "name": "沪深300", "color": "#1098ad"},
    {"id": "500", "name": "中证500", "color": "#7950f2"},
    {"id": "1000", "name": "中证1000", "color": "#40a02b"},
    {"id": "A500", "name": "中证A500", "color": "#d6336c"},
    {"id": "STAR", "name": "科创宽基", "color": "#1c7ed6"},
    {"id": "CHINEXT", "name": "创业板", "color": "#e03131"},
]

GROUP_IDS = {g["id"] for g in GROUPS}


# ============================================================
# 一、固定代码白名单：共35只
# ============================================================

CODE_LISTS = {
    # 上证50：3只
    "50": [
        "510100",
        "510050",
        "530000",
    ],
    # 沪深300：6只
    "300": [
        "510300",
        "510330",
        "159919",
        "510310",
        "515330",
        "510360",
    ],
    # 中证500：3只
    "500": [
        "510500",
        "159922",
        "512500",
    ],
    # 中证1000：5只
    "1000": [
        "512100",
        "159845",
        "560010",
        "159629",
        "159633",
    ],
    # 中证A500：8只
    "A500": [
        "563360",
        "159352",
        "159361",
        "159338",
        "512050",
        "563800",
        "159351",
        "563220",
    ],
    # 科创宽基：5只
    "STAR": [
        "588080",
        "588050",
        "588000",
        "588060",
        "588220",
    ],
    # 创业板：5只
    "CHINEXT": [
        "159915",
        "159949",
        "159952",
        "159977",
        "159682",
    ],
}

EXPECTED_COUNTS = {
    "50": 3,
    "300": 6,
    "500": 3,
    "1000": 5,
    "A500": 8,
    "STAR": 5,
    "CHINEXT": 5,
}

EXPECTED_TOTAL = 35


def _build_code_map():
    """构建代码到系列的映射，并验证白名单。"""
    code_map = {}

    for group_id, codes in CODE_LISTS.items():
        if group_id not in GROUP_IDS:
            raise ValueError(f"白名单包含未知系列：{group_id}")

        if len(codes) != EXPECTED_COUNTS[group_id]:
            raise ValueError(
                f"{group_id}白名单数量错误："
                f"预期{EXPECTED_COUNTS[group_id]}，实际{len(codes)}"
            )

        for code in codes:
            norm_code = str(code).strip().zfill(6)
            if len(norm_code) != 6 or not norm_code.isdigit():
                raise ValueError(f"ETF代码格式错误：{code}")
            if norm_code in code_map:
                raise ValueError(f"ETF代码重复：{norm_code}")
            code_map[norm_code] = group_id

    if len(code_map) != EXPECTED_TOTAL:
        raise ValueError(
            f"白名单总数错误：预期{EXPECTED_TOTAL}，实际{len(code_map)}"
        )

    return code_map


CODE_TO_GROUP = _build_code_map()
ALL_CODES = set(CODE_TO_GROUP)


# ============================================================
# 二、ETF分类函数
# ============================================================

def classify(name=None, code=None):
    """仅根据ETF代码确定所属系列；白名单外返回 None。"""
    norm_code = str(code or "").strip().zfill(6)
    if len(norm_code) != 6 or not norm_code.isdigit():
        return None
    return CODE_TO_GROUP.get(norm_code)


# ============================================================
# 三、通用数据处理
# ============================================================

def _num(v):
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None


def _request_json(url, params, referer=None, verify=True):
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer

    last_err = None
    for attempt in range(4):
        try:
            resp = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=25,
                verify=verify,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_err = exc
            time.sleep(0.6 * (attempt + 1))

    raise RuntimeError(str(last_err))


# ============================================================
# 四、上海证券交易所数据
# ============================================================

def _sse_date(day):
    js = _request_json(
        SSE_URL,
        {
            "isPagination": "true",
            "pageHelp.pageSize": "10000",
            "pageHelp.pageNo": "1",
            "STAT_DATE": day,
            "sqlId": "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L",
        },
        "https://www.sse.com.cn/",
    )

    rows = js.get("result", []) if isinstance(js, dict) else []
    out = {}

    for x in rows:
        code = str(x.get("SEC_CODE") or "").strip().zfill(6)
        group = classify(code=code)
        if group not in GROUP_IDS:
            continue

        name = (
            x.get("FUND_EXPANSION_ABBR")
            or x.get("SEC_NAME")
            or x.get("FUND_ABBR")
            or ""
        )

        shares = _num(x.get("TOT_VOL"))
        if shares is None or shares < 0:
            continue

        out[code] = {
            "code": code,
            "name": name,
            "exchange": "SSE",
            "group": group,
            "shares_10k": shares,
        }

    return out


# ============================================================
# 五、深圳证券交易所数据
# ============================================================

def _xlsx_column_index(cell_ref):
    letters = re.match(r"[A-Z]+", str(cell_ref or "").upper())
    if not letters:
        return 0
    value = 0
    for ch in letters.group(0):
        value = value * 26 + ord(ch) - ord("A") + 1
    return value - 1


def _xlsx_cell_value(cell, shared_strings):
    cell_type = cell.attrib.get("t", "")
    value_node = cell.find("{*}v")

    if cell_type == "inlineStr":
        node = cell.find(".//{*}t")
        return node.text.strip() if node is not None and node.text else ""

    if value_node is None or value_node.text is None:
        return ""

    value = value_node.text
    if cell_type == "s":
        try:
            return shared_strings[int(value)]
        except (ValueError, IndexError):
            return ""

    return str(value).strip()


def _read_szse_xlsx(content):
    if not content or not content.startswith(b"PK"):
        preview = (content or b"")[:160].decode("utf-8", errors="replace")
        raise RuntimeError("深交所返回内容不是有效XLSX文件：" + preview)

    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            names = set(zf.namelist())
            shared_strings = []
            if "xl/sharedStrings.xml" in names:
                root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
                for item in root.findall("a:si", ns):
                    shared_strings.append(
                        "".join(
                            node.text or ""
                            for node in item.findall(".//a:t", ns)
                        )
                    )

            sheet_path = "xl/worksheets/sheet1.xml"
            if sheet_path not in names:
                candidates = sorted(
                    n
                    for n in names
                    if n.startswith("xl/worksheets/sheet") and n.endswith(".xml")
                )
                if not candidates:
                    raise RuntimeError("XLSX中没有工作表")
                sheet_path = candidates[0]

            root = ET.fromstring(zf.read(sheet_path))
            rows = []

            for row_node in root.findall(".//a:sheetData/a:row", ns):
                cells = {}
                for cell in row_node.findall("a:c", ns):
                    idx = _xlsx_column_index(cell.attrib.get("r", ""))
                    cells[idx] = _xlsx_cell_value(cell, shared_strings)

                if cells:
                    max_idx = max(cells)
                    rows.append([cells.get(i, "") for i in range(max_idx + 1)])

            return rows

    except zipfile.BadZipFile as exc:
        raise RuntimeError("深交所XLSX文件损坏：" + str(exc))
    except ET.ParseError as exc:
        raise RuntimeError("深交所XLSX XML解析失败：" + str(exc))


def _szse_window(start, end):
    params = {
        "SHOWTYPE": "xlsx",
        "CATALOGID": "scsj_fund_jjgm",
        "TABKEY": "tab1",
        "txtStart": start,
        "txtEnd": end,
        "jjlb": "ETF",
        "random": str(time.time()),
    }

    headers = dict(HEADERS)
    headers["Referer"] = "https://www.szse.cn/market/fund/volume/etf/index.html"

    last_err = None
    for attempt in range(4):
        try:
            response = requests.get(
                SZSE_URL,
                params=params,
                headers=headers,
                timeout=45,
                verify=False,
            )
            response.raise_for_status()

            rows = _read_szse_xlsx(response.content)
            if not rows:
                return []

            header = [str(x).strip().replace(" ", "") for x in rows[0]]
            if len(header) < 4:
                raise RuntimeError(
                    "深交所报表列数不足，实际表头：" + repr(header)
                )

            all_rows = []
            for row in rows[1:]:
                if len(row) < 4 or not any(str(x).strip() for x in row):
                    continue
                day, code, name, shares = row[:4]
                all_rows.append(
                    {
                        "size_date": day,
                        "fund_code": code,
                        "security_short_name": name,
                        "current_size": shares,
                    }
                )

            return all_rows

        except Exception as exc:
            last_err = exc
            if attempt < 3:
                time.sleep(0.8 * (attempt + 1))

    raise RuntimeError(str(last_err))


def _record_date(row):
    v = (
        row.get("size_date")
        or row.get("stat_date")
        or row.get("date")
        or row.get("日期")
        or ""
    )
    value = str(v).strip().replace("/", "-").replace(".", "-")

    if re.fullmatch(r"\d{8}", value):
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"

    if value.isdigit() and 30000 <= int(value) <= 80000:
        excel_day = datetime(1899, 12, 30) + timedelta(days=int(value))
        return excel_day.strftime("%Y-%m-%d")

    return value[:10]


def _szse_history(start, end):
    by_date = {}
    failed_ranges = []
    successful_ranges = []

    cur = date.fromisoformat(start)
    end_d = date.fromisoformat(end)

    while cur <= end_d:
        stop = min(cur + timedelta(days=89), end_d)
        range_start = cur.isoformat()
        range_end = stop.isoformat()

        try:
            rows = _szse_window(range_start, range_end)
            successful_ranges.append([range_start, range_end])
        except Exception as exc:
            failed_ranges.append([range_start, range_end, str(exc)])
            rows = []

        for x in rows:
            day = _record_date(x)
            code = str(
                x.get("fund_code")
                or x.get("fundCode")
                or x.get("证券代码")
                or ""
            ).strip()

            if code.endswith(".0") and code[:-2].isdigit():
                code = code[:-2]

            code = code.zfill(6)
            group = classify(code=code)
            if group not in GROUP_IDS:
                continue

            name = (
                x.get("security_short_name")
                or x.get("fund_name")
                or x.get("证券简称")
                or ""
            )

            shares = _num(
                x.get("current_size")
                or x.get("fund_size")
                or x.get("基金规模")
            )

            if len(day) != 10 or shares is None or shares < 0:
                continue

            by_date.setdefault(day, {})[code] = {
                "code": code,
                "name": name,
                "exchange": "SZSE",
                "group": group,
                "shares_10k": shares,
            }

        cur = stop + timedelta(days=1)
        time.sleep(0.35)

    return by_date, failed_ranges, successful_ranges


# ============================================================
# 六、历史缓存重新分类
# ============================================================

def _reclassify_cache(old_by_date):
    result = {}
    if not isinstance(old_by_date, dict):
        return result

    for day, rows in old_by_date.items():
        if not isinstance(rows, dict):
            continue

        new_rows = {}
        for code, rec in rows.items():
            if not isinstance(rec, dict):
                continue

            norm_code = str(code).strip().zfill(6)
            group = classify(code=norm_code)
            if group not in GROUP_IDS:
                continue

            new_rec = dict(rec)
            new_rec["code"] = norm_code
            new_rec["group"] = group
            new_rows[norm_code] = new_rec

        if new_rows:
            result[day] = new_rows

    return result


# ============================================================
# 七、同步数据
# ============================================================

def sync_broad_market(force_full=False):
    os.makedirs(DATA_DIR, exist_ok=True)

    cache = {}
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, encoding="utf-8") as f:
                cache = json.load(f)
        except Exception:
            cache = {}

    old_by_date = cache.get("by_date", {})
    reclassified = _reclassify_cache(old_by_date)

    version_changed = cache.get("classification_version") != CLASSIFICATION_VERSION
    full_refresh = force_full or version_changed

    if full_refresh:
        print(
            "检测到白名单或口径更新，开始重新采集指定35只ETF历史数据。",
            flush=True,
        )
        by_date = {}
        prev_partial = set()
    else:
        by_date = reclassified
        prev_partial = {
            d for d in cache.get("partial_dates", []) if isinstance(d, str) and len(d) == 10
        }

    today = date.today()
    start_floor = date.fromisoformat(START_DATE)

    if full_refresh or not by_date:
        start = start_floor
    else:
        start = max(start_floor, today - timedelta(days=8))
        if prev_partial:
            min_partial = min(date.fromisoformat(d) for d in prev_partial if d >= START_DATE)
            start = min(start, max(start_floor, min_partial))

    end = today
    errors = []
    partial_dates = set(prev_partial)
    sse_failed_dates = set()

    # --------------------------------------------------------
    # SSE：按日历日期查询
    # --------------------------------------------------------
    days = []
    d = start
    while d <= end:
        days.append(d.isoformat())
        d += timedelta(days=1)

    def sse_job(day):
        try:
            return day, _sse_date(day), None
        except Exception as exc:
            return day, {}, str(exc)

    workers = 8 if len(days) > 30 else 4
    print(f"开始采集上交所数据，共{len(days)}个日历日期。", flush=True)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(sse_job, x) for x in days]
        completed = 0

        for fut in as_completed(futures):
            day, rows, err = fut.result()
            completed += 1

            if err is None:
                existing = by_date.setdefault(day, {})
                for code in list(existing):
                    if existing[code].get("exchange") == "SSE":
                        existing.pop(code, None)
                existing.update(rows)
                partial_dates.discard(day)
            else:
                errors.append(f"SSE {day}: {err}")
                partial_dates.add(day)
                sse_failed_dates.add(day)

            if completed % 20 == 0 or completed == len(days):
                print(f"SSE进度：{completed}/{len(days)}", flush=True)

    # --------------------------------------------------------
    # SZSE：按90天时间窗口查询
    # --------------------------------------------------------
    print("开始采集深交所历史数据。", flush=True)

    try:
        sz, failed_ranges, successful_ranges = _szse_history(
            start.isoformat(),
            end.isoformat(),
        )

        for a, b in successful_ranges:
            cur = date.fromisoformat(a)
            last = date.fromisoformat(b)
            while cur <= last:
                day = cur.isoformat()
                existing = by_date.get(day, {})
                for code in list(existing):
                    if existing[code].get("exchange") == "SZSE":
                        existing.pop(code, None)
                cur += timedelta(days=1)

        for day, rows in sz.items():
            by_date.setdefault(day, {}).update(rows)

        for item in failed_ranges:
            a, b = item[0], item[1]
            reason = item[2] if len(item) > 2 else "request failed"
            errors.append(f"SZSE {a}~{b}: {reason}")
            cur = date.fromisoformat(a)
            last = date.fromisoformat(b)
            while cur <= last:
                partial_dates.add(cur.isoformat())
                cur += timedelta(days=1)

        for a, b in successful_ranges:
            cur = date.fromisoformat(a)
            last = date.fromisoformat(b)
            while cur <= last:
                day = cur.isoformat()
                if day not in sse_failed_dates:
                    partial_dates.discard(day)
                cur += timedelta(days=1)

    except Exception as exc:
        errors.append("SZSE history: " + str(exc))
        cur = start
        while cur <= end:
            partial_dates.add(cur.isoformat())
            cur += timedelta(days=1)

    # --------------------------------------------------------
    # 全量刷新时若部分日期失败，保留原有缓存兜底
    # --------------------------------------------------------
    if full_refresh:
        for day in partial_dates:
            old_rows = reclassified.get(day, {})
            if old_rows:
                by_date.setdefault(day, {}).update(old_rows)

    # --------------------------------------------------------
    # 最终清理：严格执行35只ETF白名单
    # --------------------------------------------------------
    clean = {}
    for day, rows in by_date.items():
        if not isinstance(rows, dict):
            continue

        selected = {}
        for code, rec in rows.items():
            if not isinstance(rec, dict):
                continue

            norm_code = str(code).strip().zfill(6)
            group = classify(code=norm_code)
            if group not in GROUP_IDS:
                continue

            new_rec = dict(rec)
            new_rec["code"] = norm_code
            new_rec["group"] = group
            selected[norm_code] = new_rec

        if selected:
            clean[day] = selected

    clean = dict(sorted(clean.items()))
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cache = {
        "classification_version": CLASSIFICATION_VERSION,
        "updated_at": updated_at,
        "start_date": min(clean) if clean else START_DATE,
        "end_date": max(clean) if clean else "",
        "by_date": clean,
        "groups": GROUPS,
        "partial_dates": sorted(partial_dates),
        "last_attempt": datetime.now().isoformat(timespec="seconds"),
        "errors": errors[-30:],
        "source": "上海证券交易所、深圳证券交易所公开ETF份额披露",
    }

    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    print(
        f"数据采集完成：{len(clean)}个有数据日期；"
        f"白名单ETF数量：{len(ALL_CODES)}；"
        f"分类版本：{CLASSIFICATION_VERSION}；"
        f"异常日期：{len(partial_dates)}",
        flush=True,
    )

    return cache


# ============================================================
# 八、生成网页数据
# ============================================================

def build_broad_payload(cache):
    dates = sorted(cache.get("by_date", {}))
    groups = []
    all_observed_codes = set()
    expected_counts = {}

    for g in GROUPS:
        expected_codes = sorted(CODE_LISTS[g["id"]])
        expected_counts[g["id"]] = len(expected_codes)

        members = {}
        for day in dates:
            for code, rec in cache["by_date"].get(day, {}).items():
                norm_code = str(code).zfill(6)
                if rec.get("group") == g["id"] and norm_code in CODE_TO_GROUP:
                    members[norm_code] = rec
                    all_observed_codes.add(norm_code)

        observed_codes = sorted(members)
        missing_codes = sorted(set(expected_codes) - set(observed_codes))

        series = []
        counts = []
        missing = []

        for day in dates:
            rows = [
                r
                for code, r in cache["by_date"].get(day, {}).items()
                if (
                    str(code).zfill(6) in CODE_TO_GROUP
                    and CODE_TO_GROUP[str(code).zfill(6)] == g["id"]
                )
            ]

            if rows:
                series.append(round(sum(r["shares_10k"] for r in rows) / 10000, 4))
            else:
                series.append(None)
                missing.append(day)

            counts.append(len(rows))

        latest_idx = next(
            (i for i in range(len(dates) - 1, -1, -1) if series[i] is not None),
            None,
        )
        prev_idx = (
            next(
                (i for i in range(latest_idx - 1, -1, -1) if series[i] is not None),
                None,
            )
            if latest_idx is not None
            else None
        )

        latest = series[latest_idx] if latest_idx is not None else None
        prev = series[prev_idx] if prev_idx is not None else None

        details = []
        if latest_idx is not None:
            day = dates[latest_idx]
            for code, rec in cache["by_date"].get(day, {}).items():
                norm_code = str(code).zfill(6)
                if norm_code in CODE_TO_GROUP and CODE_TO_GROUP[norm_code] == g["id"]:
                    details.append(
                        {
                            "code": norm_code,
                            "name": rec.get("name", ""),
                            "exchange": rec.get("exchange", ""),
                            "shares": round(rec["shares_10k"] / 10000, 4),
                        }
                    )

        details.sort(key=lambda x: x["shares"], reverse=True)

        groups.append(
            {
                **g,
                "codes": observed_codes,
                "expected_count": len(expected_codes),
                "expected_codes": expected_codes,
                "observed_count": len(observed_codes),
                "missing_codes": missing_codes,
                "count": len(expected_codes),
                "actual_count": len(details),
                "latest_missing_codes": sorted(
                    set(expected_codes) - {item["code"] for item in details}
                ),
                "series": series,
                "counts": counts,
                "latest": latest,
                "change1": (
                    round(latest - prev, 4)
                    if latest is not None and prev is not None
                    else None
                ),
                "details": details,
                "missing_days": missing,
            }
        )

    total_series = []
    total_count = []

    for day in dates:
        rows = [
            r
            for code, r in cache["by_date"].get(day, {}).items()
            if (
                str(code).zfill(6) in CODE_TO_GROUP
                and r.get("group") in GROUP_IDS
            )
        ]

        if rows:
            total_series.append(round(sum(r["shares_10k"] for r in rows) / 10000, 4))
        else:
            total_series.append(None)

        total_count.append(len(rows))

    latest_idx = next(
        (i for i in range(len(dates) - 1, -1, -1) if total_series[i] is not None),
        None,
    )
    prev_idx = (
        next(
            (i for i in range(latest_idx - 1, -1, -1) if total_series[i] is not None),
            None,
        )
        if latest_idx is not None
        else None
    )

    latest = total_series[latest_idx] if latest_idx is not None else None
    prev = total_series[prev_idx] if prev_idx is not None else None

    observed_total = len(all_observed_codes)
    missing_total_codes = sorted(ALL_CODES - all_observed_codes)

    return {
        "dates": dates,
        "groups": groups,
        "total_series": total_series,
        "total_count": total_count,
        "total_latest": latest,
        "total_change1": (
            round(latest - prev, 4)
            if latest is not None and prev is not None
            else None
        ),
        "expected_total": EXPECTED_TOTAL,
        "observed_total": observed_total,
        "missing_codes": missing_total_codes,
        "expected_counts": expected_counts,
        "updated_at": cache.get("updated_at"),
        "source": cache.get("source"),
        "errors": cache.get("errors", []),
        "partial_dates": cache.get("partial_dates", []),
        "start_date": cache.get("start_date"),
        "end_date": cache.get("end_date"),
        "note": (
            "单位：亿份（非资产规模亿元，低净值宽基份额可能高于高净值沪深300）。"
            "本页面仅统计固定代码白名单中的35只指定宽基ETF。"
            "七大系列分别为上证50、沪深300、中证500、"
            "中证1000、中证A500、科创宽基及创业板。"
            "不自动纳入同系列其他ETF。"
            "分类完全依据ETF代码，不依赖基金简称。"
            "实际披露数量以交易所数据为准。"
            "缺失日不以前值填充。"
            "全市场合计指本页35只指定ETF合计。"
        ),
    }
