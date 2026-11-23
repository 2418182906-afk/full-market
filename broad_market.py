
# -*- coding: utf-8 -*-
"""沪深交易所指定宽基ETF份额采集与系列聚合。

仅追踪固定代码白名单中的35只ETF。
份额单位统一为亿份；原始缓存保留万份。
分类完全依据ETF代码，不依赖基金简称或关键词。
"""

import json
import os
import time
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
CACHE_PATH = os.path.join(DATA_DIR, 'broad_etf_cache.json')

START_DATE = '2024-06-03'

# 白名单更新后递增版本号，触发历史数据重新采集
CLASSIFICATION_VERSION = 4

SSE_URL = 'https://query.sse.com.cn/commonQuery.do'
SZSE_URL = 'https://www.szse.cn/api/report/ShowReport/data'

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
    '300': 6,
    '500': 3,
    '1000': 5,
    'A500': 8,
    'STAR': 5,
    'CHINEXT': 5,
}

EXPECTED_TOTAL = 35


def _build_code_map():
    """构建代码到系列的映射，并验证白名单。"""

    code_map = {}

    for group_id, codes in CODE_LISTS.items():

        if group_id not in GROUP_IDS:
            raise ValueError(
                f'白名单包含未知系列：{group_id}'
            )

        if len(codes) != EXPECTED_COUNTS[group_id]:
            raise ValueError(
                f'{group_id}白名单数量错误：'
                f'预期{EXPECTED_COUNTS[group_id]}，'
                f'实际{len(codes)}'
            )

        for code in codes:

            code = str(code).strip().zfill(6)

            if len(code) != 6 or not code.isdigit():
                raise ValueError(
                    f'ETF代码格式错误：{code}'
                )

            if code in code_map:
                raise ValueError(
                    f'ETF代码重复：{code}'
                )

            code_map[code] = group_id

    if len(code_map) != EXPECTED_TOTAL:
        raise ValueError(
            f'白名单总数错误：预期{EXPECTED_TOTAL}，'
            f'实际{len(code_map)}'
        )

    return code_map


CODE_TO_GROUP = _build_code_map()
ALL_CODES = set(CODE_TO_GROUP)


# ============================================================
# 二、ETF分类函数
# ============================================================

def classify(name=None, code=None):
    """
    仅根据ETF代码确定所属系列。

    白名单内：返回对应系列ID。
    白名单外：返回None。

    name参数仅为兼容原有调用方式保留，
    不参与分类判断。
    """

    code = str(code or '').strip().zfill(6)

    if len(code) != 6 or not code.isdigit():
        return None

    return CODE_TO_GROUP.get(code)


# ============================================================
# 三、通用数据处理
# ============================================================

def _num(v):

    if v is None:
        return None

    try:
        return float(str(v).replace(',', '').strip())

    except Exception:
        return None


def _request_json(
    url,
    params,
    referer=None,
    verify=True
):

    h = dict(HEADERS)

    if referer:
        h['Referer'] = referer

    last = None

    for k in range(4):

        try:

            r = requests.get(
                url,
                params=params,
                headers=h,
                timeout=25,
                verify=verify
            )

            r.raise_for_status()
            return r.json()

        except Exception as e:

            last = e
            time.sleep(0.6 * (k + 1))

    raise RuntimeError(str(last))


# ============================================================
# 四、上海证券交易所数据
# ============================================================

def _sse_date(day):

    js = _request_json(
        SSE_URL,
        {
            'isPagination': 'false',
            'STAT_DATE': day,
            'sqlId': 'COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L'
        },
        'https://www.sse.com.cn/'
    )

    rows = js.get('result', []) if isinstance(js, dict) else []

    out = {}

    for x in rows:

        code = str(
            x.get('SEC_CODE') or ''
        ).strip().zfill(6)

        group = classify(code=code)

        if group not in GROUP_IDS:
            continue

        name = (
            x.get('FUND_EXPANSION_ABBR')
            or x.get('SEC_NAME')
            or x.get('FUND_ABBR')
            or ''
        )

        shares = _num(x.get('TOT_VOL'))

        if shares is None or shares < 0:
            continue

        out[code] = {
            'code': code,
            'name': name,
            'exchange': 'SSE',
            'group': group,
            'shares_10k': shares
        }

    return out


# ============================================================
# 五、深圳证券交易所数据
# ============================================================

def _extract_sz_rows(js):

    if isinstance(js, list):

        for item in js:

            if (
                isinstance(item, dict)
                and isinstance(item.get('data'), list)
            ):

                return (
                    item['data'],
                    item.get('metadata', {})
                )

        return [], {}

    if isinstance(js, dict):

        if isinstance(js.get('data'), list):

            return (
                js['data'],
                js.get('metadata', {})
            )

        if (
            isinstance(js.get('data'), dict)
            and isinstance(js['data'].get('data'), list)
        ):

            return (
                js['data']['data'],
                js['data'].get('metadata', {})
            )

    return [], {}


def _szse_window(start, end):

    all_rows = []

    for page in range(1, 101):

        params = {
            'CATALOGID': 'scsj_fund_jjgm',
            'txtStart': start,
            'txtEnd': end,
            'txtDm': '',
            'jjlb': 'ETF',
            'tab1PAGENUM': str(page),
            'tab1PAGECOUNT': '20',
            'pageNum': str(page),
            'pageSize': '20'
        }

        js = _request_json(
            SZSE_URL,
            params,
            'https://www.szse.cn/market/fund/volume/etf/index.html',
            verify=False
        )

        rows, meta = _extract_sz_rows(js)

        if not rows:
            break

        all_rows.extend(rows)

        total = int(
            _num(
                meta.get('recordcount')
                or meta.get('recordCount')
                or meta.get('total')
            ) or 0
        )

        if total and len(all_rows) >= total:
            break

        if len(rows) < 20:
            break

        time.sleep(0.25)

    return all_rows


def _record_date(row):

    v = (
        row.get('size_date')
        or row.get('stat_date')
        or row.get('date')
        or row.get('日期')
        or ''
    )

    return str(v)[:10].replace('/', '-')


def _szse_history(start, end):

    """
    按90天窗口采集深交所历史数据。

    返回：
    1. by_date：采集到的数据
    2. failed_ranges：请求失败的日期区间
    3. successful_ranges：请求成功的日期区间
    """

    by_date = {}
    failed_ranges = []
    successful_ranges = []

    cur = date.fromisoformat(start)
    end_d = date.fromisoformat(end)

    while cur <= end_d:

        stop = min(
            cur + timedelta(days=89),
            end_d
        )

        range_start = cur.isoformat()
        range_end = stop.isoformat()

        try:

            rows = _szse_window(
                range_start,
                range_end
            )

            successful_ranges.append([
                range_start,
                range_end
            ])

        except Exception:

            failed_ranges.append([
                range_start,
                range_end
            ])

            rows = []

        for x in rows:

            day = _record_date(x)

            code = str(
                x.get('fund_code')
                or x.get('fundCode')
                or x.get('证券代码')
                or ''
            ).strip().zfill(6)

            group = classify(code=code)

            if group not in GROUP_IDS:
                continue

            name = (
                x.get('security_short_name')
                or x.get('fund_name')
                or x.get('证券简称')
                or ''
            )

            shares = _num(
                x.get('current_size')
                or x.get('fund_size')
                or x.get('基金规模')
            )

            if (
                len(day) != 10
                or shares is None
                or shares < 0
            ):
                continue

            by_date.setdefault(day, {})[code] = {
                'code': code,
                'name': name,
                'exchange': 'SZSE',
                'group': group,
                'shares_10k': shares
            }

        cur = stop + timedelta(days=1)

        time.sleep(0.35)

    return by_date, failed_ranges, successful_ranges


# ============================================================
# 六、历史缓存重新分类
# ============================================================

def _reclassify_cache(old_by_date):

    """使用当前35只ETF白名单重新整理历史缓存。"""

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

            code = str(code).strip().zfill(6)

            group = classify(code=code)

            if group not in GROUP_IDS:
                continue

            new_rec = dict(rec)

            new_rec['code'] = code
            new_rec['group'] = group

            new_rows[code] = new_rec

        if new_rows:
            result[day] = new_rows

    return result


# ============================================================
# 七、同步数据
# ============================================================

def sync_broad_market(force_full=False):

    os.makedirs(DATA_DIR, exist_ok=True)

    cache = {
        'by_date': {},
        'last_attempt': '',
        'errors': [],
        'source': 'SSE+SZSE official ETF share disclosures'
    }

    if os.path.exists(CACHE_PATH):

        try:

            with open(CACHE_PATH, encoding='utf-8') as f:
                cache = json.load(f)

        except Exception:
            pass

    old_by_date = cache.get('by_date', {})

    # 按最新白名单重新整理旧缓存
    reclassified = _reclassify_cache(old_by_date)

    version_changed = (
        cache.get('classification_version')
        != CLASSIFICATION_VERSION
    )

    full_refresh = force_full or version_changed

    if full_refresh:

        print(
            '检测到白名单更新，开始重新采集指定35只ETF历史数据。',
            flush=True
        )

        by_date = {}

    else:

        by_date = reclassified

    today = date.today()

    if full_refresh or not by_date:

        start = date.fromisoformat(START_DATE)

    else:

        start = max(
            date.fromisoformat(START_DATE),
            today - timedelta(days=8)
        )

    end = today

    errors = []

    partial_dates = (
        set(cache.get('partial_dates', []))
        if not full_refresh
        else set()
    )

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

        except Exception as e:

            return day, {}, str(e)

    workers = 8 if len(days) > 30 else 4

    print(
        f'开始采集上交所数据，共{len(days)}个日历日期。',
        flush=True
    )

    with ThreadPoolExecutor(max_workers=workers) as pool:

        futures = [
            pool.submit(sse_job, x)
            for x in days
        ]

        completed = 0

        for f in as_completed(futures):

            day, rows, err = f.result()

            completed += 1

            if err is None:

                # 请求成功时，先清除当天旧的SSE记录。
                # 即使本次没有披露数据，也不保留过期记录。
                existing = by_date.setdefault(day, {})

                for code in list(existing):

                    if existing[code].get('exchange') == 'SSE':
                        existing.pop(code, None)

                existing.update(rows)

                partial_dates.discard(day)

            else:

                errors.append(
                    'SSE ' + day + ': ' + err
                )

                partial_dates.add(day)
                sse_failed_dates.add(day)

            if (
                completed % 20 == 0
                or completed == len(days)
            ):

                print(
                    f'SSE进度：{completed}/{len(days)}',
                    flush=True
                )

    # --------------------------------------------------------
    # SZSE：按90天时间窗口查询
    # --------------------------------------------------------

    print(
        '开始采集深交所历史数据。',
        flush=True
    )

    try:

        (
            sz,
            failed_ranges,
            successful_ranges
        ) = _szse_history(
            start.isoformat(),
            end.isoformat()
        )

        # 成功区间清除旧的SZSE记录
        for a, b in successful_ranges:

            d = date.fromisoformat(a)
            last = date.fromisoformat(b)

            while d <= last:

                day = d.isoformat()
                existing = by_date.get(day, {})

                for code in list(existing):

                    if existing[code].get('exchange') == 'SZSE':
                        existing.pop(code, None)

                d += timedelta(days=1)

        # 写入本次成功采集的数据
        for day, rows in sz.items():

            by_date.setdefault(day, {}).update(rows)

        # 标记深交所请求失败日期
        for a, b in failed_ranges:

            errors.append(
                'SZSE ' + a + '~' + b + ': request failed'
            )

            d = date.fromisoformat(a)
            last = date.fromisoformat(b)

            while d <= last:

                day = d.isoformat()

                partial_dates.add(day)
                d += timedelta(days=1)

        # 成功日期只有在SSE也成功时才解除异常标记
        for a, b in successful_ranges:

            d = date.fromisoformat(a)
            last = date.fromisoformat(b)

            while d <= last:

                day = d.isoformat()

                if day not in sse_failed_dates:
                    partial_dates.discard(day)

                d += timedelta(days=1)

    except Exception as e:

        errors.append(
            'SZSE history: ' + str(e)
        )

        d = start

        while d <= end:

            partial_dates.add(d.isoformat())
            d += timedelta(days=1)

    # --------------------------------------------------------
    # 请求失败的日期保留旧缓存
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

            code = str(code).strip().zfill(6)

            group = classify(code=code)

            if group not in GROUP_IDS:
                continue

            new_rec = dict(rec)

            new_rec['code'] = code
            new_rec['group'] = group

            selected[code] = new_rec

        if selected:
            clean[day] = selected

    clean = dict(sorted(clean.items()))

    updated_at = datetime.now().strftime(
        '%Y-%m-%d %H:%M:%S'
    )

    cache = {
        'classification_version': CLASSIFICATION_VERSION,
        'updated_at': updated_at,
        'start_date': min(clean) if clean else START_DATE,
        'end_date': max(clean) if clean else '',
        'by_date': clean,
        'groups': GROUPS,
        'partial_dates': sorted(partial_dates),
        'last_attempt': datetime.now().isoformat(
            timespec='seconds'
        ),
        'errors': errors[-30:],
        'source': '上海证券交易所、深圳证券交易所公开ETF份额披露'
    }

    with open(CACHE_PATH, 'w', encoding='utf-8') as f:

        json.dump(
            cache,
            f,
            ensure_ascii=False,
            indent=2
        )

    print(
        f'数据采集完成：{len(clean)}个有数据日期。',
        flush=True
    )

    print(
        f'白名单ETF数量：{len(ALL_CODES)}',
        flush=True
    )

    print(
        f'分类版本：{CLASSIFICATION_VERSION}',
        flush=True
    )

    print(
        f'异常日期：{len(partial_dates)}',
        flush=True
    )

    return cache


# ============================================================
# 八、生成网页数据
# ============================================================

def build_broad_payload(cache):

    dates = sorted(cache.get('by_date', {}))

    groups = []

    all_observed_codes = set()

    expected_counts = {}

    for g in GROUPS:

        expected_codes = sorted(
            CODE_LISTS[g['id']]
        )

        expected_counts[g['id']] = len(expected_codes)

        members = {}

        for day in dates:

            for code, rec in cache['by_date'].get(day, {}).items():

                code = str(code).zfill(6)

                if (
                    rec.get('group') == g['id']
                    and code in CODE_TO_GROUP
                ):

                    members[code] = rec
                    all_observed_codes.add(code)

        observed_codes = sorted(members)

        missing_codes = sorted(
            set(expected_codes) - set(observed_codes)
        )

        series = []
        counts = []
        missing = []

        for day in dates:

            rows = [
                r
                for code, r in cache['by_date'].get(day, {}).items()
                if (
                    str(code).zfill(6) in CODE_TO_GROUP
                    and CODE_TO_GROUP[str(code).zfill(6)] == g['id']
                )
            ]

            if rows:

                series.append(
                    round(
                        sum(
                            r['shares_10k']
                            for r in rows
                        ) / 10000,
                        4
                    )
                )

            else:

                series.append(None)
                missing.append(day)

            counts.append(len(rows))

        latest_idx = next(
            (
                i
                for i in range(len(dates) - 1, -1, -1)
                if series[i] is not None
            ),
            None
        )

        prev_idx = (
            next(
                (
                    i
                    for i in range(latest_idx - 1, -1, -1)
                    if series[i] is not None
                ),
                None
            )
            if latest_idx is not None
            else None
        )

        latest = (
            series[latest_idx]
            if latest_idx is not None
            else None
        )

        prev = (
            series[prev_idx]
            if prev_idx is not None
            else None
        )

        details = []

        if latest_idx is not None:

            day = dates[latest_idx]

            for code, rec in cache['by_date'].get(day, {}).items():

                code = str(code).zfill(6)

                if (
                    code in CODE_TO_GROUP
                    and CODE_TO_GROUP[code] == g['id']
                ):

                    details.append({
                        'code': code,
                        'name': rec.get('name', ''),
                        'exchange': rec.get('exchange', ''),
                        'shares': round(
                            rec['shares_10k'] / 10000,
                            4
                        )
                    })

        details.sort(
            key=lambda x: x['shares'],
            reverse=True
        )

        groups.append({
            **g,

            # 历史上曾经采集到的代码
            'codes': observed_codes,

            # 白名单预期数量及代码
            'expected_count': len(expected_codes),
            'expected_codes': expected_codes,

            # 历史上曾经采集到的代码数量
            'observed_count': len(observed_codes),

            # 历史上从未采集到的代码
            'missing_codes': missing_codes,

            # 最新披露日期实际数据条数
            'count': len(details),

            'series': series,
            'counts': counts,
            'latest': latest,

            'change1': (
                round(latest - prev, 4)
                if latest is not None and prev is not None
                else None
            ),

            'details': details,
            'missing_days': missing
        })

    # --------------------------------------------------------
    # 七大系列合计
    # --------------------------------------------------------

    total_series = []
    total_count = []

    for day in dates:

        rows = [
            r
            for code, r in cache['by_date'].get(day, {}).items()
            if (
                str(code).zfill(6) in CODE_TO_GROUP
                and r.get('group') in GROUP_IDS
            )
        ]

        if rows:

            total_series.append(
                round(
                    sum(
                        r['shares_10k']
                        for r in rows
                    ) / 10000,
                    4
                )
            )

        else:

            total_series.append(None)

        total_count.append(len(rows))

    latest_idx = next(
        (
            i
            for i in range(len(dates) - 1, -1, -1)
            if total_series[i] is not None
        ),
        None
    )

    prev_idx = (
        next(
            (
                i
                for i in range(latest_idx - 1, -1, -1)
                if total_series[i] is not None
            ),
            None
        )
        if latest_idx is not None
        else None
    )

    latest = (
        total_series[latest_idx]
        if latest_idx is not None
        else None
    )

    prev = (
        total_series[prev_idx]
        if prev_idx is not None
        else None
    )

    # --------------------------------------------------------
    # 总体白名单统计
    # --------------------------------------------------------

    observed_total = len(all_observed_codes)

    missing_total_codes = sorted(
        ALL_CODES - all_observed_codes
    )

    return {
        'dates': dates,
        'groups': groups,

        'total_series': total_series,
        'total_count': total_count,

        'total_latest': latest,

        'total_change1': (
            round(latest - prev, 4)
            if latest is not None and prev is not None
            else None
        ),

        # 白名单总体统计
        'expected_total': EXPECTED_TOTAL,
        'observed_total': observed_total,
        'missing_codes': missing_total_codes,
        'expected_counts': expected_counts,

        'updated_at': cache.get('updated_at'),
        'source': cache.get('source'),
        'errors': cache.get('errors', []),
        'partial_dates': cache.get('partial_dates', []),
        'start_date': cache.get('start_date'),
        'end_date': cache.get('end_date'),

        'note': (
            '单位：亿份。'
            '本页面仅统计固定代码白名单中的35只指定宽基ETF。'
            '七大系列分别为上证50、沪深300、中证500、'
            '中证1000、中证A500、科创宽基及创业板。'
            '不自动纳入同系列其他ETF。'
            '创业板200、创业板综指、科创创业50、'
            '双创50及行业主题ETF不纳入。'
            '分类完全依据ETF代码，不依赖基金简称。'
            '实际披露数量以交易所数据为准。'
            '缺失日不以前值填充。'
            '全市场合计指本页35只指定ETF合计，'
            '不代表全部类型ETF。'
        )
    }
