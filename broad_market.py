
# -*- coding: utf-8 -*-
"""沪深交易所宽基ETF全量份额采集与系列聚合。

份额单位统一为亿份；原始缓存保留万份。
分类以基金简称、代码白名单及实际指数名称为依据。
无法可靠识别的ETF不强行归类。
"""

import json
import os
import re
import time
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
CACHE_PATH = os.path.join(DATA_DIR, 'broad_etf_cache.json')

START_DATE = '2024-06-03'

# 修改分类规则后递增版本号，触发历史数据重新采集
CLASSIFICATION_VERSION = 2

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
# 一、代码白名单
# ============================================================

# 对简称没有完整写出指数名称的产品进行补充识别。
# 这些代码来自本项目提供的候选清单。
# 只有经过确认的代码才能放入这里。

CODE_OVERRIDES = {
    '159678': '500',       # 500增强ETF博时
    '561780': '1000',      # 1000增强ETF博时
    '159685': '1000',      # 1000增强ETF天弘
    '159676': 'CHINEXT',   # 创业板增强ETF富国
}


# ============================================================
# 二、行业主题排除规则
# ============================================================

# 命中这些关键词的产品不进入七大宽基统计。
# 注意：增强、红利、价值、成长、低波、现金流、ESG
# 本身不是排除条件。

INDUSTRY_KEYWORDS = (
    '芯片',
    '半导体',
    '新材料',
    '人工智能',
    '算力',
    '软件',
    '电池',
    '新能源',
    '金融科技',
    '创新药',
    '医疗',
    '医药',
    '证券',
    '银行',
    '消费',
    '食品',
    '酒',
    '军工',
    '机器人',
    '光伏',
    '稀土',
    '有色',
    '煤炭',
    '钢铁',
    '通信',
    '传媒',
    '云计算',
    '数据中心',
    '生物科技',
    '生物医药',
    '房地产',
    '汽车',
    '储能',
    '家电',
    '农业',
    '畜牧',
    '旅游',
    '游戏',
    '教育',
    '环保',
    '基建',
    '电力',
    '化工',
    '建材',
    '能源',
    '油气',
    '港股',
    '恒生',
    '纳指',
    '标普',
    '日经',
    '德国',
    '法国',
    '海外',
    '全球',
)


# ============================================================
# 三、ETF 分类函数
# ============================================================

def classify(name, code=None):
    """
    返回七大系列ID；无法可靠识别时返回None。

    分类原则：
    1. 优先排除行业主题和跨市场指数。
    2. 代码白名单只补充简称不完整的产品。
    3. 宽基及其策略型产品可以纳入。
    4. 不把创业板200、创业板综指纳入创业板系列。
    5. 科创创业50、双创50不纳入科创宽基。
    """

    n = str(name or '').strip()
    n = n.replace(' ', '').replace('（', '(').replace('）', ')')
    n = n.replace('－', '-').replace('—', '-')

    code = str(code or '').strip().zfill(6)

    if not n:
        return None

    # 1. 优先排除跨市场指数
    cross_market_keywords = (
        '科创创业',
        '双创50',
        '双创',
    )

    if any(k in n for k in cross_market_keywords):
        return None

    # 2. 排除行业主题ETF
    if any(k in n for k in INDUSTRY_KEYWORDS):
        return None

    # 3. 排除不属于本次目标范围的创业板指数
    if any(k in n for k in (
        '创业板200',
        '创业板综',
        '创业板综指',
    )):
        return None

    # 4. 排除不属于本次目标范围的科创指数
    if '科创成长' in n:
        return None

    # 5. 对简称不完整的已确认产品进行代码补充
    if code in CODE_OVERRIDES:
        return CODE_OVERRIDES[code]

    # 6. 中证A500
    if '中证A500' in n or 'A500' in n:
        return 'A500'

    # 7. 中证1000
    if (
        '中证1000' in n
        or '1000增强' in n
    ):
        return '1000'

    # 8. 中证500
    if (
        '中证500' in n
        or '500增强' in n
    ):
        return '500'

    # 9. 沪深300
    if '沪深300' in n:
        return '300'

    # 10. 上证50
    if '上证50' in n:
        return '50'

    # 11. 科创宽基
    # 只保留明确的科创宽基指数及对应增强产品。
    if any(k in n for k in (
        '科创50',
        '科创100',
        '科创200',
        '科创综指',
        '科创板50',
        '科创板100',
        '科创板200',
    )):
        return 'STAR'

    # 12. 创业板
    # 创业板指、创业板50及明确对应的宽基增强产品。
    if any(k in n for k in (
        '创业板50',
        '创业板指',
        '创业板指数',
    )):
        return 'CHINEXT'

    # 仅有创业板ETF字样时，纳入原始候选清单中的普通创业板ETF。
    # 不允许创业板200、创业板综指或行业主题产品进入。
    if '创业板ETF' in n:
        return 'CHINEXT'

    return None


# ============================================================
# 四、通用数据处理
# ============================================================

def _num(v):
    if v is None:
        return None

    try:
        return float(str(v).replace(',', '').strip())
    except Exception:
        return None


def _request_json(url, params, referer=None, verify=True):
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
# 五、上海证券交易所数据
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

        code = str(x.get('SEC_CODE') or '').zfill(6)

        name = (
            x.get('FUND_EXPANSION_ABBR')
            or x.get('SEC_NAME')
            or x.get('FUND_ABBR')
            or ''
        )

        shares = _num(x.get('TOT_VOL'))

        group = classify(name, code)

        if (
            len(code) == 6
            and shares is not None
            and shares >= 0
            and group in GROUP_IDS
        ):

            out[code] = {
                'code': code,
                'name': name,
                'exchange': 'SSE',
                'group': group,
                'shares_10k': shares
            }

    return out


# ============================================================
# 六、深圳证券交易所数据
# ============================================================

def _extract_sz_rows(js):

    if isinstance(js, list):

        for item in js:

            if (
                isinstance(item, dict)
                and isinstance(item.get('data'), list)
            ):
                return item['data'], item.get('metadata', {})

        return [], {}

    if isinstance(js, dict):

        if isinstance(js.get('data'), list):
            return js['data'], js.get('metadata', {})

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

    by_date = {}
    failed_ranges = []

    cur = date.fromisoformat(start)
    end_d = date.fromisoformat(end)

    while cur <= end_d:

        stop = min(cur + timedelta(days=89), end_d)

        try:

            rows = _szse_window(
                cur.isoformat(),
                stop.isoformat()
            )

        except Exception:

            failed_ranges.append([
                cur.isoformat(),
                stop.isoformat()
            ])

            rows = []

        for x in rows:

            day = _record_date(x)

            code = str(
                x.get('fund_code')
                or x.get('fundCode')
                or x.get('证券代码')
                or ''
            ).zfill(6)

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

            group = classify(name, code)

            if (
                len(code) == 6
                and len(day) == 10
                and shares is not None
                and group in GROUP_IDS
            ):

                by_date.setdefault(day, {})[code] = {
                    'code': code,
                    'name': name,
                    'exchange': 'SZSE',
                    'group': group,
                    'shares_10k': shares
                }

        cur = stop + timedelta(days=1)

        time.sleep(0.35)

    return by_date, failed_ranges


# ============================================================
# 七、历史缓存重新分类
# ============================================================

def _reclassify_cache(old_by_date):

    """
    按新规则重新整理历史缓存。
    旧缓存中已有的记录重新判断分类。
    无法识别的记录不进入七大系列。
    """

    result = {}

    for day, rows in old_by_date.items():

        if not isinstance(rows, dict):
            continue

        new_rows = {}

        for code, rec in rows.items():

            if not isinstance(rec, dict):
                continue

            name = rec.get('name', '')

            group = classify(name, code)

            if group not in GROUP_IDS:
                continue

            new_rec = dict(rec)
            new_rec['code'] = str(code).zfill(6)
            new_rec['group'] = group

            new_rows[str(code).zfill(6)] = new_rec

        if new_rows:
            result[day] = new_rows

    return result


# ============================================================
# 八、同步数据
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

    # 重新分类已有缓存
    reclassified = _reclassify_cache(old_by_date)

    # 分类版本变化时，重新采集历史数据
    version_changed = (
        cache.get('classification_version')
        != CLASSIFICATION_VERSION
    )

    full_refresh = force_full or version_changed

    if full_refresh:

        print(
            '检测到分类规则更新，开始重新采集历史ETF数据。',
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

    # --------------------------------------------------------
    # SSE：按交易日期查询
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
        f'开始采集上交所数据，共 {len(days)} 个日历日期。',
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

            if rows:

                by_date.setdefault(day, {}).update(rows)

                partial_dates.discard(day)

            elif err:

                errors.append('SSE ' + day + ': ' + err)
                partial_dates.add(day)

            if completed % 20 == 0 or completed == len(days):

                print(
                    f'SSE进度：{completed}/{len(days)}',
                    flush=True
                )

    # --------------------------------------------------------
    # SZSE：按90天时间窗口查询
    # --------------------------------------------------------

    print('开始采集深交所历史数据。', flush=True)

    try:

        sz, failed_ranges = _szse_history(
            start.isoformat(),
            end.isoformat()
        )

        for day, rows in sz.items():

            by_date.setdefault(day, {}).update(rows)

            partial_dates.discard(day)

        for a, b in failed_ranges:

            errors.append(
                'SZSE ' + a + '~' + b + ': request failed'
            )

            d = date.fromisoformat(a)
            last = date.fromisoformat(b)

            while d <= last:

                partial_dates.add(d.isoformat())
                d += timedelta(days=1)

    except Exception as e:

        errors.append('SZSE history: ' + str(e))

        d = start

        while d <= end:

            partial_dates.add(d.isoformat())
            d += timedelta(days=1)

    # --------------------------------------------------------
    # 如果某些日期请求失败，保留这些日期的旧缓存
    # --------------------------------------------------------

    if full_refresh:

        for day in partial_dates:

            old_rows = reclassified.get(day, {})

            if old_rows:

                by_date.setdefault(day, {}).update(old_rows)

    # --------------------------------------------------------
    # 最终清理
    # --------------------------------------------------------

    clean = {}

    for day, rows in by_date.items():

        selected = {
            c: r
            for c, r in rows.items()
            if r.get('group') in GROUP_IDS
        }

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
        f'数据采集完成：{len(clean)} 个有数据日期。',
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
# 九、生成网页数据
# ============================================================

def build_broad_payload(cache):

    dates = sorted(cache.get('by_date', {}))

    groups = []

    all_codes = set()

    for g in GROUPS:

        members = {}

        for day in dates:

            for code, rec in cache['by_date'].get(day, {}).items():

                if rec.get('group') == g['id']:

                    members[code] = rec
                    all_codes.add(code)

        codes = sorted(members)

        series = []
        counts = []
        missing = []

        for day in dates:

            rows = [
                r
                for r in cache['by_date'].get(day, {}).values()
                if r.get('group') == g['id']
            ]

            if rows:

                series.append(
                    round(
                        sum(r['shares_10k'] for r in rows) / 10000,
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

                if rec.get('group') == g['id']:

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
            'codes': codes,
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

    for i, day in enumerate(dates):

        rows = [
            r
            for r in cache['by_date'].get(day, {}).values()
            if r.get('group') in GROUP_IDS
        ]

        if rows:

            total_series.append(
                round(
                    sum(r['shares_10k'] for r in rows) / 10000,
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
        'updated_at': cache.get('updated_at'),
        'source': cache.get('source'),
        'errors': cache.get('errors', []),
        'partial_dates': cache.get('partial_dates', []),
        'start_date': cache.get('start_date'),
        'end_date': cache.get('end_date'),
        'note': (
            '单位：亿份。仅统计名称可识别的指定宽基ETF；'
            '增强、价值、成长、红利、低波、现金流及ESG等'
            '策略产品在符合对应指数条件时纳入。'
            '行业主题、创业板200、创业板综指、'
            '科创创业50及双创50不纳入。'
            '缺失日不以前值填充。'
            '全市场合计指本页七个系列合计，'
            '不代表全部类型ETF。'
        )
    }
