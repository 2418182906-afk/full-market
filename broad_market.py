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

def classify(name):
    n = (name or '').replace(' ', '').replace('（','(').replace('）',')')
    # Exclude non-plain-index strategies and bond products from the broad-index universe.
    if any(x in n for x in ('债','增强','红利','价值','质量','成长','策略','精选','自由现金流')):
        return None
    if '中证A500' in n or 'A500' in n: return 'A500'
    if '中证1000' in n: return '1000'
    if '中证500' in n: return '500'
    if '沪深300' in n: return '300'
    if '上证50' in n: return '50'
    if any(x in n for x in ('科创50','科创100','科创200','科创综指','科创板50','科创板100','科创板200')):
        return 'STAR'
    if any(x in n for x in ('创业板指','创业板50','创业板ETF','创业板指数')):
        return 'CHINEXT'
    return None

def _num(v):
    if v is None: return None
    try: return float(str(v).replace(',','').strip())
    except Exception: return None

def _request_json(url, params, referer=None, verify=True):
    h = dict(HEADERS)
    if referer: h['Referer'] = referer
    last = None
    for k in range(4):
        try:
            r = requests.get(url, params=params, headers=h, timeout=25, verify=verify)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e; time.sleep(0.6 * (k + 1))
    raise RuntimeError(str(last))

def _sse_date(day):
    js = _request_json(SSE_URL, {'isPagination':'false','STAT_DATE':day,
       'sqlId':'COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L'}, 'https://www.sse.com.cn/')
    rows = js.get('result', []) if isinstance(js, dict) else []
    out = {}
    for x in rows:
        code = str(x.get('SEC_CODE') or '').zfill(6)
        name = x.get('FUND_EXPANSION_ABBR') or x.get('SEC_NAME') or x.get('FUND_ABBR') or ''
        shares = _num(x.get('TOT_VOL')) # 万份
        group = classify(name)
        if len(code)==6 and shares is not None and shares >= 0 and group:
            out[code] = {'code':code,'name':name,'exchange':'SSE','group':group,'shares_10k':shares}
    return out

def _extract_sz_rows(js):
    # SZSE report API commonly wraps data as [{metadata:{...},data:[...]}].
    if isinstance(js, list):
        for item in js:
            if isinstance(item, dict) and isinstance(item.get('data'), list): return item['data'], item.get('metadata', {})
        return [], {}
    if isinstance(js, dict):
        if isinstance(js.get('data'), list): return js['data'], js.get('metadata', {})
        if isinstance(js.get('data'), dict) and isinstance(js['data'].get('data'), list):
            return js['data']['data'], js['data'].get('metadata', {})
    return [], {}

def _szse_window(start, end):
    all_rows=[]
    for page in range(1, 101):
        params={'CATALOGID':'scsj_fund_jjgm','txtStart':start,'txtEnd':end,'txtDm':'',
                'jjlb':'ETF','tab1PAGENUM':str(page),'tab1PAGECOUNT':'20','pageNum':str(page),'pageSize':'20'}
        js=_request_json(SZSE_URL, params, 'https://www.szse.cn/market/fund/volume/etf/index.html', verify=False)
        rows, meta = _extract_sz_rows(js)
        if not rows: break
        all_rows.extend(rows)
        total = int(_num(meta.get('recordcount') or meta.get('recordCount') or meta.get('total')) or 0)
        if total and len(all_rows) >= total: break
        if len(rows) < 20: break
        time.sleep(0.25)
    return all_rows

def _record_date(row):
    v = row.get('size_date') or row.get('stat_date') or row.get('date') or row.get('日期') or ''
    return str(v)[:10].replace('/','-')

def _szse_history(start, end):
    # The official report supports bounded date ranges; use short windows to avoid server limits.
    by_date={}; failed_ranges=[]
    cur=date.fromisoformat(start); end_d=date.fromisoformat(end)
    while cur <= end_d:
        stop=min(cur+timedelta(days=89), end_d)
        try:
            rows=_szse_window(cur.isoformat(), stop.isoformat())
        except Exception:
            failed_ranges.append([cur.isoformat(), stop.isoformat()])
            rows=[]
        for x in rows:
            day=_record_date(x)
            code=str(x.get('fund_code') or x.get('fundCode') or x.get('证券代码') or '').zfill(6)
            name=x.get('security_short_name') or x.get('fund_name') or x.get('证券简称') or ''
            shares=_num(x.get('current_size') or x.get('fund_size') or x.get('基金规模'))
            group=classify(name)
            if len(code)==6 and len(day)==10 and shares is not None and group:
                by_date.setdefault(day,{})[code]={'code':code,'name':name,'exchange':'SZSE','group':group,'shares_10k':shares}
        cur=stop+timedelta(days=1)
        time.sleep(0.35)
    return by_date, failed_ranges

def sync_broad_market(force_full=False):
    os.makedirs(DATA_DIR, exist_ok=True)
    cache={'by_date':{},'last_attempt':'','errors':[],'source':'SSE+SZSE official ETF share disclosures'}
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH,encoding='utf-8') as f: cache=json.load(f)
        except Exception: pass
    by_date=cache.get('by_date',{}) if not force_full else {}
    today=date.today()
    # Refresh the last 5 calendar days and backfill all trading dates from START_DATE on first run.
    if force_full or not by_date:
        start=date.fromisoformat(START_DATE)
    else:
        start=max(date.fromisoformat(START_DATE), today-timedelta(days=8))
    end=today
    errors=[]
    partial_dates=set(cache.get('partial_dates', [])) if not force_full else set()
    # SSE: one all-ETF query per calendar date; bounded concurrency and retry.
    days=[]; d=start
    while d<=end:
        days.append(d.isoformat()); d+=timedelta(days=1)
    def sse_job(day):
        try: return day,_sse_date(day),None
        except Exception as e: return day,{},str(e)
    workers=8 if len(days)>30 else 4
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures=[pool.submit(sse_job,x) for x in days]
        for f in as_completed(futures):
            day, rows, err=f.result()
            if rows: by_date.setdefault(day,{}).update(rows)
            elif err:
                errors.append('SSE '+day+': '+err); partial_dates.add(day)
    # SZSE: range API, in 90-calendar-day windows. Preserve prior rows if a source request fails.
    try:
        sz, failed_ranges=_szse_history(start.isoformat(), end.isoformat())
        for day, rows in sz.items(): by_date.setdefault(day,{}).update(rows)
        for a,b in failed_ranges:
            errors.append('SZSE '+a+'~'+b+': request failed')
            d=date.fromisoformat(a); last=date.fromisoformat(b)
            while d<=last: partial_dates.add(d.isoformat()); d+=timedelta(days=1)
    except Exception as e:
        errors.append('SZSE history: '+str(e))
        d=start
        while d<=end: partial_dates.add(d.isoformat()); d+=timedelta(days=1)
    # Keep only classified series and ensure dates are sorted. No forward-fill: missing is missing.
    clean={}
    for day, rows in by_date.items():
        selected={c:r for c,r in rows.items() if r.get('group') in {g['id'] for g in GROUPS}}
        if selected: clean[day]=selected
    cache={'updated_at':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
           'start_date':min(clean) if clean else START_DATE,'end_date':max(clean) if clean else '',
           'by_date':clean,'groups':GROUPS,'partial_dates':sorted(partial_dates),'last_attempt':datetime.now().isoformat(timespec='seconds'),
           'errors':errors[-30:],'source':'上海证券交易所、深圳证券交易所公开ETF份额披露'}
    with open(CACHE_PATH,'w',encoding='utf-8') as f: json.dump(cache,f,ensure_ascii=False,indent=2)
    return cache

def build_broad_payload(cache):
    dates=sorted(cache.get('by_date',{}))
    groups=[]
    all_codes=set()
    for g in GROUPS:
        members={}
        for day in dates:
            for code,rec in cache['by_date'].get(day,{}).items():
                if rec.get('group')==g['id']:
                    members[code]=rec
                    all_codes.add(code)
        codes=sorted(members)
        series=[]; counts=[]; missing=[]
        for day in dates:
            rows=[r for r in cache['by_date'].get(day,{}).values() if r.get('group')==g['id']]
            series.append(round(sum(r['shares_10k'] for r in rows)/10000,4) if rows else None)
            counts.append(len(rows))
            if not rows: missing.append(day)
        latest_idx=next((i for i in range(len(dates)-1,-1,-1) if series[i] is not None),None)
        prev_idx=next((i for i in range((latest_idx or 0)-1,-1,-1) if series[i] is not None),None) if latest_idx is not None else None
        latest=series[latest_idx] if latest_idx is not None else None
        prev=series[prev_idx] if prev_idx is not None else None
        details=[]
        if latest_idx is not None:
            day=dates[latest_idx]
            for code,rec in cache['by_date'].get(day,{}).items():
                if rec.get('group')==g['id']:
                    details.append({'code':code,'name':rec.get('name',''),'exchange':rec.get('exchange',''),
                                    'shares':round(rec['shares_10k']/10000,4)})
        details.sort(key=lambda x:x['shares'],reverse=True)
        groups.append({**g,'codes':codes,'count':len(details),'series':series,'counts':counts,
                       'latest':latest,'change1':round(latest-prev,4) if latest is not None and prev is not None else None,
                       'details':details,'missing_days':missing})
    total_series=[]; total_count=[]
    for i,day in enumerate(dates):
        rows=[r for r in cache['by_date'].get(day,{}).values() if r.get('group') in {g['id'] for g in GROUPS}]
        total_series.append(round(sum(r['shares_10k'] for r in rows)/10000,4) if rows else None)
        total_count.append(len(rows))
    latest_idx=next((i for i in range(len(dates)-1,-1,-1) if total_series[i] is not None),None)
    prev_idx=next((i for i in range((latest_idx or 0)-1,-1,-1) if total_series[i] is not None),None) if latest_idx is not None else None
    latest=total_series[latest_idx] if latest_idx is not None else None
    prev=total_series[prev_idx] if prev_idx is not None else None
    return {'dates':dates,'groups':groups,'total_series':total_series,'total_count':total_count,
            'total_latest':latest,'total_change1':round(latest-prev,4) if latest is not None and prev is not None else None,
            'updated_at':cache.get('updated_at'),'source':cache.get('source'),'errors':cache.get('errors',[]),
            'partial_dates':cache.get('partial_dates',[]),
            'start_date':cache.get('start_date'),'end_date':cache.get('end_date'),
            'note':'单位：亿份。仅统计名称可识别的指定宽基ETF；缺失日不以前值填充。全市场合计指本页七个系列合计，不代表全部类型ETF。'}
