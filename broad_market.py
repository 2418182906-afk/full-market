        )
        changes.append(round(delta_10k / 10000.0, 4))

    return changes


# ============================================================
# 生成网页数据
# ============================================================

def build_broad_payload(cache):
    dates = sorted(cache.get('by_date', {}).keys())
    groups_payload = []
    all_observed_codes = set()
    expected_counts = {}

    for group in GROUPS:
        group_id = group['id']
        expected_codes = sorted(CODE_LISTS[group_id])
        expected_counts[group_id] = len(expected_codes)

        members = {}
        for day in dates:
            for raw_code, record in cache.get('by_date', {}).get(day, {}).items():
                code = _normalize_code(raw_code)
                if code in CODE_TO_GROUP and CODE_TO_GROUP[code] == group_id:
                    members[code] = record
                    all_observed_codes.add(code)

        observed_codes = sorted(members)
        missing_codes = sorted(set(expected_codes) - set(observed_codes))

        series = []
        counts = []
        missing_days = []

        for day in dates:
            rows = []
            for raw_code, record in cache.get('by_date', {}).get(day, {}).items():
                code = _normalize_code(raw_code)
                if code in CODE_TO_GROUP and CODE_TO_GROUP[code] == group_id:
                    value = _num(record.get('shares_10k'))
                    if value is not None:
                        rows.append(value)

            if rows:
                series.append(round(sum(rows) / 10000.0, 4))
            else:
                series.append(None)
                missing_days.append(day)
            counts.append(len(rows))

        latest_idx = next(
            (i for i in range(len(dates) - 1, -1, -1) if series[i] is not None),
            None
        )
        prev_idx = (
            next(
                (i for i in range(latest_idx - 1, -1, -1) if series[i] is not None),
                None
            ) if latest_idx is not None else None
        )

        latest = series[latest_idx] if latest_idx is not None else None
        prev = series[prev_idx] if prev_idx is not None else None

        details = []
        if latest_idx is not None:
            latest_day = dates[latest_idx]
            for raw_code, record in cache.get('by_date', {}).get(latest_day, {}).items():
                code = _normalize_code(raw_code)
                if code in CODE_TO_GROUP and CODE_TO_GROUP[code] == group_id:
                    value = _num(record.get('shares_10k'))
                    if value is None:
                        continue
                    details.append({
                        'code': code,
                        'name': record.get('name', ''),
                        'exchange': record.get('exchange', ''),
                        'shares': round(value / 10000.0, 4),
                    })

        details.sort(key=lambda item: item['shares'], reverse=True)
        daily_change = _daily_common_code_changes(cache, dates, group_id)

        groups_payload.append({
            **group,
            'codes': observed_codes,
            'expected_count': len(expected_codes),
            'expected_codes': expected_codes,
            'observed_count': len(observed_codes),
            'missing_codes': missing_codes,
            'count': len(details),
            'series': series,
            'counts': counts,
            'latest': latest,
            'change1': (
                daily_change[latest_idx]
                if latest_idx is not None and latest_idx < len(daily_change)
                else None
            ),
            'daily_change': daily_change,
            'details': details,
            'missing_days': missing_days,
        })

    # 七大系列合计份额曲线
    total_series = []
    total_count = []
    for day in dates:
        values = []
        for raw_code, record in cache.get('by_date', {}).get(day, {}).items():
            code = _normalize_code(raw_code)
            if code not in CODE_TO_GROUP:
                continue
            value = _num(record.get('shares_10k'))
            if value is not None:
                values.append(value)

        total_series.append(
            round(sum(values) / 10000.0, 4) if values else None
        )
        total_count.append(len(values))

    total_latest_idx = next(
        (i for i in range(len(dates) - 1, -1, -1) if total_series[i] is not None),
        None
    )
    total_prev_idx = (
        next(
            (i for i in range(total_latest_idx - 1, -1, -1)
             if total_series[i] is not None),
            None
        ) if total_latest_idx is not None else None
    )

    total_latest = (
        total_series[total_latest_idx]
        if total_latest_idx is not None else None
    )
    total_daily_change = _daily_common_code_changes(cache, dates)
    total_change1 = (
        total_daily_change[total_latest_idx]
        if total_latest_idx is not None and total_latest_idx < len(total_daily_change)
        else None
    )

    observed_total = len(all_observed_codes)
    missing_total_codes = sorted(ALL_CODES - all_observed_codes)

    return {
        'dates': dates,
        'groups': groups_payload,
        'total_series': total_series,
        'total_count': total_count,
        'total_daily_change': total_daily_change,
        'total_latest': total_latest,
        'total_change1': total_change1,
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
            '单位：亿份。本页面仅统计固定代码白名单中的35只指定宽基ETF。'
            '七大系列分别为上证50、沪深300、中证500、中证1000、'
            '中证A500、科创宽基及创业板。'
            '不自动纳入同系列其他ETF。创业板200、创业板综指、'
            '科创创业50、双创50及行业主题ETF不纳入。'
            '分类完全依据ETF代码，不依赖基金简称。'
            '实际披露数量以交易所数据为准；缺失日不以前值填充。'
            '每日变化仅比较相邻披露日共同有数据的ETF代码。'
        ),
    }


if __name__ == '__main__':
    result = sync_broad_market()
    payload = build_broad_payload(result)
    print('\n宽基ETF最新统计：')
    print(
        f"历史采集代码：{payload['observed_total']}/"
        f"{payload['expected_total']}"
    )
    for item in payload['groups']:
        print(
            f"{item['name']}: {item['count']}/{item['expected_count']}只，"
