            ),

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
