"""
ICT PD Array Stacking Strategy — 9:30-10:00 NQ
=================================================
Identifies Price Delivery (PD) arrays on 15m, 1h, and 4h timeframes:
  - Fair Value Gaps (FVGs)
  - Order Blocks (OBs)

When 2+ timeframes have overlapping PD arrays at the same price zone,
that level is "stacked" — high institutional significance.

Only trades 9:30-10:00: enters when price SWEEPS a stacked level
(wicks through + closes back) = rejection = entry signal.
"""

import csv
import os
import glob
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta


# ═══════════════════════════════════════════════════════════════════════
#  DATA LOADING
# ═══════════════════════════════════════════════════════════════════════

def load_local_data(path):
    """Load clean local CSV with full RTH bars."""
    data = OrderedDict()
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt = datetime.fromisoformat(row['time'])
            date_str = dt.strftime('%Y-%m-%d')
            time_str = dt.strftime('%H:%M')
            if date_str not in data:
                data[date_str] = []
            data[date_str].append({
                'time': time_str,
                'date': date_str,
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
            })
    return data


def load_repo_data_full(base_dir):
    """Load ALL bars from repo (not just opening window)."""
    data = OrderedDict()
    files = sorted(glob.glob(os.path.join(base_dir, "*/*/*_5m.csv")))
    print(f"  Loading {len(files)} monthly files (full bars)...")
    for fpath in files:
        with open(fpath) as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts = row['ts_event'][:19]
                dt = datetime.fromisoformat(ts)
                month = dt.month
                offset = 4 if 3 <= month <= 10 else 5
                dt_et = dt - timedelta(hours=offset)
                date_str = dt_et.strftime('%Y-%m-%d')
                time_str = dt_et.strftime('%H:%M')
                # Only RTH bars
                if not ('09:30' <= time_str <= '15:55'):
                    continue
                o = float(row['open'])
                h = float(row['high'])
                l = float(row['low'])
                c = float(row['close'])
                # Repair corrupted values
                ref = max(v for v in [o, h, l, c] if v > 0) if any(v > 0 for v in [o, h, l, c]) else 0
                if ref <= 0:
                    continue
                thr = ref * 0.5
                if o <= 0 or o < thr: o = c if c >= thr else ref
                if c <= 0 or c < thr: c = o if o >= thr else ref
                if l <= 0 or l < thr: l = min(o, c)
                if h <= 0 or h < thr: h = max(o, c)
                if h < l: h, l = l, h
                if date_str not in data:
                    data[date_str] = []
                data[date_str].append({
                    'time': time_str, 'date': date_str,
                    'open': o, 'high': h, 'low': l, 'close': c,
                })
    for d in data:
        data[d].sort(key=lambda b: b['time'])
    return data


# ═══════════════════════════════════════════════════════════════════════
#  TIMEFRAME AGGREGATION
# ═══════════════════════════════════════════════════════════════════════

def aggregate_bars(five_min_bars, n_bars):
    """Aggregate consecutive 5m bars into groups of n_bars."""
    result = []
    for i in range(0, len(five_min_bars), n_bars):
        chunk = five_min_bars[i:i + n_bars]
        if not chunk:
            continue
        result.append({
            'time': chunk[0]['time'],
            'date': chunk[0]['date'],
            'open': chunk[0]['open'],
            'high': max(b['high'] for b in chunk),
            'low': min(b['low'] for b in chunk),
            'close': chunk[-1]['close'],
        })
    return result


def build_timeframe_bars(data_by_date):
    """Build flat lists of bars for each timeframe across all days.
    Returns dict: {'5m': [...], '15m': [...], '1h': [...], '4h': [...]}
    Each bar has 'date', 'time', OHLC.
    """
    tf_bars = {'5m': [], '15m': [], '1h': [], '4h': []}
    for date in sorted(data_by_date.keys()):
        rth = [b for b in data_by_date[date] if '09:30' <= b['time'] <= '15:55']
        if not rth:
            continue
        tf_bars['5m'].extend(rth)
        tf_bars['15m'].extend(aggregate_bars(rth, 3))   # 15 min
        tf_bars['1h'].extend(aggregate_bars(rth, 12))    # 60 min
        tf_bars['4h'].extend(aggregate_bars(rth, 48))    # 4 hours
    return tf_bars


# ═══════════════════════════════════════════════════════════════════════
#  PD ARRAY IDENTIFICATION
# ═══════════════════════════════════════════════════════════════════════

def find_fvgs(bars):
    """Find Fair Value Gaps. Returns list of {type, top, bottom, idx}."""
    fvgs = []
    for i in range(2, len(bars)):
        c1, c2, c3 = bars[i - 2], bars[i - 1], bars[i]
        # Bullish FVG: candle3.low > candle1.high
        if c3['low'] > c1['high']:
            fvgs.append({
                'type': 'bullish', 'top': c3['low'], 'bottom': c1['high'],
                'midpoint': (c3['low'] + c1['high']) / 2, 'idx': i,
            })
        # Bearish FVG: candle3.high < candle1.low
        if c3['high'] < c1['low']:
            fvgs.append({
                'type': 'bearish', 'top': c1['low'], 'bottom': c3['high'],
                'midpoint': (c1['low'] + c3['high']) / 2, 'idx': i,
            })
    return fvgs


def find_order_blocks(bars):
    """Find Order Blocks. Returns list of {type, top, bottom, idx}."""
    obs = []
    for i in range(1, len(bars)):
        prev, curr = bars[i - 1], bars[i]
        # Bullish OB: down candle (prev) followed by close above prev high (displacement up)
        if prev['close'] < prev['open'] and curr['close'] > prev['high']:
            obs.append({
                'type': 'bullish',
                'top': prev['open'],    # opening price of down candle
                'bottom': prev['low'],  # low of down candle
                'midpoint': (prev['open'] + prev['low']) / 2,
                'idx': i,
            })
        # Bearish OB: up candle (prev) followed by close below prev low (displacement down)
        if prev['close'] > prev['open'] and curr['close'] < prev['low']:
            obs.append({
                'type': 'bearish',
                'top': prev['high'],    # high of up candle
                'bottom': prev['open'], # opening price of up candle
                'midpoint': (prev['high'] + prev['open']) / 2,
                'idx': i,
            })
    return obs


def get_unfilled_pd_arrays(bars, lookback, current_idx):
    """Get PD arrays from lookback period that haven't been filled yet.
    A PD array is 'filled' if any subsequent bar fully traded through it.
    """
    start = max(0, current_idx - lookback)
    lookback_bars = bars[start:current_idx]

    fvgs = find_fvgs(lookback_bars)
    obs = find_order_blocks(lookback_bars)
    all_pd = fvgs + obs

    # Check each PD array: is it filled by subsequent bars?
    unfilled = []
    for pd in all_pd:
        pd_start = start + pd['idx']
        filled = False
        for j in range(pd_start + 1, current_idx):
            bar = bars[j]
            if pd['type'] == 'bullish':
                # Filled if a bar trades entirely through the zone from above
                if bar['close'] < pd['bottom']:
                    filled = True
                    break
            else:
                # Filled if a bar trades entirely through the zone from below
                if bar['close'] > pd['top']:
                    filled = True
                    break
        if not filled:
            unfilled.append(pd)
    return unfilled


# ═══════════════════════════════════════════════════════════════════════
#  PD ARRAY STACKING — Find Where 2+ Timeframes Overlap
# ═══════════════════════════════════════════════════════════════════════

def zones_overlap(z1, z2, tolerance_pct=0.001):
    """Check if two price zones overlap (with small tolerance)."""
    # Expand zones slightly by tolerance
    mid = (z1['top'] + z1['bottom'] + z2['top'] + z2['bottom']) / 4
    tol = mid * tolerance_pct
    top1, bot1 = z1['top'] + tol, z1['bottom'] - tol
    top2, bot2 = z2['top'] + tol, z2['bottom'] - tol
    # Overlap exists if one starts before the other ends
    return bot1 <= top2 and bot2 <= top1


def find_stacked_levels(pd_arrays_by_tf, min_stack=2):
    """Find price levels where min_stack or more timeframes have overlapping PD arrays.
    Returns list of {type, top, bottom, midpoint, tf_count, timeframes}.
    """
    timeframes = list(pd_arrays_by_tf.keys())
    stacked = []
    used = set()  # avoid duplicate stacks

    for i, tf1 in enumerate(timeframes):
        for pa1 in pd_arrays_by_tf[tf1]:
            matching_tfs = {tf1}
            overlap_top = pa1['top']
            overlap_bottom = pa1['bottom']

            for j, tf2 in enumerate(timeframes):
                if tf2 == tf1:
                    continue
                for pa2 in pd_arrays_by_tf[tf2]:
                    if pa1['type'] != pa2['type']:
                        continue
                    if zones_overlap(
                        {'top': overlap_top, 'bottom': overlap_bottom},
                        pa2
                    ):
                        matching_tfs.add(tf2)
                        # Narrow the overlap zone
                        overlap_top = min(overlap_top, pa2['top'])
                        overlap_bottom = max(overlap_bottom, pa2['bottom'])

            if len(matching_tfs) >= min_stack:
                # De-duplicate by rounding midpoint
                mid = round((overlap_top + overlap_bottom) / 2, 1)
                key = (pa1['type'], mid)
                if key not in used:
                    used.add(key)
                    stacked.append({
                        'type': pa1['type'],
                        'top': overlap_top,
                        'bottom': overlap_bottom,
                        'midpoint': (overlap_top + overlap_bottom) / 2,
                        'tf_count': len(matching_tfs),
                        'timeframes': sorted(matching_tfs),
                    })

    # Sort by tf_count descending (strongest stacks first)
    stacked.sort(key=lambda x: -x['tf_count'])
    return stacked


# ═══════════════════════════════════════════════════════════════════════
#  SWEEP DETECTION — Price wicks through level + closes back
# ═══════════════════════════════════════════════════════════════════════

def detect_sweep(bar, level, direction):
    """Check if a 5m bar sweeps a PD array level.
    Bullish sweep: bar wicks below level bottom and closes above it (rejection = buy).
    Bearish sweep: bar wicks above level top and closes below it (rejection = sell).
    Returns sweep_extreme or None.
    """
    if direction == 'bullish':
        # Price wicks below the bullish zone bottom, closes back above
        if bar['low'] < level['bottom'] and bar['close'] > level['bottom']:
            return bar['low']  # sweep extreme
    elif direction == 'bearish':
        # Price wicks above the bearish zone top, closes back below
        if bar['high'] > level['top'] and bar['close'] < level['top']:
            return bar['high']  # sweep extreme
    return None


# ═══════════════════════════════════════════════════════════════════════
#  TRADE SIMULATION
# ═══════════════════════════════════════════════════════════════════════

def simulate_trade(future_bars, direction, entry, stop, target):
    """Market order simulation bar-by-bar."""
    if not future_bars:
        return None
    for bar in future_bars:
        if direction == 'long':
            if bar['low'] <= stop:
                return stop - entry
            if bar['high'] >= target:
                return target - entry
        else:
            if bar['high'] >= stop:
                return entry - stop
            if bar['low'] <= target:
                return entry - target
    eod = future_bars[-1]['close']
    return (eod - entry) if direction == 'long' else (entry - eod)


# ═══════════════════════════════════════════════════════════════════════
#  MAIN STRATEGY
# ═══════════════════════════════════════════════════════════════════════

def run_pd_stacking_strategy(data_by_date, min_stack=2, lookback_15m=40, lookback_1h=20, lookback_4h=10):
    """
    For each trading day:
    1. Build PD arrays on 15m, 1h, 4h from lookback before 9:30
    2. Find stacked levels (2+ TF overlap)
    3. During 9:30-10:00, check for sweeps of stacked levels
    4. Enter on sweep rejection, stop at sweep extreme, target 1R or next level
    """
    tf_bars = build_timeframe_bars(data_by_date)
    dates = sorted(data_by_date.keys())

    # Build date→index maps for each timeframe (index of first bar on/after each date)
    def build_date_idx(bars):
        idx_map = {}
        for i, b in enumerate(bars):
            if b['date'] not in idx_map:
                idx_map[b['date']] = i
        return idx_map

    idx_15m = build_date_idx(tf_bars['15m'])
    idx_1h = build_date_idx(tf_bars['1h'])
    idx_4h = build_date_idx(tf_bars['4h'])

    trades = []
    daily_stats = []

    for date in dates:
        five_min = [b for b in data_by_date[date] if '09:30' <= b['time'] <= '09:55']
        if len(five_min) < 6:
            continue

        # Get index of first bar today in each timeframe
        i15 = idx_15m.get(date)
        i1h = idx_1h.get(date)
        i4h = idx_4h.get(date)
        if i15 is None or i1h is None or i4h is None:
            continue

        # Get unfilled PD arrays from lookback before today's open
        pd_15m = get_unfilled_pd_arrays(tf_bars['15m'], lookback_15m, i15)
        pd_1h = get_unfilled_pd_arrays(tf_bars['1h'], lookback_1h, i1h)
        pd_4h = get_unfilled_pd_arrays(tf_bars['4h'], lookback_4h, i4h)

        pd_by_tf = {'15m': pd_15m, '1h': pd_1h, '4h': pd_4h}

        # Find stacked levels
        stacked = find_stacked_levels(pd_by_tf, min_stack=min_stack)

        if not stacked:
            daily_stats.append({'date': date, 'levels': 0, 'traded': False})
            continue

        daily_stats.append({'date': date, 'levels': len(stacked), 'traded': False})

        # Price relative to levels — only consider levels near current price
        open_price = five_min[0]['open']
        price_range = open_price * 0.02  # levels within 2% of open
        nearby_levels = [
            s for s in stacked
            if abs(s['midpoint'] - open_price) < price_range
        ]

        if not nearby_levels:
            continue

        # Scan 9:30-10:00 bars for sweeps
        traded_today = False
        for bar_idx, bar in enumerate(five_min):
            if traded_today:
                break

            for level in nearby_levels:
                sweep_ext = detect_sweep(bar, level, level['type'])
                if sweep_ext is None:
                    continue

                # SWEEP DETECTED — enter trade
                if level['type'] == 'bullish':
                    direction = 'long'
                    entry = bar['close']       # enter at close of sweep bar
                    stop = sweep_ext           # stop below the sweep wick
                    risk = entry - stop
                    if risk <= 0:
                        continue
                    # Target: top of the stacked zone or 1R, whichever is closer
                    target_zone = level['top']
                    target_1r = entry + risk
                    target = min(target_zone, target_1r) if target_zone > entry else target_1r
                    if target <= entry:
                        continue
                else:
                    direction = 'short'
                    entry = bar['close']
                    stop = sweep_ext
                    risk = stop - entry
                    if risk <= 0:
                        continue
                    target_zone = level['bottom']
                    target_1r = entry - risk
                    target = max(target_zone, target_1r) if target_zone < entry else target_1r
                    if target >= entry:
                        continue

                # Simulate from next bar onward
                future = five_min[bar_idx + 1:]
                if not future:
                    continue

                pnl = simulate_trade(future, direction, entry, stop, target)
                if pnl is None:
                    continue

                trades.append({
                    'date': date,
                    'dir': 'L' if direction == 'long' else 'S',
                    'pnl': round(pnl, 2),
                    'risk': round(risk, 2),
                    'entry': round(entry, 2),
                    'stop': round(stop, 2),
                    'target': round(target, 2),
                    'tf_count': level['tf_count'],
                    'timeframes': ','.join(level['timeframes']),
                    'sweep_bar': bar['time'],
                })
                traded_today = True
                daily_stats[-1]['traded'] = True
                break  # one trade per day

    return trades, daily_stats


# ═══════════════════════════════════════════════════════════════════════
#  STATS & OUTPUT
# ═══════════════════════════════════════════════════════════════════════

def calc_stats(trades):
    if not trades:
        return None
    pnls = [t['pnl'] for t in trades]
    winners = [p for p in pnls if p > 0]
    losers = [p for p in pnls if p < 0]
    total = sum(pnls)
    avg = total / len(pnls)
    win_rate = len(winners) / len(pnls) * 100
    avg_win = sum(winners) / len(winners) if winners else 0
    avg_loss = sum(losers) / len(losers) if losers else 0
    gp = sum(winners)
    gl = abs(sum(losers))
    pf = gp / gl if gl > 0 else float('inf')
    r_vals = [t['pnl'] / t['risk'] if t['risk'] > 0 else 0 for t in trades]
    avg_r = sum(r_vals) / len(r_vals)
    eq = 0; peak = 0; max_dd = 0
    for p in pnls:
        eq += p; peak = max(peak, eq); max_dd = max(max_dd, peak - eq)
    return {
        'trades': len(pnls), 'winners': len(winners), 'losers': len(losers),
        'win_rate': win_rate, 'avg_win': avg_win, 'avg_loss': avg_loss,
        'pf': pf, 'total_pts': total, 'avg_pts': avg, 'max_dd': max_dd,
        'avg_r': avg_r,
    }


def print_stats(name, trades):
    s = calc_stats(trades)
    if not s:
        print(f"  {name}: No trades")
        return s
    longs = len([t for t in trades if t['dir'] == 'L'])
    shorts = len([t for t in trades if t['dir'] == 'S'])
    print(f"\n{'='*75}")
    print(f"  {name}")
    print(f"{'='*75}")
    print(f"  Trades: {s['trades']}  |  Longs: {longs}  |  Shorts: {shorts}")
    print(f"  Win Rate:    {s['win_rate']:.1f}%  ({s['winners']}W / {s['losers']}L)")
    print(f"  Avg Winner:  {s['avg_win']:+.1f} pts  |  Avg Loser: {s['avg_loss']:+.1f} pts")
    print(f"  Avg R:       {s['avg_r']:+.2f}R")
    print(f"  PF:          {s['pf']:.2f}")
    print(f"  Total P&L:   {s['total_pts']:+.1f} pts  (${s['total_pts']*5:+,.0f} MNQ)")
    print(f"  Avg/Trade:   {s['avg_pts']:+.1f} pts")
    print(f"  Max DD:      {s['max_dd']:.1f} pts")
    return s


# ═══════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_file = os.path.join(script_dir, "nq_5min.csv")
    repo_files = glob.glob(os.path.join(script_dir, "*/*/*_5m.csv"))

    # Run on both datasets
    datasets = []
    if os.path.exists(local_file):
        datasets.append(("Local (2025-2026, clean)", load_local_data(local_file)))
    if repo_files:
        datasets.append(("Repo (2010-2025, repaired)", load_repo_data_full(script_dir)))

    for source_name, raw_data in datasets:
        dates = sorted(raw_data.keys())
        print(f"\n{'#'*75}")
        print(f"#  ICT PD ARRAY STACKING — 9:30-10:00 NQ")
        print(f"#  Source: {source_name}")
        print(f"#  Days: {dates[0]} to {dates[-1]}  ({len(dates)} days)")
        print(f"{'#'*75}")

        # ── Run with min_stack = 2 (baseline) ─────────────────────────
        print(f"\n  Building timeframe bars and scanning for stacked PD arrays...")
        trades_2, stats_2 = run_pd_stacking_strategy(raw_data, min_stack=2)
        s2 = print_stats("PD STACKING: 2+ TF Overlap — ALL sweeps", trades_2)

        # ── Run with min_stack = 3 (strongest confluence) ─────────────
        trades_3, stats_3 = run_pd_stacking_strategy(raw_data, min_stack=3)
        s3 = print_stats("PD STACKING: 3 TF Overlap — ALL sweeps", trades_3)

        # ── FILTER: Skip 9:30 bar (let Judas play out) ────────────────
        filtered_no930 = [t for t in trades_2 if t['sweep_bar'] != '09:30']
        print_stats("FILTERED: 2+ TF, skip 9:30 bar (let Judas play out)", filtered_no930)

        # ── FILTER: 3-TF stacks + skip 9:30 ───────────────────────────
        filtered_3tf_no930 = [t for t in trades_2 if t['tf_count'] >= 3 and t['sweep_bar'] != '09:30']
        print_stats("FILTERED: 3 TF + skip 9:30", filtered_3tf_no930)

        # ── FILTER: Gap-aligned only (sweep direction matches gap fill) ─
        # Build gap info per date
        gap_info = {}
        prev_close = None
        for d in sorted(raw_data.keys()):
            rth = [b for b in raw_data[d] if '09:30' <= b['time'] <= '15:55']
            if rth and prev_close is not None:
                gap = rth[0]['open'] - prev_close
                gap_pct = gap / prev_close * 100
                # Gap up → expect fill down → bearish. Gap down → expect fill up → bullish.
                gap_info[d] = 'S' if gap_pct > 0.05 else ('L' if gap_pct < -0.05 else None)
            if rth:
                prev_close = rth[-1]['close']

        gap_aligned = [t for t in trades_2
                       if t['date'] in gap_info and gap_info[t['date']] == t['dir']]
        print_stats("FILTERED: 2+ TF, gap-aligned direction only", gap_aligned)

        # ── COMBINED FILTER: 3-TF + skip 9:30 + gap-aligned ───────────
        best_filter = [t for t in trades_2
                       if t['tf_count'] >= 3
                       and t['sweep_bar'] != '09:30'
                       and t['date'] in gap_info
                       and gap_info[t['date']] == t['dir']]
        print_stats("BEST: 3 TF + skip 9:30 + gap-aligned", best_filter)

        # ── Breakdown by stack count for min_stack=2 ──────────────────
        if trades_2:
            tf2_trades = [t for t in trades_2 if t['tf_count'] == 2]
            tf3_trades = [t for t in trades_2 if t['tf_count'] >= 3]
            print(f"\n{'='*75}")
            print(f"  BREAKDOWN BY STACK DEPTH")
            print(f"{'='*75}")
            for label, subset in [("2-TF stacks only", tf2_trades), ("3-TF stacks", tf3_trades)]:
                s = calc_stats(subset)
                if s:
                    print(f"  {label:<22} Trades:{s['trades']:>4}  Win%:{s['win_rate']:>5.1f}%  "
                          f"PF:{s['pf']:>5.2f}  AvgR:{s['avg_r']:>+5.2f}R  "
                          f"Total:{s['total_pts']:>+8.1f}")
                else:
                    print(f"  {label:<22} No trades")

        # ── Breakdown by direction ────────────────────────────────────
        if trades_2:
            long_trades = [t for t in trades_2 if t['dir'] == 'L']
            short_trades = [t for t in trades_2 if t['dir'] == 'S']
            print(f"\n  BREAKDOWN BY DIRECTION")
            for label, subset in [("Longs", long_trades), ("Shorts", short_trades)]:
                s = calc_stats(subset)
                if s:
                    print(f"  {label:<22} Trades:{s['trades']:>4}  Win%:{s['win_rate']:>5.1f}%  "
                          f"PF:{s['pf']:>5.2f}  AvgR:{s['avg_r']:>+5.2f}R  "
                          f"Total:{s['total_pts']:>+8.1f}")

        # ── Breakdown by sweep bar time ───────────────────────────────
        if trades_2:
            print(f"\n  BREAKDOWN BY SWEEP TIME")
            by_time = defaultdict(list)
            for t in trades_2:
                by_time[t['sweep_bar']].append(t)
            for time_str in sorted(by_time.keys()):
                s = calc_stats(by_time[time_str])
                if s:
                    print(f"  {time_str:<8} Trades:{s['trades']:>4}  Win%:{s['win_rate']:>5.1f}%  "
                          f"PF:{s['pf']:>5.2f}  Total:{s['total_pts']:>+8.1f}")

        # ── Yearly breakdown ──────────────────────────────────────────
        if trades_2:
            yearly = defaultdict(list)
            for t in trades_2:
                yearly[t['date'][:4]].append(t)
            print(f"\n{'='*75}")
            print(f"  YEARLY BREAKDOWN — 2+ TF Stacked Sweeps")
            print(f"{'='*75}")
            print(f"  {'Year':<6} {'Trades':>6} {'Win%':>7} {'PF':>7} {'AvgR':>7} {'Total':>10} {'Cum':>10}")
            print(f"  {'-'*55}")
            cum = 0
            for year in sorted(yearly.keys()):
                s = calc_stats(yearly[year])
                cum += s['total_pts']
                print(f"  {year:<6} {s['trades']:>6} {s['win_rate']:>6.1f}% {s['pf']:>7.2f} "
                      f"{s['avg_r']:>+6.2f}R {s['total_pts']:>+10.1f} {cum:>+10.1f}")

        # ── Level utilization stats ───────────────────────────────────
        if stats_2:
            days_with_levels = [d for d in stats_2 if d['levels'] > 0]
            days_traded = [d for d in stats_2 if d['traded']]
            avg_levels = sum(d['levels'] for d in days_with_levels) / len(days_with_levels) if days_with_levels else 0
            print(f"\n  LEVEL STATS")
            print(f"  Days with stacked levels: {len(days_with_levels)}/{len(stats_2)} "
                  f"({len(days_with_levels)/len(stats_2)*100:.0f}%)")
            print(f"  Days with sweep trades:   {len(days_traded)}/{len(stats_2)} "
                  f"({len(days_traded)/len(stats_2)*100:.0f}%)")
            print(f"  Avg stacked levels/day:   {avg_levels:.1f}")

        # ── Sample trades ─────────────────────────────────────────────
        if trades_2:
            print(f"\n  SAMPLE TRADES (first 10)")
            print(f"  {'Date':<12} {'Dir':>3} {'Entry':>10} {'Stop':>10} {'Target':>10} "
                  f"{'P&L':>8} {'TFs':>4} {'Sweep':>6}")
            for t in trades_2[:10]:
                print(f"  {t['date']:<12} {t['dir']:>3} {t['entry']:>10.2f} {t['stop']:>10.2f} "
                      f"{t['target']:>10.2f} {t['pnl']:>+8.2f} {t['tf_count']:>4} {t['sweep_bar']:>6}")

    print()
