"""
ICT 9:30-10:00 NQ Futures Strategy
====================================
Based on Inner Circle Trader concepts researched from quagmyre.com wiki:
  - Opening Range Gap (ORG) Fill to Consequent Encroachment
  - Judas Swing (manipulation phase of Power of 3)
  - Fair Value Gaps (FVG) as precision entry
  - Market Structure Shift (MSS) + Displacement as confirmation
  - Liquidity sweep of previous day levels

Strategies:
  1. ICT ORG Gap Fill — trade toward mid-gap after Judas swing + FVG
  2. ICT Judas Swing Fade — detect fake move, enter on MSS + FVG
  3. ICT PO3 Displacement — enter on displacement FVG after manipulation
  4. ICT Liquidity Sweep + FVG — sweep of prev day H/L then FVG entry

All trades enter AND exit within 9:30-10:00 (6 five-minute bars).
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
    """Load clean local CSV (time,open,high,low,close). Returns full RTH."""
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
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
            })
    return data


def load_repo_data(base_dir):
    """Load repo monthly 5m CSVs. Returns bars for 9:25-10:05 AND 15:50-16:00."""
    data = OrderedDict()
    files = sorted(glob.glob(os.path.join(base_dir, "*/*/*_5m.csv")))
    print(f"  Loading {len(files)} monthly files...")
    for fpath in files:
        with open(fpath) as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts = row['ts_event'][:19]
                dt = datetime.fromisoformat(ts)
                month = dt.month
                offset = 4 if 3 <= month <= 10 else 5
                dt_et = dt - timedelta(hours=offset)
                time_str = dt_et.strftime('%H:%M')
                # Load opening window + end of day for prev close
                if not (('09:25' <= time_str <= '10:05') or ('15:50' <= time_str <= '15:55')):
                    continue
                date_str = dt_et.strftime('%Y-%m-%d')
                o = float(row['open'])
                h = float(row['high'])
                l = float(row['low'])
                c = float(row['close'])
                # Repair corrupted values
                ref = max(v for v in [o, h, l, c] if v > 0) if any(v > 0 for v in [o, h, l, c]) else 0
                if ref <= 0:
                    continue
                threshold = ref * 0.5
                if o <= 0 or o < threshold:
                    o = c if c >= threshold else ref
                if c <= 0 or c < threshold:
                    c = o if o >= threshold else ref
                if l <= 0 or l < threshold:
                    l = min(o, c)
                if h <= 0 or h < threshold:
                    h = max(o, c)
                if h < l:
                    h, l = l, h
                if date_str not in data:
                    data[date_str] = []
                data[date_str].append({
                    'time': time_str,
                    'open': o, 'high': h, 'low': l, 'close': c,
                })
    for d in data:
        data[d].sort(key=lambda b: b['time'])
    return data


def prepare_days(raw_data):
    """Build list of trading days with: window bars, prev_close, prev_high, prev_low."""
    dates = sorted(raw_data.keys())
    days = []
    for i, date in enumerate(dates):
        bars = raw_data[date]
        window = [b for b in bars if '09:30' <= b['time'] <= '09:55']
        if len(window) < 6:
            continue

        # Find previous day's close, high, low
        prev_close = None
        prev_high = None
        prev_low = None
        for j in range(i - 1, max(i - 5, -1), -1):
            if j < 0:
                break
            prev_bars = raw_data[dates[j]]
            rth = [b for b in prev_bars if '09:30' <= b['time'] <= '15:55']
            if rth:
                prev_close = rth[-1]['close']
                prev_high = max(b['high'] for b in rth)
                prev_low = min(b['low'] for b in rth)
                break
            # Try settlement bar
            settle = [b for b in prev_bars if b['time'] == '15:55']
            if settle:
                prev_close = settle[0]['close']
                prev_high = max(b['high'] for b in prev_bars if '09:30' <= b['time'] <= '15:55') if any('09:30' <= b['time'] <= '15:55' for b in prev_bars) else settle[0]['high']
                prev_low = min(b['low'] for b in prev_bars if '09:30' <= b['time'] <= '15:55') if any('09:30' <= b['time'] <= '15:55' for b in prev_bars) else settle[0]['low']
                break

        if prev_close is None:
            continue

        days.append({
            'date': date,
            'bars': window,  # 6 bars: 9:30, 9:35, 9:40, 9:45, 9:50, 9:55
            'open': window[0]['open'],
            'prev_close': prev_close,
            'prev_high': prev_high,
            'prev_low': prev_low,
            'gap': window[0]['open'] - prev_close,
            'gap_pct': (window[0]['open'] - prev_close) / prev_close * 100,
        })
    return days


# ═══════════════════════════════════════════════════════════════════════
#  ICT BUILDING BLOCKS
# ═══════════════════════════════════════════════════════════════════════

def find_fvgs(bars, start_idx=0):
    """Find all Fair Value Gaps in a bar sequence.
    Bullish FVG: candle3.low > candle1.high (gap above)
    Bearish FVG: candle3.high < candle1.low (gap below)
    """
    fvgs = []
    for i in range(max(2, start_idx), len(bars)):
        c1, c2, c3 = bars[i - 2], bars[i - 1], bars[i]
        # Bullish FVG
        if c3['low'] > c1['high']:
            fvgs.append({
                'type': 'bullish',
                'top': c3['low'],
                'bottom': c1['high'],
                'midpoint': (c3['low'] + c1['high']) / 2,
                'bar_idx': i,
                'candle2': c2,
            })
        # Bearish FVG
        if c3['high'] < c1['low']:
            fvgs.append({
                'type': 'bearish',
                'top': c1['low'],
                'bottom': c3['high'],
                'midpoint': (c1['low'] + c3['high']) / 2,
                'bar_idx': i,
                'candle2': c2,
            })
    return fvgs


def is_displacement(bar, avg_range):
    """Check if a bar shows displacement (large body relative to range)."""
    body = abs(bar['close'] - bar['open'])
    rng = bar['high'] - bar['low']
    if rng <= 0:
        return False
    body_ratio = body / rng
    # Displacement = body dominant AND large relative to average
    return body_ratio > 0.5 and rng > avg_range * 0.8


def detect_mss(bars, direction):
    """Detect Market Structure Shift.
    Bullish MSS: after making lows, price breaks above a prior swing high.
    Bearish MSS: after making highs, price breaks below a prior swing low.
    Returns (mss_bar_idx, swing_level) or None.
    """
    if len(bars) < 3:
        return None

    if direction == 'bullish':
        # Look for: bars making lower lows, then a bar closes above a prior bar's high
        lowest_low_idx = 0
        for i in range(1, len(bars)):
            if bars[i]['low'] < bars[lowest_low_idx]['low']:
                lowest_low_idx = i
        # After the lowest low, find a bar that closes above the high of bars before the low
        swing_high = max(bars[j]['high'] for j in range(lowest_low_idx + 1))
        for i in range(lowest_low_idx + 1, len(bars)):
            if bars[i]['close'] > swing_high:
                return (i, swing_high)
    else:
        # Bearish MSS: bars making higher highs, then close below a prior low
        highest_high_idx = 0
        for i in range(1, len(bars)):
            if bars[i]['high'] > bars[highest_high_idx]['high']:
                highest_high_idx = i
        swing_low = min(bars[j]['low'] for j in range(highest_high_idx + 1))
        for i in range(highest_high_idx + 1, len(bars)):
            if bars[i]['close'] < swing_low:
                return (i, swing_low)
    return None


def get_avg_range(bars):
    """Average bar range for normalization."""
    ranges = [b['high'] - b['low'] for b in bars if b['high'] - b['low'] > 0]
    return sum(ranges) / len(ranges) if ranges else 1


def simulate_limit_trade(future_bars, direction, entry, stop, target):
    """Simulate a LIMIT ORDER trade. Entry must fill first, then track stop/target.
    For long: limit buy at entry → bar.low must reach entry to fill.
    For short: limit sell at entry → bar.high must reach entry to fill.
    Returns PnL or None if entry never fills.
    """
    if not future_bars:
        return None
    filled = False
    for bar in future_bars:
        if not filled:
            # Check if limit order fills this bar
            fills = False
            if direction == 'long' and bar['low'] <= entry:
                fills = True
            elif direction == 'short' and bar['high'] >= entry:
                fills = True
            if not fills:
                continue
            filled = True
            # Entry fills this bar — check stop/target in same bar (conservative: stop priority)
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
        else:
            # Already filled, check stop/target
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
    if not filled:
        return None  # limit order never filled
    # EOD exit (within window)
    eod = future_bars[-1]['close']
    return (eod - entry) if direction == 'long' else (entry - eod)


def simulate_market_trade(future_bars, direction, entry, stop, target):
    """Simulate a MARKET ORDER trade (fills immediately at entry price)."""
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
#  STRATEGY 1: ICT Opening Range Gap Fill (1st FVG to ORG CE)
#  ─────────────────────────────────────────────────────────────────────
#  Core ICT model: 70% of the time, price trades to mid-gap within 30 min.
#  - Measure gap: today's open vs yesterday's close
#  - Gap up → expect fill DOWN → bearish bias
#  - Gap down → expect fill UP → bullish bias
#  - Wait for Judas swing (extends gap direction), then MSS reversal
#  - Enter at first FVG formed in gap-fill direction
#  - Target: consequent encroachment (50% of gap)
#  - Stop: beyond the FVG's candle 1 extreme
# ═══════════════════════════════════════════════════════════════════════

def strat_1_org_gap_fill(days, min_gap_pct=0.1):
    trades = []
    for day in days:
        bars = day['bars']
        gap = day['gap']
        gap_pct = day['gap_pct']
        open_price = day['open']
        prev_close = day['prev_close']

        # Need meaningful gap (min ~0.1% of price)
        if abs(gap_pct) < min_gap_pct:
            continue

        # Gap up → bearish fill. Gap down → bullish fill.
        if gap > 0:
            fill_dir = 'bearish'
            mid_gap = open_price - abs(gap) * 0.5  # target below open
        else:
            fill_dir = 'bullish'
            mid_gap = open_price + abs(gap) * 0.5  # target above open

        # Look for FVG in fill direction after bar 1 (allow Judas on bar 0-1)
        fvgs = find_fvgs(bars, start_idx=2)
        target_fvg = None
        for fvg in fvgs:
            if fvg['type'] == fill_dir.replace('ish', '').replace('bear', 'bearish').replace('bull', 'bullish'):
                pass
            if (fill_dir == 'bearish' and fvg['type'] == 'bearish') or \
               (fill_dir == 'bullish' and fvg['type'] == 'bullish'):
                target_fvg = fvg
                break

        if target_fvg is None:
            continue

        fvg_idx = target_fvg['bar_idx']

        if fill_dir == 'bearish':
            entry = target_fvg['top']  # limit sell at top of bearish FVG
            stop = bars[fvg_idx - 2]['high']  # candle 1 high
            target = mid_gap
            direction = 'short'
            if entry <= target or stop <= entry:
                continue
        else:
            entry = target_fvg['bottom']  # limit buy at bottom of bullish FVG
            stop = bars[fvg_idx - 2]['low']  # candle 1 low
            target = mid_gap
            direction = 'long'
            if entry >= target or stop >= entry:
                continue

        risk = abs(entry - stop)
        if risk <= 0:
            continue

        future_bars = bars[fvg_idx + 1:] if fvg_idx + 1 < len(bars) else []
        if not future_bars:
            continue

        pnl = simulate_limit_trade(future_bars, direction, entry, stop, target)
        if pnl is None:
            continue  # limit order never filled

        trades.append({
            'date': day['date'], 'dir': 'L' if direction == 'long' else 'S',
            'pnl': round(pnl, 2), 'risk': risk,
            'gap_pct': gap_pct, 'entry': entry,
        })
    return trades


# ═══════════════════════════════════════════════════════════════════════
#  STRATEGY 2: ICT Judas Swing Fade
#  ─────────────────────────────────────────────────────────────────────
#  Detect the fake move at 9:30 (Judas swing), wait for MSS + FVG,
#  then enter in the true direction.
#  - If first 2-3 bars rally (while gap is up or flat) → Judas up → short
#  - If first 2-3 bars drop (while gap is down or flat) → Judas down → long
#  - Confirmation: MSS (break of swing point in opposite direction)
#  - Entry: first FVG after MSS
#  - Target: opposite extreme of Judas swing
#  - Stop: beyond Judas swing extreme
# ═══════════════════════════════════════════════════════════════════════

def strat_2_judas_swing(days):
    trades = []
    for day in days:
        bars = day['bars']
        open_price = day['open']

        # Detect Judas direction from first 2-3 bars
        judas_bars = bars[:3]
        judas_high = max(b['high'] for b in judas_bars)
        judas_low = min(b['low'] for b in judas_bars)
        judas_close = judas_bars[-1]['close']  # close of 9:40 bar

        # Judas went UP if close > open significantly
        # Judas went DOWN if close < open significantly
        move_pct = (judas_close - open_price) / open_price * 100

        if abs(move_pct) < 0.05:
            continue  # no clear Judas swing

        if move_pct > 0:
            # Judas went UP → true direction is DOWN
            true_dir = 'short'
            # MSS: need a bar that closes below judas swing low
            mss = detect_mss(bars[:], 'bearish')
        else:
            # Judas went DOWN → true direction is UP
            true_dir = 'long'
            mss = detect_mss(bars[:], 'bullish')

        if mss is None:
            continue

        mss_idx, _ = mss

        # Find FVG after MSS in true direction
        fvgs = find_fvgs(bars, start_idx=max(2, mss_idx))
        target_fvg = None
        for fvg in fvgs:
            if (true_dir == 'long' and fvg['type'] == 'bullish') or \
               (true_dir == 'short' and fvg['type'] == 'bearish'):
                target_fvg = fvg
                break

        use_limit = False
        if target_fvg is None:
            # No FVG found — market entry at MSS bar close
            fvg_idx = mss_idx
            if true_dir == 'long':
                entry = bars[mss_idx]['close']
                stop = judas_low
                target = judas_high
            else:
                entry = bars[mss_idx]['close']
                stop = judas_high
                target = judas_low
        else:
            # FVG found — limit entry at FVG level
            use_limit = True
            fvg_idx = target_fvg['bar_idx']
            if true_dir == 'long':
                entry = target_fvg['bottom']
                stop = judas_low
                target = judas_high
            else:
                entry = target_fvg['top']
                stop = judas_high
                target = judas_low

        risk = abs(entry - stop)
        if risk <= 0:
            continue

        if true_dir == 'long' and (entry >= target or stop >= entry):
            continue
        if true_dir == 'short' and (entry <= target or stop <= entry):
            continue

        future_bars = bars[fvg_idx + 1:] if fvg_idx + 1 < len(bars) else []
        if not future_bars:
            continue

        if use_limit:
            pnl = simulate_limit_trade(future_bars, true_dir, entry, stop, target)
            if pnl is None:
                continue  # limit never filled
        else:
            pnl = simulate_market_trade(future_bars, true_dir, entry, stop, target)

        trades.append({
            'date': day['date'], 'dir': 'L' if true_dir == 'long' else 'S',
            'pnl': round(pnl, 2), 'risk': risk,
        })
    return trades


# ═══════════════════════════════════════════════════════════════════════
#  STRATEGY 3: ICT Power of 3 — Displacement Entry
#  ─────────────────────────────────────────────────────────────────────
#  Pure displacement model:
#  - Wait for the first displacement candle (large body, body > 50% of range)
#  - If displacement is in gap-fill direction → enter at next FVG
#  - If no gap context, use displacement direction as bias
#  - Target: 1R (risk = reward)
#  - Stop: beyond displacement candle extreme
# ═══════════════════════════════════════════════════════════════════════

def strat_3_po3_displacement(days):
    trades = []
    for day in days:
        bars = day['bars']
        avg_rng = get_avg_range(bars)

        # Skip bar 0 (let accumulation/manipulation play out)
        # Look for first displacement bar from bar 1 onward
        disp_idx = None
        disp_dir = None
        for i in range(1, len(bars)):
            bar = bars[i]
            if is_displacement(bar, avg_rng):
                if bar['close'] > bar['open']:
                    disp_dir = 'long'
                else:
                    disp_dir = 'short'
                disp_idx = i
                break

        if disp_idx is None or disp_idx >= len(bars) - 1:
            continue

        # Check if displacement aligns with gap fill (higher probability)
        gap = day['gap']
        gap_aligned = False
        if (gap > 0 and disp_dir == 'short') or (gap < 0 and disp_dir == 'long'):
            gap_aligned = True

        # Find FVG at or after displacement
        fvgs = find_fvgs(bars, start_idx=max(2, disp_idx))
        target_fvg = None
        for fvg in fvgs:
            if (disp_dir == 'long' and fvg['type'] == 'bullish') or \
               (disp_dir == 'short' and fvg['type'] == 'bearish'):
                target_fvg = fvg
                break

        use_limit = False
        if target_fvg is not None:
            use_limit = True
            fvg_idx = target_fvg['bar_idx']
            if disp_dir == 'long':
                entry = target_fvg['bottom']
                stop = bars[fvg_idx - 2]['low']
            else:
                entry = target_fvg['top']
                stop = bars[fvg_idx - 2]['high']
        else:
            # No FVG — market entry at displacement bar close
            fvg_idx = disp_idx
            disp_bar = bars[disp_idx]
            entry = disp_bar['close']
            if disp_dir == 'long':
                stop = disp_bar['low']
            else:
                stop = disp_bar['high']

        risk = abs(entry - stop)
        if risk <= 0:
            continue

        if gap_aligned and abs(gap) > risk:
            target_price = day['open'] - gap * 0.5 if gap > 0 else day['open'] + abs(gap) * 0.5
        else:
            target_price = entry + risk if disp_dir == 'long' else entry - risk

        if disp_dir == 'long' and (entry >= target_price or stop >= entry):
            continue
        if disp_dir == 'short' and (entry <= target_price or stop <= entry):
            continue

        future_bars = bars[fvg_idx + 1:] if fvg_idx + 1 < len(bars) else []
        if not future_bars:
            continue

        if use_limit:
            pnl = simulate_limit_trade(future_bars, disp_dir, entry, stop, target_price)
            if pnl is None:
                continue
        else:
            pnl = simulate_market_trade(future_bars, disp_dir, entry, stop, target_price)

        trades.append({
            'date': day['date'], 'dir': 'L' if disp_dir == 'long' else 'S',
            'pnl': round(pnl, 2), 'risk': risk,
            'gap_aligned': gap_aligned,
        })
    return trades


# ═══════════════════════════════════════════════════════════════════════
#  STRATEGY 4: ICT Liquidity Sweep + FVG
#  ─────────────────────────────────────────────────────────────────────
#  - If price sweeps below previous day's low in first 2-3 bars → buy setup
#  - If price sweeps above previous day's high → sell setup
#  - Confirmation: displacement candle reversing from the sweep
#  - Entry: FVG formed in reversal direction
#  - Target: opening price or 1R
#  - Stop: beyond the sweep extreme
# ═══════════════════════════════════════════════════════════════════════

def strat_4_liquidity_sweep(days):
    trades = []
    for day in days:
        bars = day['bars']
        prev_high = day['prev_high']
        prev_low = day['prev_low']
        open_price = day['open']

        if prev_high is None or prev_low is None:
            continue

        # Check if early bars sweep prev day high or low
        sweep_dir = None
        sweep_bar_idx = None
        sweep_extreme = None

        for i in range(min(4, len(bars))):
            bar = bars[i]
            if bar['low'] < prev_low and bar['close'] > prev_low:
                # Swept below prev low but closed back above = buy signal
                sweep_dir = 'long'
                sweep_bar_idx = i
                sweep_extreme = bar['low']
                break
            elif bar['high'] > prev_high and bar['close'] < prev_high:
                # Swept above prev high but closed back below = sell signal
                sweep_dir = 'short'
                sweep_bar_idx = i
                sweep_extreme = bar['high']
                break

        if sweep_dir is None:
            continue

        # Look for FVG after sweep
        fvgs = find_fvgs(bars, start_idx=max(2, sweep_bar_idx + 1))
        target_fvg = None
        for fvg in fvgs:
            if (sweep_dir == 'long' and fvg['type'] == 'bullish') or \
               (sweep_dir == 'short' and fvg['type'] == 'bearish'):
                target_fvg = fvg
                break

        use_limit = False
        if target_fvg is not None:
            use_limit = True
            fvg_idx = target_fvg['bar_idx']
            if sweep_dir == 'long':
                entry = target_fvg['bottom']
            else:
                entry = target_fvg['top']
        else:
            fvg_idx = min(sweep_bar_idx + 1, len(bars) - 1)
            entry = bars[fvg_idx]['close']

        stop = sweep_extreme
        risk = abs(entry - stop)
        if risk <= 0:
            continue

        if sweep_dir == 'long':
            target_1r = entry + risk
            target_open = open_price
            target = min(target_1r, target_open) if target_open > entry else target_1r
            if target <= entry or stop >= entry:
                continue
        else:
            target_1r = entry - risk
            target_open = open_price
            target = max(target_1r, target_open) if target_open < entry else target_1r
            if target >= entry or stop <= entry:
                continue

        future_bars = bars[fvg_idx + 1:] if fvg_idx + 1 < len(bars) else []
        if not future_bars:
            continue

        if use_limit:
            pnl = simulate_limit_trade(future_bars, sweep_dir, entry, stop, target)
            if pnl is None:
                continue
        else:
            pnl = simulate_market_trade(future_bars, sweep_dir, entry, stop, target)

        trades.append({
            'date': day['date'], 'dir': 'L' if sweep_dir == 'long' else 'S',
            'pnl': round(pnl, 2), 'risk': risk,
        })
    return trades


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
    # R-values
    r_vals = [t['pnl'] / t['risk'] if t['risk'] > 0 else 0 for t in trades]
    avg_r = sum(r_vals) / len(r_vals)
    # Max drawdown
    eq = 0; peak = 0; max_dd = 0
    for p in pnls:
        eq += p; peak = max(peak, eq); max_dd = max(max_dd, peak - eq)
    return {
        'trades': len(pnls), 'winners': len(winners), 'losers': len(losers),
        'win_rate': win_rate, 'avg_win': avg_win, 'avg_loss': avg_loss,
        'pf': pf, 'total_pts': total, 'avg_pts': avg, 'max_dd': max_dd,
        'avg_r': avg_r,
    }


def print_strat(name, desc, trades):
    s = calc_stats(trades)
    if not s:
        print(f"\n{'='*75}\n  {name}\n  {desc}\n{'='*75}\n  No trades.\n")
        return s
    longs = len([t for t in trades if t.get('dir') == 'L'])
    shorts = len([t for t in trades if t.get('dir') == 'S'])
    print(f"\n{'='*75}")
    print(f"  {name}")
    print(f"  {desc}")
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

    # Try repo data first, fall back to local
    repo_files = glob.glob(os.path.join(script_dir, "*/*/*_5m.csv"))
    local_file = os.path.join(script_dir, "nq_5min.csv")

    if repo_files:
        raw = load_repo_data(script_dir)
        source = "Repo (2010-2025)"
    elif os.path.exists(local_file):
        raw = load_local_data(local_file)
        source = "Local (2025-2026)"
    else:
        print("No data found!")
        exit(1)

    days = prepare_days(raw)
    dates = [d['date'] for d in days]

    print(f"\n{'#'*75}")
    print(f"#  ICT 9:30-10:00 NQ STRATEGY BACKTEST")
    print(f"#  Source: {source}")
    print(f"#  Days: {dates[0]} to {dates[-1]}  ({len(days)} trading days)")
    print(f"#  All trades enter & exit within 9:30-10:00")
    print(f"{'#'*75}")

    # Show gap distribution
    gaps = [d['gap_pct'] for d in days]
    gap_up = len([g for g in gaps if g > 0.1])
    gap_down = len([g for g in gaps if g < -0.1])
    gap_flat = len(days) - gap_up - gap_down
    print(f"\n  Gap distribution: {gap_up} up ({gap_up/len(days)*100:.0f}%) | "
          f"{gap_down} down ({gap_down/len(days)*100:.0f}%) | "
          f"{gap_flat} flat ({gap_flat/len(days)*100:.0f}%)")

    results = {}
    all_trades = {}

    t1 = strat_1_org_gap_fill(days, min_gap_pct=0.1)
    results['1'] = print_strat(
        "STRATEGY 1: ICT ORG Gap Fill (1st FVG → Mid-Gap)",
        "Gap > 0.1% → Judas extends gap → FVG in fill direction → target mid-gap", t1)
    all_trades['1'] = t1

    t2 = strat_2_judas_swing(days)
    results['2'] = print_strat(
        "STRATEGY 2: ICT Judas Swing Fade",
        "Detect fake move in first 3 bars → MSS + FVG → target Judas extreme", t2)
    all_trades['2'] = t2

    t3 = strat_3_po3_displacement(days)
    results['3'] = print_strat(
        "STRATEGY 3: ICT PO3 Displacement Entry",
        "First displacement candle after bar 0 → FVG entry → 1R or mid-gap", t3)
    all_trades['3'] = t3

    t4 = strat_4_liquidity_sweep(days)
    results['4'] = print_strat(
        "STRATEGY 4: ICT Liquidity Sweep + FVG",
        "Sweep prev day H/L with rejection → FVG entry → target open or 1R", t4)
    all_trades['4'] = t4

    # ── Comparison ─────────────────────────────────────────────────────
    print(f"\n\n{'='*75}")
    print(f"  COMPARISON TABLE")
    print(f"{'='*75}")
    print(f"  {'Strategy':<40} {'Trades':>6} {'Win%':>7} {'AvgR':>7} {'PF':>7} {'Total':>10} {'MaxDD':>9}")
    print(f"  {'-'*86}")

    labels = {
        '1': '1: ORG Gap Fill (FVG→MidGap)',
        '2': '2: Judas Swing Fade',
        '3': '3: PO3 Displacement',
        '4': '4: Liquidity Sweep + FVG',
    }
    for key in ['1', '2', '3', '4']:
        s = results[key]
        if s:
            print(f"  {labels[key]:<40} {s['trades']:>6} {s['win_rate']:>6.1f}% {s['avg_r']:>+6.2f}R {s['pf']:>7.2f} {s['total_pts']:>+10.1f} {s['max_dd']:>9.1f}")
        else:
            print(f"  {labels[key]:<40}    N/A")

    # ── Yearly breakdown for best ──────────────────────────────────────
    best_key = max(results.keys(), key=lambda k: results[k]['total_pts'] if results[k] else float('-inf'))
    best_trades = all_trades[best_key]
    best_label = labels[best_key]

    print(f"\n\n{'='*75}")
    print(f"  YEARLY BREAKDOWN — {best_label}")
    print(f"{'='*75}")

    yearly = defaultdict(list)
    for t in best_trades:
        year = t['date'][:4]
        yearly[year].append(t)

    print(f"\n  {'Year':<6} {'Trades':>6} {'Win%':>7} {'PF':>7} {'AvgR':>7} {'Total Pts':>10} {'Cumulative':>12}")
    print(f"  {'-'*61}")
    cum = 0
    for year in sorted(yearly.keys()):
        yr_trades = yearly[year]
        s = calc_stats(yr_trades)
        cum += s['total_pts']
        print(f"  {year:<6} {s['trades']:>6} {s['win_rate']:>6.1f}% {s['pf']:>7.2f} {s['avg_r']:>+6.2f}R {s['total_pts']:>+10.1f} {cum:>+12.1f}")

    # ── Gap-aligned filter for Strategy 3 ──────────────────────────────
    if '3' in all_trades and all_trades['3']:
        gap_aligned = [t for t in all_trades['3'] if t.get('gap_aligned')]
        if gap_aligned:
            print(f"\n\n{'='*75}")
            print(f"  STRATEGY 3 — GAP-ALIGNED SUBSET ONLY")
            print(f"{'='*75}")
            s = calc_stats(gap_aligned)
            if s:
                print(f"  Trades: {s['trades']}  Win%: {s['win_rate']:.1f}%  PF: {s['pf']:.2f}  "
                      f"AvgR: {s['avg_r']:+.2f}R  Total: {s['total_pts']:+.1f} pts")

    print()
