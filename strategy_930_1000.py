"""
NQ 9:30–10:00 Opening Window Strategy
======================================
Trades ONLY within the first 30 minutes of RTH.
All positions closed by 10:00.

Strategies tested:
  A) First 5-min bar reversal — fade the opening 9:30 candle direction
  B) Opening range breakout (5-min OR) — break of 9:30 candle H/L
  C) 3-bar momentum — enter in direction of first 3 bars (9:30–9:45)
  D) Mean reversion — if price moves X pts in first 10 min, fade it
  E) VWAP-style: first candle sets bias, enter on pullback

Data: 5-minute NQ futures bars
"""

import csv
import os
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta


# ── Load Data ──────────────────────────────────────────────────────────
def load_data(path="nq_5min.csv"):
    """Load from single CSV (old local format: time,open,high,low,close)."""
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
    """Load all *_5m.csv files from the repo (format: ts_event,open,high,low,close,...)."""
    import glob
    data = OrderedDict()
    files = sorted(glob.glob(os.path.join(base_dir, "*/*/*_5m.csv")))
    print(f"  Loading {len(files)} monthly files...")

    for fpath in files:
        with open(fpath) as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts = row['ts_event']
                # Format: 2010-06-06T22:00:00.000000000Z — parse as UTC
                dt_str = ts[:19]  # trim nanoseconds and Z
                dt = datetime.fromisoformat(dt_str)
                # Convert UTC to US/Eastern (ET): UTC-4 during EDT, UTC-5 during EST
                # Approximate: Mar-Nov = EDT (UTC-4), Nov-Mar = EST (UTC-5)
                month = dt.month
                if 3 <= month <= 10:
                    offset_hours = 4  # EDT
                elif month == 11:
                    offset_hours = 5  # EST (approximate)
                elif month == 2:
                    offset_hours = 5  # EST
                else:
                    offset_hours = 5  # EST (Nov-Mar)
                dt_et = dt - timedelta(hours=offset_hours)

                date_str = dt_et.strftime('%Y-%m-%d')
                time_str = dt_et.strftime('%H:%M')

                # Only keep RTH window bars (9:25 to 10:05 to be safe)
                if not ('09:25' <= time_str <= '10:05'):
                    continue

                o = float(row['open'])
                h = float(row['high'])
                l = float(row['low'])
                c = float(row['close'])

                # Data quality: repair corrupted OHLC values
                # Common issues: truncated lows (230.75 when ~23075),
                # negative values (-1.8 when ~1838)
                ref = max(v for v in [o, h, l, c] if v > 0) if any(v > 0 for v in [o, h, l, c]) else 0
                if ref <= 0:
                    continue

                # Repair each field if it's way off from the reference
                threshold = ref * 0.5
                if o <= 0 or o < threshold:
                    o = c if c >= threshold else ref
                if c <= 0 or c < threshold:
                    c = o if o >= threshold else ref
                if l <= 0 or l < threshold:
                    l = min(o, c)  # conservative: use min of open/close
                if h <= 0 or h < threshold:
                    h = max(o, c)
                if h < l:
                    h, l = l, h

                if date_str not in data:
                    data[date_str] = []
                data[date_str].append({
                    'time': time_str,
                    'open': o,
                    'high': h,
                    'low': l,
                    'close': c,
                })

    # Sort bars within each day
    for date_str in data:
        data[date_str].sort(key=lambda b: b['time'])

    return data


def get_window_bars(bars):
    """Return bars from 9:30 to 9:55 (6 bars, all closing by 10:00)."""
    return [b for b in bars if '09:30' <= b['time'] <= '09:55']


# ── Stats Engine ───────────────────────────────────────────────────────
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
    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    pf = gross_profit / gross_loss if gross_loss > 0 else float('inf')

    # Max drawdown
    equity = 0
    peak = 0
    max_dd = 0
    for p in pnls:
        equity += p
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)

    return {
        'trades': len(pnls),
        'winners': len(winners),
        'losers': len(losers),
        'win_rate': win_rate,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'pf': pf,
        'total_pts': total,
        'avg_pts': avg,
        'max_dd': max_dd,
    }


def print_results(name, description, trades):
    s = calc_stats(trades)
    if not s:
        print(f"\n{'='*70}\n  {name}\n  {description}\n{'='*70}")
        print("  No trades.\n")
        return s

    longs = len([t for t in trades if t.get('dir') == 'L'])
    shorts = len([t for t in trades if t.get('dir') == 'S'])

    print(f"\n{'='*70}")
    print(f"  {name}")
    print(f"  {description}")
    print(f"{'='*70}")
    print(f"  Trades: {s['trades']}  |  Longs: {longs}  |  Shorts: {shorts}")
    print(f"  Win Rate:   {s['win_rate']:.1f}%  ({s['winners']}W / {s['losers']}L)")
    print(f"  Avg Winner: {s['avg_win']:+.1f} pts")
    print(f"  Avg Loser:  {s['avg_loss']:+.1f} pts")
    print(f"  PF:         {s['pf']:.2f}")
    print(f"  Total P&L:  {s['total_pts']:+.1f} pts  (${s['total_pts']*5:+,.0f} MNQ / ${s['total_pts']*20:+,.0f} NQ)")
    print(f"  Avg/Trade:  {s['avg_pts']:+.1f} pts")
    print(f"  Max DD:     {s['max_dd']:.1f} pts")
    return s


# ══════════════════════════════════════════════════════════════════════
#  STRATEGY A: First Candle Reversal
#  Logic: Fade the 9:30 candle. If 9:30 is bullish → short at 9:35 open.
#         If 9:30 is bearish → long at 9:35 open.
#  Stop: beyond 9:30 candle extreme. Exit at 10:00 (9:55 close).
# ══════════════════════════════════════════════════════════════════════
def strat_a_first_candle_reversal(data):
    trades = []
    for date, bars in data.items():
        wb = get_window_bars(bars)
        if len(wb) < 6:
            continue

        candle_930 = wb[0]  # 9:30 bar
        is_bullish = candle_930['close'] > candle_930['open']

        entry = wb[1]['open']  # 9:35 open
        exit_price = wb[-1]['close']  # 9:55 close

        buf = candle_930['close'] * 0.0003  # ~0.03% buffer scales with price

        if is_bullish:
            # Short: fade the bull candle
            direction = 'S'
            stop = candle_930['high'] + buf
            risk = stop - entry
            # Check stop hit on bars 9:35–9:55
            stopped = False
            for bar in wb[1:]:
                if bar['high'] >= stop:
                    stopped = True
                    break
            pnl = (entry - stop) if stopped else (entry - exit_price)
        else:
            # Long: fade the bear candle
            direction = 'L'
            stop = candle_930['low'] - buf
            risk = entry - stop
            stopped = False
            for bar in wb[1:]:
                if bar['low'] <= stop:
                    stopped = True
                    break
            pnl = (stop - entry) if stopped else (exit_price - entry)

        trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})
    return trades


# ══════════════════════════════════════════════════════════════════════
#  STRATEGY B: First Candle Breakout
#  Logic: Use 9:30 candle as micro opening range.
#         Buy stop above 9:30 high, sell stop below 9:30 low.
#         First trigger wins. Exit at 9:55 close.
#  Stop: opposite side of 9:30 candle.
# ══════════════════════════════════════════════════════════════════════
def strat_b_first_candle_breakout(data):
    trades = []
    for date, bars in data.items():
        wb = get_window_bars(bars)
        if len(wb) < 6:
            continue

        c930 = wb[0]
        high_level = c930['high']
        low_level = c930['low']
        rng = high_level - low_level
        if rng <= c930['close'] * 0.0005:  # skip tiny ranges (<0.05%)
            continue

        exit_price = wb[-1]['close']
        entered = False

        for bar in wb[1:]:
            if not entered:
                # Check long trigger
                if bar['high'] > high_level:
                    entry = high_level
                    stop = low_level
                    direction = 'L'
                    risk = entry - stop
                    entered = True
                    # Check if also stopped in same bar
                    if bar['low'] <= stop:
                        pnl = stop - entry
                        trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})
                        entered = 'done'
                        break
                # Check short trigger
                elif bar['low'] < low_level:
                    entry = low_level
                    stop = high_level
                    direction = 'S'
                    risk = stop - entry
                    entered = True
                    if bar['high'] >= stop:
                        pnl = entry - stop
                        trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})
                        entered = 'done'
                        break
            elif entered is True:
                # Check stop
                if direction == 'L' and bar['low'] <= stop:
                    pnl = stop - entry
                    trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})
                    entered = 'done'
                    break
                elif direction == 'S' and bar['high'] >= stop:
                    pnl = entry - stop
                    trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})
                    entered = 'done'
                    break

        if entered is True:
            # Exit at 9:55 close
            pnl = (exit_price - entry) if direction == 'L' else (entry - exit_price)
            trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})

    return trades


# ══════════════════════════════════════════════════════════════════════
#  STRATEGY C: 3-Bar Momentum
#  Logic: After 3 bars (9:30–9:44), if all 3 close in same direction
#         (all green or all red), enter in that direction at 9:45 open.
#         Exit at 9:55 close. Stop: low/high of the 3 bars.
# ══════════════════════════════════════════════════════════════════════
def strat_c_three_bar_momentum(data):
    trades = []
    for date, bars in data.items():
        wb = get_window_bars(bars)
        if len(wb) < 6:
            continue

        b1, b2, b3 = wb[0], wb[1], wb[2]
        all_green = all(b['close'] > b['open'] for b in [b1, b2, b3])
        all_red = all(b['close'] < b['open'] for b in [b1, b2, b3])

        if not all_green and not all_red:
            continue

        entry = wb[3]['open']  # 9:45 open
        exit_price = wb[-1]['close']  # 9:55 close

        if all_green:
            direction = 'L'
            stop = min(b['low'] for b in [b1, b2, b3])
            risk = entry - stop
            stopped = False
            for bar in wb[3:]:
                if bar['low'] <= stop:
                    stopped = True
                    break
            pnl = (stop - entry) if stopped else (exit_price - entry)
        else:
            direction = 'S'
            stop = max(b['high'] for b in [b1, b2, b3])
            risk = stop - entry
            stopped = False
            for bar in wb[3:]:
                if bar['high'] >= stop:
                    stopped = True
                    break
            pnl = (entry - stop) if stopped else (entry - exit_price)

        trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})
    return trades


# ══════════════════════════════════════════════════════════════════════
#  STRATEGY D: Mean Reversion — Fade Big First Move
#  Logic: If price moves > 50 pts from 9:30 open by 9:40 close (2 bars),
#         fade the move at 9:45 open. Exit at 9:55 close.
#         Stop: extreme of move + buffer.
# ══════════════════════════════════════════════════════════════════════
def strat_d_mean_reversion(data, threshold_pct=0.25):
    trades = []
    for date, bars in data.items():
        wb = get_window_bars(bars)
        if len(wb) < 6:
            continue

        open_price = wb[0]['open']
        # Measure move after 2 bars (9:30, 9:35 → check at 9:40 open = 9:35 close)
        close_2bars = wb[1]['close']  # 9:35 close
        move = close_2bars - open_price
        threshold = open_price * threshold_pct / 100  # percentage-based

        if abs(move) < threshold:
            continue

        entry = wb[2]['open']  # 9:40 open
        exit_price = wb[-1]['close']  # 9:55 close

        if move > threshold:
            # Big move up → fade short
            direction = 'S'
            extreme = max(wb[0]['high'], wb[1]['high'])
            stop = extreme + open_price * 0.0005
            risk = stop - entry
            stopped = False
            for bar in wb[2:]:
                if bar['high'] >= stop:
                    stopped = True
                    break
            pnl = (entry - stop) if stopped else (entry - exit_price)
        else:
            # Big move down → fade long
            direction = 'L'
            extreme = min(wb[0]['low'], wb[1]['low'])
            stop = extreme - open_price * 0.0005
            risk = entry - stop
            stopped = False
            for bar in wb[2:]:
                if bar['low'] <= stop:
                    stopped = True
                    break
            pnl = (stop - entry) if stopped else (exit_price - entry)

        trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})
    return trades


# ══════════════════════════════════════════════════════════════════════
#  STRATEGY E: First Candle Bias + Pullback Entry
#  Logic: 9:30 candle sets directional bias.
#         Wait for a pullback (bar that trades against bias).
#         Enter on the close of the pullback bar in direction of bias.
#         Exit at 9:55 close. Stop: pullback extreme.
# ══════════════════════════════════════════════════════════════════════
def strat_e_bias_pullback(data):
    trades = []
    for date, bars in data.items():
        wb = get_window_bars(bars)
        if len(wb) < 6:
            continue

        c930 = wb[0]
        bias_up = c930['close'] > c930['open']
        bias_down = c930['close'] < c930['open']

        if not bias_up and not bias_down:
            continue  # doji

        exit_price = wb[-1]['close']
        entered = False

        for i, bar in enumerate(wb[1:], 1):
            if not entered:
                if bias_up and bar['close'] < bar['open']:
                    # Pullback candle in uptrend → buy at close
                    entry = bar['close']
                    stop = bar['low'] - bar['close'] * 0.0003
                    risk = entry - stop
                    direction = 'L'
                    entered = True
                elif bias_down and bar['close'] > bar['open']:
                    # Pullback candle in downtrend → sell at close
                    entry = bar['close']
                    stop = bar['high'] + bar['close'] * 0.0003
                    risk = stop - entry
                    direction = 'S'
                    entered = True
            else:
                # Check stop
                if direction == 'L' and bar['low'] <= stop:
                    pnl = stop - entry
                    trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})
                    entered = 'done'
                    break
                elif direction == 'S' and bar['high'] >= stop:
                    pnl = entry - stop
                    trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})
                    entered = 'done'
                    break

        if entered is True:
            pnl = (exit_price - entry) if direction == 'L' else (entry - exit_price)
            trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})

    return trades


# ══════════════════════════════════════════════════════════════════════
#  STRATEGY F: 2-Bar Opening Range Breakout
#  Logic: Use first 2 bars (9:30 + 9:35) as micro range.
#         Enter on breakout after 9:40. Exit at 9:55 close.
#         Stop: opposite side of 2-bar range.
# ══════════════════════════════════════════════════════════════════════
def strat_f_two_bar_orb(data):
    trades = []
    for date, bars in data.items():
        wb = get_window_bars(bars)
        if len(wb) < 6:
            continue

        or_high = max(wb[0]['high'], wb[1]['high'])
        or_low = min(wb[0]['low'], wb[1]['low'])
        rng = or_high - or_low
        if rng <= wb[0]['close'] * 0.001:  # skip tiny ranges (<0.1%)
            continue

        exit_price = wb[-1]['close']
        entered = False

        for i, bar in enumerate(wb[2:], 2):  # start from 9:40
            if not entered:
                if bar['close'] > or_high:
                    entry = bar['close']
                    stop = or_low
                    direction = 'L'
                    risk = entry - stop
                    entered = True
                elif bar['close'] < or_low:
                    entry = bar['close']
                    stop = or_high
                    direction = 'S'
                    risk = stop - entry
                    entered = True
            else:
                if direction == 'L' and bar['low'] <= stop:
                    pnl = stop - entry
                    trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})
                    entered = 'done'
                    break
                elif direction == 'S' and bar['high'] >= stop:
                    pnl = entry - stop
                    trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})
                    entered = 'done'
                    break

        if entered is True:
            pnl = (exit_price - entry) if direction == 'L' else (entry - exit_price)
            trades.append({'date': date, 'dir': direction, 'pnl': round(pnl, 2), 'risk': risk})

    return trades


# ══════════════════════════════════════════════════════════════════════
#  RUN ALL STRATEGIES
# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Load from repo's monthly 5m files (15 years of data)
    data = load_repo_data(script_dir)
    dates = sorted(data.keys())

    # Filter to days that have a proper 9:30-9:55 window (6 bars)
    valid_days = {d: bars for d, bars in data.items() if len(get_window_bars(bars)) >= 6}
    data = OrderedDict(sorted(valid_days.items()))

    print("#" * 70)
    print("#    NQ 9:30–10:00 WINDOW-ONLY STRATEGIES")
    dates = sorted(data.keys())
    print(f"#    Data: {dates[0]} to {dates[-1]}  ({len(dates)} days)")
    print(f"#    All trades enter & exit within 9:30–10:00")
    print("#" * 70)

    results = {}

    ta = strat_a_first_candle_reversal(data)
    results['A'] = print_results(
        "STRATEGY A: First Candle Reversal",
        "Fade the 9:30 candle direction at 9:35 open. Exit 9:55 close.", ta)

    tb = strat_b_first_candle_breakout(data)
    results['B'] = print_results(
        "STRATEGY B: First Candle Breakout",
        "Break of 9:30 H/L → enter. Stop opposite side. Exit 9:55 close.", tb)

    tc = strat_c_three_bar_momentum(data)
    results['C'] = print_results(
        "STRATEGY C: 3-Bar Momentum",
        "If 9:30–9:44 all green/red → enter at 9:45 in that direction.", tc)

    td = strat_d_mean_reversion(data, threshold_pct=0.25)
    results['D'] = print_results(
        "STRATEGY D: Mean Reversion (Fade Big Move)",
        "If >0.25% move in first 10min → fade at 9:40. Exit 9:55 close.", td)

    te = strat_e_bias_pullback(data)
    results['E'] = print_results(
        "STRATEGY E: First Candle Bias + Pullback",
        "9:30 sets bias. Enter on first pullback candle close. Exit 9:55.", te)

    tf = strat_f_two_bar_orb(data)
    results['F'] = print_results(
        "STRATEGY F: 2-Bar Micro ORB",
        "9:30+9:35 form range. Enter on breakout close. Exit 9:55 close.", tf)

    # ── Comparison ─────────────────────────────────────────────────────
    print(f"\n\n{'='*70}")
    print("  COMPARISON TABLE")
    print(f"{'='*70}")
    print(f"  {'Strategy':<35} {'Trades':>6} {'Win%':>7} {'PF':>7} {'Total':>10} {'Avg/Tr':>8} {'MaxDD':>9}")
    print(f"  {'-'*82}")

    labels = {
        'A': 'A: 1st Candle Reversal',
        'B': 'B: 1st Candle Breakout',
        'C': 'C: 3-Bar Momentum',
        'D': 'D: Mean Reversion (50pt)',
        'E': 'E: Bias + Pullback',
        'F': 'F: 2-Bar Micro ORB',
    }
    for key in ['A', 'B', 'C', 'D', 'E', 'F']:
        s = results[key]
        if s:
            print(f"  {labels[key]:<35} {s['trades']:>6} {s['win_rate']:>6.1f}% {s['pf']:>7.2f} {s['total_pts']:>+10.1f} {s['avg_pts']:>+7.1f} {s['max_dd']:>9.1f}")
        else:
            print(f"  {labels[key]:<35}    N/A")

    # ── Monthly breakdown for best strategy ────────────────────────────
    best_key = max(results.keys(), key=lambda k: results[k]['total_pts'] if results[k] else float('-inf'))
    best_label = labels[best_key]

    # Rerun best to get trade list
    all_trades = {'A': ta, 'B': tb, 'C': tc, 'D': td, 'E': te, 'F': tf}
    best_trades = all_trades[best_key]

    print(f"\n\n{'='*70}")
    print(f"  YEARLY BREAKDOWN — {best_label}")
    print(f"{'='*70}")

    yearly = defaultdict(list)
    for t in best_trades:
        year = t['date'][:4]
        yearly[year].append(t['pnl'])

    print(f"\n  {'Year':<6} {'Trades':>6} {'Win%':>7} {'PF':>7} {'Total Pts':>10} {'Cumulative':>12}")
    print(f"  {'-'*54}")
    cum = 0
    for year in sorted(yearly.keys()):
        pnls = yearly[year]
        w = len([p for p in pnls if p > 0])
        l_pnls = [p for p in pnls if p < 0]
        total = sum(pnls)
        cum += total
        wr = w / len(pnls) * 100
        gp = sum(p for p in pnls if p > 0)
        gl = abs(sum(p for p in pnls if p < 0))
        pf = gp / gl if gl > 0 else float('inf')
        print(f"  {year:<6} {len(pnls):>6} {wr:>6.1f}% {pf:>7.2f} {total:>+10.1f} {cum:>+12.1f}")

    print()
