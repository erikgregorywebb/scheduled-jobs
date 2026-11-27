import pandas as pd
import json
import time

from common import RUN_DATETIME, RUN_LABEL, get_json, save_csv

# The page https://nylottery.ny.gov/all-winning-numbers loads its data from this JSON API.
# robots.txt (checked 2026-11-27) allows all paths for all user agents.
API_URL = 'https://nylottery.ny.gov/drupal-api/api/v2/winning_numbers'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; scheduled-jobs data collector)'}

# game name -> node id used by the site (from its front-end bundle).
# Quick Draw (nid 400, a draw every 4 minutes, no prize data) and Cash4Life
# (nid 31, discontinued Feb 2026) are left out on purpose.
GAMES = {
    'powerball': '21',
    'megamillions': '16',
    'lotto': '26',
    'million4life': '374901',
    'take5': '36',
    'numbers': '41',
    'win4': '46',
    'pick10': '56',
}

# page 0 returns the 25 most recent draws per game: 8+ weeks for the twice-weekly
# games and ~12 days for the twice-daily ones, so a daily run always overlaps.
PAGES = 1

def get_rows(nid, page):
    params = {'_format': 'json', 'nid': nid, 'page': page}
    return get_json(API_URL, params=params, headers=HEADERS, timeout=30, name=f'nid {nid} page {page}').get('rows', [])

def join(values):
    return '-'.join(values) if isinstance(values, list) else values

draws = []
prizes = []
for game_key, nid in GAMES.items():
    for page in range(PAGES):
        rows = get_rows(nid, page)
        print(f'{game_key}: page {page}, {len(rows)} draws')
        for row in rows:
            draws.append({
                'game_key': game_key,
                'game': row.get('game'),
                'draw_id': row.get('id'),
                'draw_number': row.get('draw_number'),
                'draw_date': row.get('date'),
                'draw_time': row.get('draw_time'),
                'winning_numbers': join(row.get('winning_numbers')),
                'bonus_number': row.get('bonus_number'),
                'multiplier': row.get('multiplier'),
                'multiplier_label': row.get('multiplier_label'),
                'jackpot': row.get('jackpot'),
                'estimated_cash_option': row.get('estimated_cash_option'),
                'total_prizes': row.get('total_prizes'),
                'dp_winning_numbers': join(row.get('dp_winning_numbers')),
                'dp_bonus_number': row.get('dp_bonus_number'),
                'next_draw_date': row.get('next_draw_date'),
                'next_jackpot': row.get('next_jackpot'),
                'datetime': RUN_DATETIME,
            })

            # every list of prize-tier dicts is a prize group, e.g. local_winners,
            # national_winners, power_play_local_winners, dp_local_winners and
            # Mega Millions' second_prz_multiplier_winners. Powerball repeats the
            # same Power Play tiers under two keys, so identical groups are dropped.
            seen_groups = []
            for group, tiers in row.items():
                if not (isinstance(tiers, list) and tiers and isinstance(tiers[0], dict)):
                    continue
                if tiers in seen_groups:
                    continue
                seen_groups.append(tiers)
                for tier in tiers:
                    if tier.get('type') != 'winners_data':
                        continue
                    prizes.append({
                        'game_key': game_key,
                        'game': row.get('game'),
                        'draw_id': row.get('id'),
                        'draw_number': row.get('draw_number'),
                        'draw_date': row.get('date'),
                        'draw_time': row.get('draw_time'),
                        'prize_group': group,
                        'prize_level': tier.get('prize_levels'),
                        'prize_amount': tier.get('prize_amount'),
                        'prize_winners': tier.get('prize_winners'),
                        'wager_type': tier.get('wager_type'),
                        'mm_multiplier_level': tier.get('mm_multiplier_level'),
                        'mm_national_winners': tier.get('mm_national_winners'),
                        'winner_states': json.dumps(tier.get('state') or []),
                        'winner_locations': json.dumps(tier.get('location_data') or []),
                        'datetime': RUN_DATETIME,
                    })
        time.sleep(1)  # be polite between requests

df_draws = pd.DataFrame(draws)
df_prizes = pd.DataFrame(prizes)

save_csv(df_draws, 'ny-lottery/ny-lottery-draws-' + RUN_LABEL + '.csv')
save_csv(df_prizes, 'ny-lottery/ny-lottery-prizes-' + RUN_LABEL + '.csv')
