import requests
import json

seasonID = "489"
club_stats = ['wins', 'losses', 'touches', 'own_goals', 'total_yel_card', 'total_red_card', 'goals', 'total_pass', 'total_scoring_att', 'total_offside',
              'hit_woodwork', 'big_chance_missed', 'total_tackle', 'total_clearance', 'clearance_off_line', 'dispossessed', 'clean_sheet', 'saves', 'penalty_save',
              'total_high_claim', 'punches']
for stat in club_stats:
    url = f'https://footballapi.pulselive.com/football/stats/ranked/teams/{stat}?page=0&pageSize=20&compSeasons={seasonID}&comps=1&altIds=true'
    rs = requests.get(
        url,
        headers={
            "origin": "https://www.premierleague.com"
        }
    )
    data = json.loads(rs.text)
    print(data)