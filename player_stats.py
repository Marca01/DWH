import requests
import json
from main import upload_blob_from_memory, getDataFromUrl

seasonID = "489"

player_stats = ['goals', 'goal_assist', 'clean_sheet', 'appearances', 'mins_played', 'yellow_card', 'red_card', 'total_pass', 'touches', 'total_scoring_att', 'hit_woodwork',
                'big_chance_missed', 'total_offside', 'total_tackle', 'fouls', 'dispossessed', 'own_goals', 'total_clearance', 'clearance_off_line', 'saves',
                'penalty_save', 'total_high_claim', 'punches']
for stat in player_stats:
    url = f"https://footballapi.pulselive.com/football/stats/ranked/players/{stat}?page=0&pageSize=20&compSeasons={seasonID}&comps=1&compCodeForActivePlayer=EN_PR&altIds=true"
    rs = requests.get(
        url,
        headers = {
            "origin": "https://www.premierleague.com"
        }
    )
    data = json.loads(rs.text)
    print(data)

