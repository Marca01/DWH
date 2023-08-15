import sys
import json
from datetime import datetime, date
import requests
import os

# clubs_url = f'https://footballapi.pulselive.com/football/compseasons/{seasonID}/teams'
# detail_player_stats_url = 'https://footballapi.pulselive.com/football/stats/player/65970?comps=1' #main data for player stat dim table
def getDataFromUrl(url):
    rs = requests.get(
        url,
        headers = {
            "origin": "https://www.premierleague.com"
        }
    )
    if rs:
        data = json.loads(rs.text)
        return data
#
# seasons_data = getDataFromUrl(season_url)

# Read data from STDIN
# data = sys.stdin.read()
#
# # Process the data
# processed_data = data.upper()
#
# # Write the processed data to STDOUT
# sys.stdout.write(processed_data)
def get_academic_year(date: date, start_month: int = 9) -> str:
    if date.month < start_month:
        academic_year_start = date.year - 1
    else:
        academic_year_start = date.year

    academic_year_end = str(academic_year_start + 1)[2:]
    academic_year = f"{academic_year_start}-{academic_year_end}"
    return academic_year

# seasons_data = json.load(sys.stdin)
seasons_data = json.loads(json.dumps(os.environ['content']))
seasons = [{"label": "-".join(season['label'].split('/')), "id": int(season['id'])} for season in
           seasons_data]

academic_year = get_academic_year(datetime.now())
current_season = [season for season in seasons if season['label'] == academic_year]

# UPLOAD SEASON TO CS
# def uploadSeason():
#     season_file = f"season/seasons.json"
#     total_seasons_list = json.dumps(seasons, indent=2)
#     print(total_seasons_list)
    # upload_blob_from_memory(bucket_name, total_seasons_list, season_file)
    # print(total_club_info_list)

# clubs = data
# print(data)

# {'page': 0, 'numPages': 43, 'pageSize': 30, 'numEntries': 1289}
# UPLOAD PLAYER_INFO TO CS
def getPlayerInfo():
    for season in seasons:
        # page_info_url = f"https://footballapi.pulselive.com/football/players?pageSize=30&compSeasons={season['id']}&altIds=true&type=player&id=-1&compSeasonId={season['id']}"
        # numPages = getDataFromUrl(page_info_url)['pageInfo']['numPages']
        # numPages = json.load(sys.stdin)
        numPages = int(os.environ['numPages'])
        total_player_info = []
        for page in range(0, numPages):
            player_info_url = f"https://footballapi.pulselive.com/football/players?pageSize=30&compSeasons={season['id']}&altIds=true&page={page}&type=player&id=-1&compSeasonId={season['id']}"
            player_info_data = getDataFromUrl(player_info_url)
            player_info = player_info_data['content']
            if player_info:
                total_player_info.append(player_info)

        player_info_file = f"player/{season['label']}/player_info.json"
        total_player_info = [item for sub in total_player_info for item in sub]
        total_player_info_list = json.dumps(total_player_info, indent=2)
        print(total_player_info_list)
        # upload_blob_from_memory(bucket_name, total_player_info_list, player_info_file)


# UPLOAD PLAYER_STATS TO CS
player_stats = ['goals', 'goal_assist', 'clean_sheet', 'appearances', 'mins_played', 'yellow_card', 'red_card', 'total_pass', 'touches', 'total_scoring_att', 'hit_woodwork',
                'big_chance_missed', 'total_offside', 'total_tackle', 'fouls', 'dispossessed', 'own_goals', 'total_clearance', 'clearance_off_line', 'saves',
                'penalty_save', 'total_high_claim', 'punches']
# def uploadPlayerStats():
#     for season in seasons:
#         total_player_stats = []
#         for stat in player_stats:
#             page_info_url = f"https://footballapi.pulselive.com/football/stats/ranked/players/{stat}?page=0&pageSize=20&compSeasons={season['id']}&comps=1&compCodeForActivePlayer=EN_PR&altIds=true"
#             numPages = getDataFromUrl(page_info_url)['stats']['pageInfo']['numPages']
#             for page in range(0, numPages):
#                 player_stats_url = f"https://footballapi.pulselive.com/football/stats/ranked/players/{stat}?page={page}&pageSize=20&compSeasons={season['id']}&comps=1&compCodeForActivePlayer=EN_PR&altIds=true"
#                 player_stats_data = getDataFromUrl(player_stats_url)
#                 player_stats_data = {"entity": player_stats_data['entity'], "stats": player_stats_data['stats']['content']}
#                 if player_stats_data:
#                     total_player_stats.append(player_stats_data)
#
#         player_stats_file = f"player/{season['label']}/player_stats.json"
#         total_player_stats_list = json.dumps(total_player_stats, indent=2)
#         print(total_player_stats_list)
        # upload_blob_from_memory(bucket_name, total_player_info_list, player_stats_file)


# UPLOAD CLUB_INFO TO CS
# def uploadClubInfo():
#     for season in seasons:
#         # club_info_url = f"https://footballapi.pulselive.com/football/compseasons/{season['id']}/teams"
#         club_info_url = f"https://footballapi.pulselive.com/football/teams?pageSize=100&compSeasons={season['id']}&comps=1&altIds=true&page=0"
#         # all_time_club_info_url = f"https://footballapi.pulselive.com/football/teams?pageSirze=100&comps=1&altIds=true&page=0". #Remove compSeasons
#         club_info = getDataFromUrl(club_info_url)['content']
#         if club_info:
#             club_info_file = f"club/{season['label']}/club_info.json"
#             total_club_info_list = json.dumps(club_info, indent=2)
#             print(total_club_info_list)
            # upload_blob_from_memory(bucket_name, total_club_info_list, club_info_file)
            # print(club_info)

club_stats = ['wins', 'losses', 'touches', 'own_goals', 'total_yel_card', 'total_red_card', 'goals', 'total_pass', 'total_scoring_att', 'total_offside',
              'hit_woodwork', 'big_chance_missed', 'total_tackle', 'total_clearance', 'clearance_off_line', 'dispossessed', 'clean_sheet', 'saves', 'penalty_save',
              'total_high_claim', 'punches']

# UPLOAD CLUB_STATS TO CS
# def uploadClubS/
        # upload_blob_from_memory(bucket_name, total_player_stats_list, player_stats_file)
        # print(total_player_info_list)

# player_stat = data['stats']
# print(player_stat)
# players = data['stats']['content']
# # player1 = data['stats']['content'][0]['owner']['name']
# player1 = data['stats']['content'][0]
# print(json.dumps(player1, indent=2))

# for t in teams:
#     print(f"stats for team {t[1]} with id {t[0]}")
#     stats = data["data"][str(t[0])]
#     print(stats)

if __name__ == '__main__':
    # start = time.time()

    function_name = sys.argv[1]
    match function_name:
        case 'seasons':
            print(json.dumps(seasons))
        case 'playerInfo':
            getPlayerInfo()
        # case 'playerStats':
        #     uploadPlayerStats()
        # case 'clubInfo':
        #     uploadClubInfo()
        # case 'clubStats':
        #     uploadClubStats()


    # end = time.time()
    # print('Execution Time: {}'.format(end - start))

# import sys
# import json
# import os
#
# print(os.environ['content'])
