import requests
import json
import os
from dotenv import load_dotenv
import time
from datetime import datetime, date
# from utilss import getAcademicYear
import threading
from google.cloud import storage

load_dotenv()

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_GoogleCloudStorage.json'

project_id = os.getenv('PROJECT_ID')

# storage_client = storage.Client(project=project_id)
# buckets = storage_client.list_buckets()
# print("Buckets:")
# for bucket in buckets:
#     # print(bucket)
#     print(vars(bucket)) #print bucket detail
def getAcademicYear(date: date, start_month: int = 9) -> str:
    if date.month < start_month:
        academic_year_start = date.year - 1
    else:
        academic_year_start = date.year

    academic_year_end = str(academic_year_start + 1)[2:]
    academic_year = f"{academic_year_start}-{academic_year_end}"
    return academic_year

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # blob.upload_from_string(contents)
    blob.upload_from_string(contents, content_type='application/json')

    print(
        f"{destination_blob_name} uploaded to {bucket_name}."
    )

# def delete_blob(bucket_name, blob_name):
#     storage_client = storage.Client()
#
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(blob_name)
#
#     blob.delete()
#
#     print(f"Blob {blob_name} deleted.")

# upload blob file
bucket_name = os.getenv('BUCKET_NAME')
# source_file_name = 'Data_files/test_upload.png'
# # source_file_name = os.path.join('Data_files\', 'test_upload.png')
# destination_blob_name = 'document/test6.json'

# upload string
# contents = "{'name': 'KHale', 'age': 22}"

# delete file
# blob_name = 'rosy-resolver-382816'

# upload_blob(bucket_name, source_file_name, destination_blob_name)
# upload_blob_from_memory(bucket_name, contents, destination_blob_name)
# delete_blob(bucket_name, blob_name)



seasonID = "489"
season_url = 'https://footballapi.pulselive.com/football/competitions/1/compseasons?page=0&pageSize=100'
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

seasons_data = getDataFromUrl(season_url)
# print(pageInfo_data)

# teams = [(t["team"]["id"], t["team"]["name"]) for t in data["entity"]["teams"]]

seasons = [{'label': '-'.join(season['label'].split('/')), 'id': int(season['id'])} for season in
           seasons_data['content']]

academic_year = getAcademicYear(datetime.now())
current_season = [season for season in seasons if season['label'] == academic_year]

# UPLOAD SEASON TO CS
def uploadSeason():
    season_file = f"season/seasons.json"
    total_seasons_list = json.dumps(seasons, indent=2)
    upload_blob_from_memory(bucket_name, total_seasons_list, season_file)
    # print(total_club_info_list)

# clubs = data
# print(data)

# {'page': 0, 'numPages': 43, 'pageSize': 30, 'numEntries': 1289}
# UPLOAD PLAYER_INFO TO CS
def uploadPlayerInfo():
    for season in current_season:
        page_info_url = f"https://footballapi.pulselive.com/football/players?pageSize=30&compSeasons={season['id']}&altIds=true&type=player&id=-1&compSeasonId={season['id']}"
        numPages = getDataFromUrl(page_info_url)['pageInfo']['numPages']
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
        upload_blob_from_memory(bucket_name, total_player_info_list, player_info_file)


# UPLOAD PLAYER_STATS TO CS
player_stats = ['goals', 'goal_assist', 'clean_sheet', 'appearances', 'mins_played', 'yellow_card', 'red_card', 'total_pass', 'touches', 'total_scoring_att', 'hit_woodwork',
                'big_chance_missed', 'total_offside', 'total_tackle', 'fouls', 'dispossessed', 'own_goals', 'total_clearance', 'clearance_off_line', 'saves',
                'penalty_save', 'total_high_claim', 'punches']
def uploadPlayerStats():
    for season in current_season:
        total_player_stats = []
        for stat in player_stats:
            page_info_url = f"https://footballapi.pulselive.com/football/stats/ranked/players/{stat}?page=0&pageSize=20&compSeasons={season['id']}&comps=1&compCodeForActivePlayer=EN_PR&altIds=true"
            numPages = getDataFromUrl(page_info_url)['stats']['pageInfo']['numPages']
            for page in range(0, numPages):
                player_stats_url = f"https://footballapi.pulselive.com/football/stats/ranked/players/{stat}?page={page}&pageSize=20&compSeasons={season['id']}&comps=1&compCodeForActivePlayer=EN_PR&altIds=true"
                player_stats_data = getDataFromUrl(player_stats_url)
                player_stats_data = {"entity": player_stats_data['entity'], "stats": player_stats_data['stats']['content']}
                if player_stats_data:
                    total_player_stats.append(player_stats_data)

        player_stats_file = f"player/{season['label']}/player_stats.json"
        total_player_info_list = json.dumps(total_player_stats, indent=2)
        upload_blob_from_memory(bucket_name, total_player_info_list, player_stats_file)


# UPLOAD CLUB_INFO TO CS
def uploadClubInfo():
    for season in current_season:
        # club_info_url = f"https://footballapi.pulselive.com/football/compseasons/{season['id']}/teams"
        club_info_url = f"https://footballapi.pulselive.com/football/teams?pageSize=100&compSeasons={season['id']}&comps=1&altIds=true&page=0"
        # all_time_club_info_url = f"https://footballapi.pulselive.com/football/teams?pageSirze=100&comps=1&altIds=true&page=0". #Remove compSeasons
        club_info = getDataFromUrl(club_info_url)['content']
        if club_info:
            club_info_file = f"club/{season['label']}/club_info.json"
            total_club_info_list = json.dumps(club_info, indent=2)
            upload_blob_from_memory(bucket_name, total_club_info_list, club_info_file)
            # print(club_info)

club_stats = ['wins', 'losses', 'touches', 'own_goals', 'total_yel_card', 'total_red_card', 'goals', 'total_pass', 'total_scoring_att', 'total_offside',
              'hit_woodwork', 'big_chance_missed', 'total_tackle', 'total_clearance', 'clearance_off_line', 'dispossessed', 'clean_sheet', 'saves', 'penalty_save',
              'total_high_claim', 'punches']

# UPLOAD CLUB_STATS TO CS
def uploadClubStats():
    for season in current_season:
        total_club_stats = []
        for stat in club_stats:
            club_stats_url = f"https://footballapi.pulselive.com/football/stats/ranked/teams/{stat}?page=0&pageSize=20&compSeasons={season['id']}&comps=1&altIds=true"
            club_stats_data = getDataFromUrl(club_stats_url)
            club_stats_data = {"entity": club_stats_data['entity'], "stats": club_stats_data['stats']['content']}
            if club_stats_data:
                total_club_stats.append(club_stats_data)

        player_stats_file = f"club/{season['label']}/club_stats.json"
        total_player_stats_list = json.dumps(total_club_stats, indent=2)
        upload_blob_from_memory(bucket_name, total_player_stats_list, player_stats_file)
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

    season_thread = threading.Thread(target=uploadSeason)
    player_info_thread = threading.Thread(target=uploadPlayerInfo)
    player_stats_thread = threading.Thread(target=uploadPlayerStats)
    club_info_thread = threading.Thread(target=uploadClubInfo)
    club_stats_thread = threading.Thread(target=uploadClubStats)

    season_thread.start()
    club_stats_thread.start()
    club_stats_thread.start()
    player_info_thread.start()
    player_stats_thread.start()

    season_thread.join()
    club_stats_thread.join()
    club_stats_thread.join()
    player_info_thread.join()
    player_stats_thread.join()

    # end = time.time()
    # print('Execution Time: {}'.format(end - start))