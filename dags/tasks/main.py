import sys
import os
sys.path.append('/opt/airflow/')

import requests
import json
from dotenv import load_dotenv
import time
from datetime import datetime, date

from dags.tasks.utilss import get_academic_year, get_bs_content, format_time

# from utilss import get_academic_year, get_bs_content, format_time
import threading
import pathlib
import boto3
from functools import lru_cache
from google.cloud import storage

load_dotenv()

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/khale/Documents/Giaotrinh/DWH/MP2/dags/ServiceKey_GoogleCloudStorage.json"
os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "/opt/airflow/dags/ServiceKey_GoogleCloudStorage.json"
project_id = os.getenv("PROJECT_ID")

# def upload_to_gcs(data, destination_object_name):
#     s3 = boto3.resource("s3")
#     bucket_name = os.getenv("BUCKET_NAME")
#     s3.Bucket(bucket_name).put_object(Key=destination_object_name, Body=data)
#     print(f"{destination_object_name} uploaded to {bucket_name}.")


seasonID = "489"
season_url = "https://footballapi.pulselive.com/football/competitions/1/compseasons?page=0&pageSize=100"


def upload_to_gcs(contents, destination_blob_name):
    storage_client = storage.Client()
    bucket_name = os.getenv("BUCKET_NAME")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(contents, content_type="application/json")
    print(f"{destination_blob_name} uploaded to {bucket_name}.")


# clubs_url = f'https://footballapi.pulselive.com/football/compseasons/{seasonID}/teams'
def get_data_from_url(url):
    rs = requests.get(url, headers={"origin": "https://www.premierleague.com"})
    if rs:
        data = json.loads(rs.text)
        return data


seasons_data = get_data_from_url(season_url)

# seasons = [{'label': '-'.join(season['label'].split('/')), 'id': int(season['id'])} for season in
#            seasons_data['content']]
seasons = [
    {"label": "-".join(season["label"].split("/")), "id": int(season["id"])}
    for season in seasons_data["content"]
]
# seasons = seasons[0]

# academic_year = get_academic_year(datetime.now())
# current_season = [season for season in seasons if season['label'] == academic_year]
current_season = [seasons[0]]


# UPLOAD SEASON TO S3
def upload_season():
    season_file = f"season/seasons.json"
    total_seasons_list = json.dumps(seasons, indent=2)
    upload_to_gcs(total_seasons_list, season_file)


# UPLOAD PLAYER_INFO TO S3
def upload_player_info():
    for season in current_season:
        page_info_url = f"https://footballapi.pulselive.com/football/players?pageSize=30&compSeasons={season['id']}&altIds=true&type=player&id=-1&compSeasonId={season['id']}"
        num_pages = get_data_from_url(page_info_url)["pageInfo"]["numPages"]
        total_player_info = []
        for page in range(0, num_pages):
            player_info_url = f"https://footballapi.pulselive.com/football/players?pageSize=30&compSeasons={season['id']}&altIds=true&page={page}&type=player&id=-1&compSeasonId={season['id']}"
            player_info_data = get_data_from_url(player_info_url)
            player_info = player_info_data["content"]
            if player_info:
                total_player_info.append(player_info)

        player_info_file = f"player/{season['label']}/player_info.json"
        total_player_info = [item for sub in total_player_info for item in sub]
        total_player_info_list = json.dumps(total_player_info, indent=2)
        upload_to_gcs(total_player_info_list, player_info_file)


# UPLOAD PLAYER_STATS TO S3
@lru_cache
def list_all_player_stats():
    url = "https://www.premierleague.com/stats/top/players/goals"
    soup = get_bs_content(url)
    stats_code_list_top = soup.find_all("a", class_="topStatsLink")
    stats_code_list_more = soup.find_all("a", class_="more-stats__link")
    stats_code_list = stats_code_list_top + stats_code_list_more
    stats_code_list = list(
        set(
            [
                stat_code.get("href").split("/")[-1].replace("?po=GOALKEEPER", "")
                for stat_code in stats_code_list
            ]
        )
    )
    stats_code_list.sort()
    return stats_code_list


def upload_player_stats():
    for season in current_season:
        total_player_stats = []
        player_stats = list_all_player_stats()
        for stat in player_stats:
            page_info_url = f"https://footballapi.pulselive.com/football/stats/ranked/players/{stat}?page=0&pageSize=20&compSeasons={season['id']}&comps=1&compCodeForActivePlayer=EN_PR&altIds=true"
            num_pages = get_data_from_url(page_info_url)["stats"]["pageInfo"][
                "numPages"
            ]
            for page in range(0, num_pages):
                player_stats_url = f"https://footballapi.pulselive.com/football/stats/ranked/players/{stat}?page={page}&pageSize=20&compSeasons={season['id']}&comps=1&compCodeForActivePlayer=EN_PR&altIds=true"
                player_stats_data = get_data_from_url(player_stats_url)
                player_stats_data = {
                    "entity": player_stats_data["entity"],
                    "stats": player_stats_data["stats"]["content"],
                }
                if player_stats_data:
                    total_player_stats.append(player_stats_data)

        player_stats_file = f"player/{season['label']}/player_stats.json"
        total_player_stats_list = json.dumps(total_player_stats, indent=2)
        upload_to_gcs(total_player_stats_list, player_stats_file)


# UPLOAD CLUB_INFO TO S3
def upload_club_info():
    for season in current_season:
        club_info_url = f"https://footballapi.pulselive.com/football/teams?pageSize=100&compSeasons={season['id']}&comps=1&altIds=true&page=0"
        club_info = get_data_from_url(club_info_url)["content"]
        if club_info:
            club_info_file = f"club/{season['label']}/club_info.json"
            total_club_info_list = json.dumps(club_info, indent=2)
            upload_to_gcs(total_club_info_list, club_info_file)


# UPLOAD CLUB_STATS TO S3
@lru_cache
def list_all_club_stats():
    url = "https://www.premierleague.com/stats/top/clubs/wins"
    soup = get_bs_content(url)
    stats_code_list_top = soup.find_all("a", class_="topStatsLink")
    stats_code_list_more = soup.find_all("a", class_="more-stats__link")
    stats_code_list = stats_code_list_top + stats_code_list_more
    stats_code_list = list(
        set([stat_code.get("href").split("/")[-1] for stat_code in stats_code_list])
    )
    return stats_code_list


def upload_club_stats():
    for season in current_season:
        total_club_stats = []
        club_stats = list_all_club_stats()
        for stat in club_stats:
            club_stats_url = f"https://footballapi.pulselive.com/football/stats/ranked/teams/{stat}?page=0&pageSize=20&compSeasons={season['id']}&comps=1&altIds=true"
            club_stats_data = get_data_from_url(club_stats_url)
            club_stats_data = {
                "entity": club_stats_data["entity"],
                "stats": club_stats_data["stats"]["content"],
            }
            if club_stats_data:
                total_club_stats.append(club_stats_data)

        player_stats_file = f"club/{season['label']}/club_stats.json"
        total_player_stats_list = json.dumps(total_club_stats, indent=2)
        upload_to_gcs(total_player_stats_list, player_stats_file)


# UPLOAD MANAGER_INFO TO S3
def upload_manager_info():
    for season in current_season:
        manager_info_url = f"https://footballapi.pulselive.com/football/teamofficials?pageSize=100&compSeasons={season['id']}&compCodeForActivePlayer=EN_PR&comps=1&altIds=true&type=manager&page=0"
        manager_info = get_data_from_url(manager_info_url)["content"]
        if manager_info:
            manager_info_file = f"manager/{season['label']}/manager_info.json"
            total_manager_info_list = json.dumps(manager_info, indent=2)
            upload_to_gcs(total_manager_info_list, manager_info_file)


# UPLOAD AWARDS TO S3


def upload_award():
    for season in current_season:
        award_url = f"https://footballapi.pulselive.com/football/compseasons/{season['id']}/awards?altIds=true"
        awards = get_data_from_url(award_url)

        month_award = awards["monthAwards"]
        season_award = awards["seasonAwards"]
        month_award_data = json.dumps(month_award)
        season_award_data = json.dumps(season_award)

        month_award_file = f"award/{season['label']}/month/award.json"
        season_award_file = f"award/{season['label']}/season/award.json"

        upload_to_gcs(month_award_data, month_award_file)
        upload_to_gcs(season_award_data, season_award_file)

        # return month_award, season_award, month_award_data, season_award_data


def upload_award_contents():
    for season in current_season:
        award_type = "PROMO"

        award_url = f"https://footballapi.pulselive.com/football/compseasons/{season['id']}/awards?altIds=true"
        awards = get_data_from_url(award_url)

        month_award = awards["monthAwards"]
        season_award = awards["seasonAwards"]

        # month_award, season_award, *rest = getAllAwards()
        month_award_references_id = [
            relatedCt["reference"]
            for month in month_award.keys()
            for detail in month_award[month]
            if "relatedContent" in detail
            for relatedCt in detail["relatedContent"]
            if relatedCt["type"] == award_type
        ]

        season_award_references_id = [
            relatedCt["reference"]
            for detail in season_award
            if "relatedContent" in detail
            for relatedCt in detail["relatedContent"]
        ]

        total_month_award_contents = []
        total_season_award_contents = []

        # award_content_ids = month_award_references_id + season_award_references_id

        for month_award_id in month_award_references_id:
            award_content_url = f"https://footballapi.pulselive.com/content/PremierLeague/promo/EN/{month_award_id}?page=0&pageSize=1"
            month_award_content = get_data_from_url(award_content_url)
            if month_award_content:
                total_month_award_contents.append(month_award_content)

            month_award_content_file = (
                f"award/{season['label']}/month/award_content.json"
            )
            total_month_award_contents_list = json.dumps(
                total_month_award_contents, indent=2
            )
            upload_to_gcs(total_month_award_contents_list, month_award_content_file)

        for season_award_id in season_award_references_id:
            award_content_url = f"https://footballapi.pulselive.com/content/PremierLeague/promo/EN/{season_award_id}?page=0&pageSize=1"
            season_award_content = get_data_from_url(award_content_url)
            if season_award_content:
                total_season_award_contents.append(season_award_content)

            season_award_content_file = (
                f"award/{season['label']}/season/award_content.json"
            )
            total_season_award_contents_list = json.dumps(
                total_season_award_contents, indent=2
            )
            upload_to_gcs(total_season_award_contents_list, season_award_content_file)


# UPLOAD MATCH INFO TO S3
def upload_match_info():
    for season in current_season:
        total_match_info = []
        page_info_url = f"https://footballapi.pulselive.com/football/fixtures?comps=1&compSeasons={season['id']}&page=0&pageSize=20&sort=desc&statuses=C&altIds=true"
        num_pages = get_data_from_url(page_info_url)["pageInfo"]["numPages"]
        for page in range(0, num_pages):
            match_info_url = f"https://footballapi.pulselive.com/football/fixtures?comps=1&compSeasons={season['id']}&page={page}&pageSize=20&sort=desc&statuses=C&altIds=true"
            match_info_data = get_data_from_url(match_info_url)["content"]

            if match_info_data:
                total_match_info.append(match_info_data)

        total_match_info = [item for sub in total_match_info for item in sub]
        total_match_info_list = json.dumps(total_match_info, indent=2)
        match_info_file = f"match/{season['label']}/match_info.json"

        upload_to_gcs(total_match_info_list, match_info_file)

        # return total_match_info, total_match_info_list


# UPLOAD MATCH STATS TO S3
def upload_match_stats():
    # match_info, *rest = getAllMatchInfos()
    for season in current_season:
        total_match_info = []
        total_match_stats = []
        page_info_url = f"https://footballapi.pulselive.com/football/fixtures?comps=1&compSeasons={season['id']}&page=0&pageSize=20&sort=desc&statuses=C&altIds=true"
        num_pages = get_data_from_url(page_info_url)["pageInfo"]["numPages"]
        for page in range(0, num_pages):
            match_info_url = f"https://footballapi.pulselive.com/football/fixtures?comps=1&compSeasons={season['id']}&page={page}&pageSize=20&sort=desc&statuses=C&altIds=true"
            match_info_data = get_data_from_url(match_info_url)["content"]

            if match_info_data:
                total_match_info.append(match_info_data)

        total_match_info = [item for sub in total_match_info for item in sub]

        match_ids = [info["id"] for info in total_match_info]
        if len(match_ids) > 0:
            for match_id in match_ids:
                match_stats_url = (
                    f"https://footballapi.pulselive.com/football/stats/match/{match_id}"
                )
                match_stats_data = get_data_from_url(match_stats_url)
                match_stats_data = {
                    "entity": match_stats_data["entity"],
                    "stats": match_stats_data["data"],
                }

                if match_stats_data:
                    total_match_stats.append(match_stats_data)

        total_match_stats_data_list = json.dumps(total_match_stats)
        match_stats_file = f"match/{season['label']}/match_stats.json"

        upload_to_gcs(total_match_stats_data_list, match_stats_file)


if __name__ == "__main__":
    start = time.time()

    season_thread = threading.Thread(target=upload_season)
    player_info_thread = threading.Thread(target=upload_player_info)
    player_stats_thread = threading.Thread(target=upload_player_stats)
    club_info_thread = threading.Thread(target=upload_club_info)
    club_stats_thread = threading.Thread(target=upload_club_stats)
    manager_info_thread = threading.Thread(target=upload_manager_info)
    award_thread = threading.Thread(target=upload_award)
    award_content_thread = threading.Thread(target=upload_award_contents)
    match_info_thread = threading.Thread(target=upload_match_info)
    match_stats_thread = threading.Thread(target=upload_match_stats)

    season_thread.start()
    player_info_thread.start()
    player_stats_thread.start()
    club_info_thread.start()
    club_stats_thread.start()
    manager_info_thread.start()
    award_thread.start()
    award_content_thread.start()
    match_info_thread.start()
    match_stats_thread.start()

    season_thread.join()
    player_info_thread.join()
    player_stats_thread.join()
    club_info_thread.join()
    club_stats_thread.join()
    manager_info_thread.join()
    award_thread.join()
    award_content_thread.join()
    match_info_thread.join()
    match_stats_thread.join()

    end = time.time()
    sec = end - start
    exe_time = format_time(sec)
    print(exe_time)
