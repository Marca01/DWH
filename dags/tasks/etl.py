from pyspark.sql import SparkSession, Window, Row
from pyspark.sql.functions import to_timestamp, get_json_object, col, create_map, explode, lit, when, sum, concat, asc, desc, row_number, collect_list, concat_ws, split, flatten, to_date, date_format,udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
import os
import threading
import json
# from utilss import getFirstLetterUdf, formatDateTypeUdf, formatTime
import re
import time
from datetime import datetime, date
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_GoogleCloudStorage.json'

SEASON_DATA_PATH = os.getenv('SEASON_DATA_PATH')
SNOWFLAKE_PWD = os.getenv('SNOWFLAKE_PWD')
def getFirstLetter(text):
    words = re.split(r'[ /]', text)
    first_letters = [word[0] for word in words]
    first_letters = ''.join(first_letters)
    return first_letters

def formatDateType(date_string):
    date_object = datetime.strptime(date_string, "%d %B %Y").date()
    return str(date_object)
def formatTime(sec):
    sec = round(sec)
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    formatted_time = f"{h:02d}:{m:02d}:{s:02d}"
    return formatted_time

getFirstLetterUdf = udf(getFirstLetter, StringType())
formatDateTypeUdf = udf(formatDateType, StringType())

def create_spark_session():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName('epl-pipeline') \
        .config("spark.executor.memory", "7g") \
        .config("spark.driver.memory", "7g") \
        .config("spark.executor.memoryOverhead", "1g") \
        .config("spark.executor.memory.offHeap.enabled", "true") \
        .config("spark.executor.memory.offHeap.size", "3g") \
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")\
        .getOrCreate()
    return spark

def main(spark):
    data = extract_data(spark)
    data_transformed = transform_data(data)
    load_data(data_transformed)

def extract_data(spark):
    seasons = spark.read.option('multiline', True).json(SEASON_DATA_PATH)
    data = {}

    club_info_data = []
    club_stats_data = []
    player_info_data = []
    player_stats_data = []

    for season in seasons.collect()[:1]:
        season_id = season[0]
        season_label = season[1]

        CLUB_INFO_DATA_PATH = f'gs://epl_mp2/club/{season_label}/club_info.json'
        CLUB_STATS_DATA_PATH = f'gs://epl_mp2/club/{season_label}/club_stats.json'
        PLAYER_INFO_DATA_PATH = f'gs://epl_mp2/player/{season_label}/player_info.json'
        PLAYER_STATS_DATA_PATH = f'gs://epl_mp2/player/{season_label}/player_stats.json'

        club_info = spark.read.option('multiline', True).json(CLUB_INFO_DATA_PATH)
        club_info_data.append(club_info)

        club_stats = spark.read.option('multiline', True).json(CLUB_STATS_DATA_PATH)
        club_stats_data.append({'season_id': season_id, 'club_stats': club_stats})

        player_info = spark.read.option('multiline', True).json(PLAYER_INFO_DATA_PATH)
        player_info_data.append(player_info)

        player_stats = spark.read.option('multiline', True).json(PLAYER_STATS_DATA_PATH)
        player_stats_data.append({'season_id': season_id, 'player_stats': player_stats})

    data.update({'seasons': seasons,
                 'club_info': club_info_data,
                 'club_stats': club_stats_data,
                 'player_info': player_info_data,
                 'player_stats': player_stats_data
                 })

    return data

def transform_data(data):
    # general variables
    seasons = data['seasons']
    club_info = data['club_info']
    club_stats = data['club_stats']
    player_info = data['player_info']
    player_stats = data['player_stats']

    data_transformed = {}

    fct_club_schema = StructType([
        StructField("clubid", FloatType(), True),
        StructField("stadiumid", FloatType(), True),
        StructField("seasonid", FloatType(), True),
        StructField("wins", FloatType(), True),
        StructField("losses", FloatType(), True),
        StructField("touches", FloatType(), True),
        StructField("own_goals", FloatType(), True),
        StructField("total_yel_card", FloatType(), True),
        StructField("total_red_card", FloatType(), True),
        StructField("goals", FloatType(), True),
        StructField("total_pass", FloatType(), True),
        StructField("total_scoring_att", FloatType(), True),
        StructField("total_offside", FloatType(), True),
        StructField("hit_woodwork", FloatType(), True),
        StructField("big_chance_missed", FloatType(), True),
        StructField("total_tackle", FloatType(), True),
        StructField("total_clearance", FloatType(), True),
        StructField("clearance_off_line", FloatType(), True),
        StructField("dispossessed", FloatType(), True),
        StructField("clean_sheet", FloatType(), True),
        StructField("saves", FloatType(), True),
        StructField("penalty_save", FloatType(), True),
        StructField("total_high_claim", FloatType(), True),
        StructField("punches", FloatType(), True),
    ])
    fct_player_schema = StructType([
        StructField("playerid", FloatType(), True),
        StructField("clubid", FloatType(), True),
        StructField("seasonid", FloatType(), True),
        StructField("appearances", FloatType(), True),
        StructField("big_chance_missed", FloatType(), True),
        StructField("clean_sheet", FloatType(), True),
        StructField("clearance_off_line", FloatType(), True),
        StructField("dispossessed", FloatType(), True),
        StructField("fouls", FloatType(), True),
        StructField("goal_assist", FloatType(), True),
        StructField("goals", FloatType(), True),
        StructField("hit_woodwork", FloatType(), True),
        StructField("mins_played", FloatType(), True),
        StructField("own_goals", FloatType(), True),
        StructField("penalty_save", FloatType(), True),
        StructField("punches", FloatType(), True),
        StructField("red_card", FloatType(), True),
        StructField("saves", FloatType(), True),
        StructField("total_clearance", FloatType(), True),
        StructField("total_high_claim", FloatType(), True),
        StructField("total_offside", FloatType(), True),
        StructField("total_pass", FloatType(), True),
        StructField("total_scoring_att", FloatType(), True),
        StructField("total_tackle", FloatType(), True),
        StructField("touches", FloatType(), True),
        StructField("yellow_card", FloatType(), True),
    ])
    dim_player_schema = StructType([
        StructField("playerid", FloatType(), True),
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("shirt_num", FloatType(), True),
        StructField("countryid", StringType(), True),
        StructField("positionid", StringType(), True),
    ])
    dim_country_schema = StructType([
        StructField("countryid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("demonym", StringType(), True)
    ])
    dim_position_schema = StructType([
        StructField("positionid", StringType(), True),
        StructField("name", StringType(), True)
    ])
    dim_club_schema = StructType([
        StructField("clubid", FloatType(), True),
        StructField("name", StringType(), True),
        StructField("short_name", StringType(), True),
        StructField("abbr", StringType(), True),
    ])
    dim_stadium_schema = StructType([
        StructField("stadiumid", FloatType(), True),
        StructField("name", StringType(), True),
        StructField("capacity", FloatType(), True),
        StructField("city", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", StringType(), True),
    ])

    fct_club_stats = spark.createDataFrame([], fct_club_schema)
    fct_player_stats = spark.createDataFrame([], fct_player_schema)

    dim_player_stats = spark.createDataFrame([], dim_player_schema)
    dim_country_stats = spark.createDataFrame([], dim_country_schema)
    dim_position_stats = spark.createDataFrame([], dim_position_schema)

    dim_club_stats = spark.createDataFrame([], dim_club_schema)
    dim_stadium_stats = spark.createDataFrame([], dim_stadium_schema)



    # ========================================================= SEASON =========================================================
    dim_season = seasons.select(seasons.id.alias('seasonid'), 'label')

    # ========================================================= CLUB =========================================================
    # club_info
    for info in club_info:
        dim_club = info.select(info.id.alias('clubid'), 'name', info.club.shortName.alias('short_name'),
                                    info.club.abbr.alias('abbr'))

        # stadium
        stadium_info = info.select('id', explode('grounds').alias('stadium_info'))
        dim_stadium = stadium_info.select(col('stadium_info.id').alias('stadiumid'), 'stadium_info.name',
                                          'stadium_info.capacity', 'stadium_info.city', 'stadium_info.location.latitude',
                                          'stadium_info.location.longitude')

        dim_club_stats = dim_club_stats.union(dim_club).dropDuplicates().orderBy('clubid')
        dim_stadium_stats = dim_stadium_stats.union(dim_stadium).dropDuplicates().orderBy('stadiumid')

    # dim_club_stats.show(1000, truncate=False)
    # dim_stadium_stats.show(1000, truncate=False)

    # club_stats
    for st in club_stats:
        item = dict(st)
        season = item['season_id']
        stat = item['club_stats']

        club_stats_columns = stat.select(stat.entity.alias('stats')).collect()
        club_stats_detail = stat.select(explode('stats').alias('club_stats_detail'))

        grouped_club_stats_detail = club_stats_detail.withColumn('clubid', col('club_stats_detail.owner.club.id')) \
            .withColumn('stat_name', col('club_stats_detail.name')) \
            .withColumn('stat_value', col('club_stats_detail.value')) \
            .drop('club_stats_detail')

        club_id = club_stats_detail.select(col('club_stats_detail.owner.club.id').cast('int').alias('clubid'),
                                           explode('club_stats_detail.owner.grounds').alias('stadium_info'))

        club_id = club_id.withColumn('stadiumid', lit(col('stadium_info.id'))).drop('stadium_info')

        for column in club_stats_columns:
            stat_col = column[0]
            club_stat_type = grouped_club_stats_detail.groupBy(col('clubid')).agg(sum(when(col('stat_name') == stat_col, lit(col('stat_value')))).alias(stat_col))

            club_id = club_id.withColumn('seasonid', lit(int(season))).join(club_stat_type, 'clubid')
            club_id = club_id.withColumn(stat_col, lit(club_id[stat_col])).dropDuplicates().sort('clubid')

        fct_club_stats = fct_club_stats.union(club_id).orderBy('clubid')
    # fct_club_stats.show(1000, truncate=False)

        # ========================================================= REFERENCE CODE =========================================================
        # fct_club_stat = club_stats_detail.select(col('club_stats_detail.owner.club.id').alias('clubid'), *club_stats_detail.columns)
        # # joined_club_stats = fct_club_stat.join(dim_club, 'clubid').crossJoin(dim_season)
        # joined_club_stats = fct_club_stat.join(dim_club, 'clubid').crossJoin(dim_season)
        #
        # columns_to_remove = ['club_stats_detail', 'label', 'name', 'short_name', 'abbr']
        # joined_club_stats = joined_club_stats.drop(*columns_to_remove)
        #
        # headed_columns = ['clubid', 'seasonid']
        # columns_to_select = headed_columns + [column for column in joined_club_stats.columns if col not in headed_columns]
        #
        # fct_club_stats = joined_club_stats.select(*columns_to_select)
        # ==================================================================================================================



    # ========================================================= PLAYER =========================================================
    # player_info
    for p_info in player_info:

        country_info = p_info.nationalTeam
        position_info = p_info.info

        dim_country = p_info.select(country_info.isoCode.alias('countryid'), country_info.country.alias('name'), country_info.demonym.alias('demonym')).dropDuplicates().dropna()
        dim_country_with_playerid = p_info.select(p_info.playerId.alias('playerid'), country_info.isoCode.alias('countryid'))

        dim_position = p_info.select(position_info.position.alias('positionid'), position_info.positionInfo.alias('name')).dropDuplicates().dropna()
        dim_position = dim_position.withColumn('positionid', getFirstLetterUdf(col('name')))
        dim_position_with_playerid = p_info.select(p_info.playerId.alias('playerid'), position_info.position.alias('positionid'), position_info.positionInfo.alias('name')).dropDuplicates().dropna()
        dim_position_with_playerid = dim_position_with_playerid.withColumn('positionid', getFirstLetterUdf(col('name'))).drop('name')

        player = p_info.select(
            p_info.playerId.alias('playerid'),
            p_info.name.first.alias('firstname'),
            p_info.name.last.alias('lastname'),
            # formatDateTypeUdf(p_info.birth.date.label).alias('dob'),
            p_info.birth.date.label.alias('dob'),
            p_info.info.shirtNum.alias('shirt_num')
        ).dropna(subset=['dob'])

        player = player.withColumn('dob', formatDateTypeUdf(col('dob')))
        dim_player = player.join(dim_country_with_playerid, on='playerid').join(dim_position_with_playerid, on='playerid').sort('playerid')

        dim_player_stats = dim_player_stats.union(dim_player).orderBy('playerid').dropDuplicates()
        dim_country_stats = dim_country_stats.union(dim_country).orderBy('countryid').dropDuplicates().dropna()
        dim_position_stats = dim_position_stats.union(dim_position).orderBy('positionid').dropDuplicates().dropna()

    # dim_player_stats.show(1000, truncate=False)
    # dim_country_stats.show(1000, truncate=False)
    # dim_position_stats.show(1000, truncate=False)

    # player_stats
    for ps in player_stats:
        item = dict(ps)
        season = item['season_id']
        p_stat = item['player_stats']

        p_stat = p_stat.groupBy('entity').agg(collect_list('stats').alias('stats'))

        # player_stats_columns = p_stat.select(p_stat.entity.alias('stats')).sort('stats').collect()
        player_stats_columns = fct_player_stats[fct_player_stats.columns[3:26]].columns
        player_stats_detail = p_stat.select(flatten(p_stat.stats).alias('stats'))
        player_stats_detail = player_stats_detail.select(explode('stats').alias('player_stats_detail'))

        grouped_player_stats_detail = player_stats_detail.withColumn('playerid', col('player_stats_detail.owner.playerId')) \
            .withColumn('stat_name', col('player_stats_detail.name')) \
            .withColumn('stat_value', col('player_stats_detail.value')) \
            .drop('player_stats_detail')

        try:
            player_id = player_stats_detail.select(col('player_stats_detail.owner.playerId').alias('playerid'), col('player_stats_detail.owner.currentTeam.club.id').alias('clubid'))
        except:
            player_id = player_stats_detail.select(col('player_stats_detail.owner.playerId').alias('playerid'))
            player_id = player_id.withColumn('clubid', lit(None))

        for column in player_stats_columns:
            # p_stat_col = column[0]
            p_stat_col = column

            player_stat_type = grouped_player_stats_detail.groupBy(col('playerid')).agg(sum(when(col('stat_name') == p_stat_col, lit(col('stat_value')))).alias(p_stat_col))

            player_id = player_id.withColumn('seasonid', lit(season)).join(player_stat_type, 'playerid')
            player_id = player_id.withColumn(p_stat_col, lit(player_id[p_stat_col])).dropDuplicates().sort('playerid')
            try:
                player_id.select(col(p_stat_col))
            except:
                player_id = player_id.withColumn(p_stat_col, lit(None)).dropDuplicates().sort('playerid')

        fct_player_stats = fct_player_stats.union(player_id).orderBy('playerid')
    # fct_player_stats.show(1000, truncate=False)

    data_transformed.update({
                 'dim_season': dim_season,
                 'dim_stadium': dim_stadium_stats,
                 'dim_club': dim_club_stats,
                 'dim_country': dim_country_stats,
                 'dim_position': dim_position_stats,
                 'dim_player': dim_player_stats,
                 'fct_club_stat': fct_club_stats,
                 'fct_player_stat': fct_player_stats
                 })
    return data_transformed

def load_data(data_transformed):
    conn = snowflake.connector.connect(
        user='KHale',
        password=SNOWFLAKE_PWD,
        account='nr98578.ap-southeast-1',
        database='EPL',
        schema='warehouse'
    )

    cursor = conn.cursor()

    for table, df in data_transformed.items():

        match table:
            case 'fct_player_stat':
                arr_idx = [1, 3]
                stat_idx_from = 3
            case 'fct_club_stat':
                arr_idx = [1, 2, 3]
                stat_idx_from = 3
            case _:
                arr_idx = [1]
                stat_idx_from = 1

        # all_col_idx = [str(i + 1) for i in range(len(df.columns))]
        # col_idx = [str(i + 1) for i in range(len(df.columns[:stat_idx_from]))]

        # arr_idx = [c for c in df.columns if 'id' in c]
        # arr_idx = [str(i + 1) for i in range(len(arr_idx))]
        #
        # stat_cols = [c for c in df.columns if 'id' not in c]

        # sql = f"INSERT INTO {table} VALUES ({'%s, '*(len(df.columns) - 1) + '%s'})"
        insert_to_temp_table_sql = f"INSERT INTO temp_table VALUES ({'%s, '*(len(df.columns) - 1) + '%s'})"
        drop_temp_table_sql = f"drop table temp_table"

        # find_all_duplicates_sql = f"create or replace temp table duplicate_holder as (select {', '.join(df.columns)} from {table} group by {', '.join(all_col_idx)} having count(*)>1)"
        # make_duplicates_sql = f'''
        #     merge into {table} AS T1
        #         using (select {', '.join(df.columns[:stat_idx_from])}
        #         from {table}
        #         group by {', '.join(col_idx)}
        #         having count(*)>1
        #     ) AS T2 on
        #     {', '.join([f'T1.${i}' for i in arr_idx])} = {', '.join([f'T2.${i}' for i in arr_idx])}
        #     when matched then update set {', '.join([f'T1.{df.columns[i]} = %s' for i in range(stat_idx_from, len(df.columns))])}
        # '''
        update_old_data_sql = f'''
            merge into {table} AS T1 using temp_table AS T2 on
            ({', '.join([f'T1.${i}' for i in arr_idx])}) = ({', '.join([f'T2.${i}' for i in arr_idx])})
            when matched then update set {', '.join([f'T1.{df.columns[i]} = T2.{df.columns[i]}' for i in range(stat_idx_from, len(df.columns))])}
        '''

        conn.execute_string(
            "begin transaction;"
            f"create or replace table temp_table clone {table};"
            f"truncate table temp_table;"
            "commit;"
        )
        for row in df.collect():
            data = tuple(row)
            cursor.execute(insert_to_temp_table_sql, data)

        cursor.execute(update_old_data_sql)
        cursor.execute(drop_temp_table_sql)

        # cursor.execute(find_all_duplicates_sql)
        # conn.execute_string(
        #     "begin transaction;"
        #     f"delete from {table} a using duplicate_holder b where ({', '.join([f'a.${i}' for i in arr_idx])})=({', '.join([f'b.${i}' for i in arr_idx])});"
        #     f"insert into {table} select * from duplicate_holder;"
        #     "commit;"
        #     "drop table duplicate_holder;"
        # )
        conn.commit()

    conn.close()
    print('All data is uploaded!')

if __name__ == '__main__':
    start = time.time()

    spark = create_spark_session()

    main_thread = threading.Thread(target=main, args=(spark,))
    main_thread.start()
    main_thread.join()

    # main(spark)

    end = time.time()
    sec = end - start
    exe_time = formatTime(sec)
    print(exe_time)