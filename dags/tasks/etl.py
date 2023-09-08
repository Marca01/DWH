# import findspark

import sys
import os
sys.path.append('/opt/airflow/dags/tasks')

import pyspark.sql.utils
from bs4 import BeautifulSoup
import requests
import json
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, Row
from pyspark.sql.functions import to_timestamp, get_json_object, col, create_map, explode, posexplode, lit, when, \
    sum as spark_sum, \
    concat, asc, \
    desc, row_number, collect_list, concat_ws, split, flatten, to_date, date_format, udf, to_json, split, count, \
    coalesce, upper, first, max, min, avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DataType, IntegerType, DecimalType, \
    DateType
import threading
import json
from utilss import get_first_letter_udf, format_date_type, format_date_type_udf, format_time, get_bs_content, \
    convert_bst_to_local_date_udf, convert_bst_to_local_time_udf, format_match_length_min_udf, has_column
import re
import time
from datetime import datetime, date
from dotenv import load_dotenv
import snowflake.connector
import great_expectations as gx
from great_expectations.dataset import SparkDFDataset
from gx import check_mandatory_columns, check_not_null_columns, check_atleast_columns, check_table_columns_to_match_set, \
    check_table_columns_type
import re
from bs4 import BeautifulSoup
import requests
import json
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from datetime import datetime, date
import pytz

# findspark.init()

load_dotenv()

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "/opt/airflow/dags/ServiceKey_GoogleCloudStorage.json"

project_id = os.getenv("PROJECT_ID")

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--py-files "/home/khale/Documents/Giaotrinh/DWH/MP2/dags/tasks/utilss.py" pyspark-shell'
SEASON_DATA_PATH = os.getenv('SEASON_DATA_PATH')

# AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
# AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
SNOWFLAKE_PWD = os.getenv('SNOWFLAKE_PWD')


def create_spark_session():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('epl-pipeline') \
        .config("spark.executor.memory", "7g") \
        .config("spark.driver.memory", "7g") \
        .config("spark.executor.memoryOverhead", "1g") \
        .config("spark.executor.memory.offHeap.enabled", "true") \
        .config("spark.executor.memory.offHeap.size", "3g") \
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    return spark

    # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    # .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-bundle:1.11.1000") \
    # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    # .config("spark.hadoop.fs.s3a.access.key", "AKIA4I7OS2C7TZZRFZQI") \
    # .config("spark.hadoop.fs.s3a.secret.Key", "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") \
    #     .config("spark.jars.packages",
    #             "net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3") \
    # .config("spark.hadoop.fs.s3a.bucket.epl.accesspoint.arn", "arn:aws:s3:ap-southeast-1:843924558015:accesspoint/epl") \
    # .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \


def main(spark):
    data = extract_data(spark)
    test_raw_data(data)
    transformed_data = transform_data(data)
    test_transformed_data(transformed_data)
    load_data(transformed_data)


def extract_data(spark):
    seasons = spark.read.option('multiline', True).json(SEASON_DATA_PATH)

    data = {}

    club_info_data = []
    club_stats_data = []
    player_info_data = []
    player_stats_data = []
    manager_info_data = []
    month_award_data = []
    month_award_content_data = []
    season_award_data = []
    season_award_content_data = []
    match_info_data = []
    match_stats_data = []

    # for season in seasons.collect()[:3]:
    for season in seasons.collect()[:1]:
    # for season in seasons.collect()[1:2]:
    # for season in seasons.collect():
        season_id = season[0]
        season_label = season[1]

        # CLUB_INFO_DATA_PATH = f's3a://epl-it/club/{season_label}/club_info.json'

        CLUB_INFO_DATA_PATH = f'gs://epl-it/club/{season_label}/club_info.json'
        CLUB_STATS_DATA_PATH = f'gs://epl-it/club/{season_label}/club_stats.json'
        PLAYER_INFO_DATA_PATH = f'gs://epl-it/player/{season_label}/player_info.json'
        PLAYER_STATS_DATA_PATH = f'gs://epl-it/player/{season_label}/player_stats.json'
        MANAGER_INFO_DATA_PATH = f'gs://epl-it/manager/{season_label}/manager_info.json'
        MONTH_AWARD_DATA_PATH = f'gs://epl-it/award/{season_label}/month/award.json'
        SEASON_AWARD_DATA_PATH = f'gs://epl-it/award/{season_label}/season/award.json'
        MATCH_INFO_DATA_PATH = f'gs://epl-it/match/{season_label}/match_info.json'
        MATCH_STATS_DATA_PATH = f'gs://epl-it/match/{season_label}/match_stats.json'

        club_info = spark.read.option('multiline', True).json(CLUB_INFO_DATA_PATH)
        club_info_data.append(club_info)

        club_stats = spark.read.option('multiline', True).json(CLUB_STATS_DATA_PATH)
        club_stats_data.append({'season_id': season_id, 'club_stats': club_stats})

        player_info = spark.read.option('multiline', True).json(PLAYER_INFO_DATA_PATH)
        player_info_data.append(player_info)

        player_stats = spark.read.option('multiline', True).json(PLAYER_STATS_DATA_PATH)
        player_stats_data.append({'season_id': season_id, 'player_stats': player_stats})

        manager_info = spark.read.option('multiline', True).json(MANAGER_INFO_DATA_PATH)
        manager_info_data.append(manager_info)

        month_award = spark.read.option('multiline', True).json(MONTH_AWARD_DATA_PATH)
        month_award_data.append({'season_id': season_id, 'month_award': month_award})

        season_award = spark.read.option('multiline', True).json(SEASON_AWARD_DATA_PATH)
        season_award_data.append({'season_id': season_id, 'season_award': season_award})

        # try:
        #     MONTH_AWARD_CONTENT_DATA_PATH = f'gs://epl-it/award/{season_label}/month/award_content.json'
        #
        #     month_award_content = spark.read.option('multiline', True).json(MONTH_AWARD_CONTENT_DATA_PATH)
        #     month_award_content_data.append(month_award_content)
        # except pyspark.sql.utils.AnalysisException:
        #     pass
        #
        # try:
        #     SEASON_AWARD_CONTENT_DATA_PATH = f'gs://epl-it/award/{season_label}/season/award_content.json'
        #
        #     season_award_content = spark.read.option('multiline', True).json(SEASON_AWARD_CONTENT_DATA_PATH)
        #     season_award_content_data.append(season_award_content)
        # except pyspark.sql.utils.AnalysisException:
        #     pass

        match_info = spark.read.option('multiline', True).json(MATCH_INFO_DATA_PATH)
        match_info_data.append(match_info)

        match_stats = spark.read.option('multiline', True).json(MATCH_STATS_DATA_PATH)
        match_stats_data.append({'season_id': season_id, 'match_stats': match_stats})

    data.update({'seasons': seasons,
                 'club_info': club_info_data,
                 'club_stats': club_stats_data,
                 'player_info': player_info_data,
                 'player_stats': player_stats_data,
                 'manager_info': manager_info_data,
                 'month_award': month_award_data,
                 # 'month_award_content': month_award_content_data,
                 'season_award': season_award_data,
                 # 'season_award_content': season_award_content_data,
                 'match_info': match_info_data,
                 'match_stats': match_stats_data
                 })
    return data


def test_raw_data(data):
    print('=========================================== START TESTING RAW DATA ===========================================')
    failed_dfs = []

    seasons_df = data['seasons']
    club_info_df = data['club_info']
    club_stats_df = data['club_stats']
    player_info_df = data['player_info']
    player_stats_df = data['player_stats']
    manager_info_df = data['manager_info']
    month_award_df = data['month_award']
    season_award_df = data['season_award']
    match_info_df = data['match_info']

    seasons_test_df = SparkDFDataset(seasons_df)
    # seasons_test_df.spark_df.show(1)

    MANDATORY_SEASONS_COLUMNS = ['id', 'label']
    check_mandatory_columns('season', seasons_test_df, MANDATORY_SEASONS_COLUMNS)

    for club_info in club_info_df:
        MANDATORY_CLUB_INFO_COLUMNS = ['id', 'name', 'club', 'grounds'
                                       # 'club.name', 'club.shortName', 'club.abbr',
                                       # '$.grounds[*].id', '$.grounds[*].name', '$.grounds[*].capacity',
                                       # '$.grounds[*].city', '$.grounds[*].location.latitude',
                                       # '$.grounds[*].location.longitude'
                                       ]
        NULLABLE_CLUB_INFO_COLUMNS = ['grounds']
        club_info_test_df = SparkDFDataset(club_info)
        club_info_check_mandatory_columns_result = check_mandatory_columns('club_info', club_info_test_df, MANDATORY_CLUB_INFO_COLUMNS)
        club_info_check_not_null_columns_result = check_not_null_columns('club_info', club_info_test_df, NULLABLE_CLUB_INFO_COLUMNS)

        if not club_info_check_mandatory_columns_result:
            failed_dfs.append('Check mandatory columns result: club_info')

        if not club_info_check_not_null_columns_result:
            failed_dfs.append('Check not null columns result: club_info')

    for club_stats in club_stats_df:
        item = dict(club_stats)
        # season = item['season_id']
        stats = item['club_stats']

        MANDATORY_CLUB_STATS_COLUMNS = ['entity', 'stats']
        NULLABLE_CLUB_STATS_COLUMNS = ['stats']
        club_stats_test_df = SparkDFDataset(stats)
        club_stats_check_mandatory_columns_result = check_mandatory_columns('club_stats', club_stats_test_df, MANDATORY_CLUB_STATS_COLUMNS)
        club_stats_check_not_null_columns_result = check_not_null_columns('club_stats', club_stats_test_df, NULLABLE_CLUB_STATS_COLUMNS)

        if not club_stats_check_mandatory_columns_result:
            failed_dfs.append('Check mandatory columns result: club_stats')

        if not club_stats_check_not_null_columns_result:
            failed_dfs.append('Check not null columns result: club_stats')

    for player_info in player_info_df:
        MANDATORY_PLAYER_INFO_COLUMNS = ['playerId', 'info', 'nationalTeam', 'name', 'birth']

        player_info_test_df = SparkDFDataset(player_info)
        player_info_check_mandatory_columns_result = check_mandatory_columns('player_info', player_info_test_df, MANDATORY_PLAYER_INFO_COLUMNS)

        if not player_info_check_mandatory_columns_result:
            failed_dfs.append('Check mandatory columns result: player_info')

    for player_stats in player_stats_df:
        item = dict(player_stats)
        stats = item['player_stats']

        MANDATORY_PLAYER_STATS_COLUMNS = ['entity', 'stats']
        NULLABLE_PLAYER_STATS_COLUMNS = ['stats']

        player_stats_test_df = SparkDFDataset(stats)
        player_stats_check_mandatory_columns_result = check_mandatory_columns('player_stats', player_stats_test_df, MANDATORY_PLAYER_STATS_COLUMNS)

        if not player_stats_check_mandatory_columns_result:
            failed_dfs.append('Check mandatory columns result: player_stats')

    for manager_info in manager_info_df:
        MANDATORY_MANAGER_INFO_COLUMNS = ['officialId', 'birth', 'currentTeam']

        manager_info_test_df = SparkDFDataset(manager_info)
        manager_info_check_mandatory_columns_result = check_mandatory_columns('manager_info', manager_info_test_df, MANDATORY_MANAGER_INFO_COLUMNS)

        if not manager_info_check_mandatory_columns_result:
            failed_dfs.append('Check mandatory columns result: manager_info')

    for month_award in month_award_df:
        item = dict(month_award)
        awards = item['month_award']

        NULLABLE_MONTH_AWARD_COLUMNS = awards.columns

        month_award_test_df = SparkDFDataset(awards)
        month_award_check_not_null_columns_result = check_not_null_columns('month_award', month_award_test_df, NULLABLE_MONTH_AWARD_COLUMNS)

        if not month_award_check_not_null_columns_result:
            failed_dfs.append('Check not null columns result: month_award')

    for season_award in season_award_df:
        item = dict(season_award)
        awards = item['season_award']

        MANDATORY_SEASON_AWARD_COLUMNS = ['awardTypeId', 'award', 'player', 'apiTeam', 'official']

        season_award_test_df = SparkDFDataset(awards)
        season_award_check_mandatory_columns_result = check_mandatory_columns('season_award', season_award_test_df, MANDATORY_SEASON_AWARD_COLUMNS)

        if not season_award_check_mandatory_columns_result:
            failed_dfs.append('Check mandatory columns result: season_award')

    for match_info in match_info_df:
        # MANDATORY_MATCH_INFO_COLUMNS = ['id', 'teams', 'kickoff', 'attendance', 'clock', 'ground']
        MANDATORY_MATCH_INFO_COLUMNS = ['id', 'teams', 'kickoff', 'attendance', 'clock', 'ground']

        match_info_test_df = SparkDFDataset(match_info)
        match_info_check_mandatory_columns_result = check_mandatory_columns('match_info', match_info_test_df, MANDATORY_MATCH_INFO_COLUMNS)

        if not match_info_check_mandatory_columns_result:
            failed_dfs.append('Check mandatory columns result: match_info')

    # if failed_dfs:
    #     raise Exception(f'The following dataframes failed the test: {failed_dfs}')
    print('=========================================== END TESTING RAW DATA ===========================================')


def transform_data(data):
    # general variables
    seasons = data['seasons']
    club_info = data['club_info']
    club_stats = data['club_stats']
    player_info = data['player_info']
    player_stats = data['player_stats']
    manager_info = data['manager_info']
    month_award = data['month_award']
    # month_award_content = data['month_award_content']
    season_award = data['season_award']
    # season_award_content = data['season_award_content']
    match_info = data['match_info']
    match_stats = data['match_stats']

    # Define Snowflake options
    sf_options = {
        "sfURL": "el88601.ap-southeast-1.snowflakecomputing.com",
        "sfUser": "KHale",
        "sfPassword": SNOWFLAKE_PWD,
        "sfDatabase": "EPL",
        "sfSchema": "warehouse"
        # "sfWarehouse": "your_snowflake_warehouse"
    }
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    # Define the table name and column prefix
    # table_name = "fct_player_stat"
    # column_prefix = "your_column_prefix"

    table_name_list = ['dim_season', 'dim_country', 'dim_position', 'dim_player', 'dim_stadium', 'dim_club',
                       'dim_manager', 'dim_award', 'dim_match', 'fct_object_award', 'fct_player_stat', 'fct_club_stat']
    table_schemas = {}

    for table in table_name_list:
        # Retrieve the schema from Snowflake
        snowflake_df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sf_options) \
            .option("query", f"SELECT * FROM {table} WHERE 1=0") \
            .load()

        snowflake_schema = snowflake_df.schema
        table_schemas.update({f'{table}_schema': snowflake_schema})

    # renamed_fields = [f.name.replace(column_prefix, "") for f in snowflake_schema.fields]
    # renamed_schema = StructType([StructField(name, f.dataType, f.nullable, f.metadata) for name, f in
    #                              zip(renamed_fields, snowflake_schema.fields)])
    # ===========================================================================================

    data_transformed = {}

    fct_club_stats = spark.createDataFrame([], table_schemas['fct_club_stat_schema'])
    fct_player_stats = spark.createDataFrame([], table_schemas['fct_player_stat_schema'])
    fct_object_award_stats = spark.createDataFrame([], table_schemas['fct_object_award_schema'])
    # fct_match_stats = spark.createDataFrame([], table_schemas['fct_match_stat_schema'])

    dim_season_stats = spark.createDataFrame([], table_schemas['dim_season_schema'])

    dim_player_stats = spark.createDataFrame([], table_schemas['dim_player_schema'])
    dim_country_stats = spark.createDataFrame([], table_schemas['dim_country_schema'])
    dim_position_stats = spark.createDataFrame([], table_schemas['dim_position_schema'])

    dim_club_stats = spark.createDataFrame([], table_schemas['dim_club_schema'])
    dim_stadium_stats = spark.createDataFrame([], table_schemas['dim_stadium_schema'])

    dim_manager_stats = spark.createDataFrame([], table_schemas['dim_manager_schema'])

    dim_award_stats = spark.createDataFrame([], table_schemas['dim_award_schema'])
    # dim_award_content_stats = spark.createDataFrame([], table_schemas['dim_award_content_schema'])

    dim_match_stats = spark.createDataFrame([], table_schemas['dim_match_schema'])

    dim_season_columns = dim_season_stats.columns
    dim_stadium_columns = dim_stadium_stats.columns
    dim_club_columns = dim_club_stats.columns
    dim_country_columns = dim_country_stats.columns
    dim_position_columns = dim_position_stats.columns
    dim_player_columns = dim_player_stats.columns
    dim_manager_columns = dim_manager_stats.columns
    dim_award_columns = dim_award_stats.columns
    dim_match_columns = dim_match_stats.columns
    fct_club_stat_columns = fct_club_stats.columns
    fct_player_stat_columns = fct_player_stats.columns
    fct_object_award_columns = fct_object_award_stats.columns

    dim_season_column_type = table_schemas['dim_season_schema']
    dim_stadium_column_type = table_schemas['dim_stadium_schema']
    dim_club_column_type = table_schemas['dim_club_schema']
    dim_country_column_type = table_schemas['dim_country_schema']
    dim_position_column_type = table_schemas['dim_position_schema']
    dim_player_column_type = table_schemas['dim_player_schema']
    dim_manager_column_type = table_schemas['dim_manager_schema']
    dim_award_column_type = table_schemas['dim_award_schema']
    dim_match_column_type = table_schemas['dim_match_schema']
    fct_club_stat_column_type = table_schemas['fct_club_stat_schema']
    fct_player_stat_column_type = table_schemas['fct_player_stat_schema']
    fct_object_award_column_type = table_schemas['fct_object_award_schema']

    # ========================================================= SEASON =========================================================
    dim_season = seasons.select(seasons.id.alias('season_id'), 'label')
    dim_season_stats = dim_season_stats.union(dim_season)

    # ========================================================= MANAGER =========================================================
    # manager info
    manager_schema = StructType([
        StructField("manager_id", FloatType(), True),
        StructField("club_id", FloatType(), True),
    ])
    manager_club = spark.createDataFrame([], manager_schema)

    for mi in manager_info:
        # general vars
        country_info = mi.birth.country
        manager_id = mi.officialId.alias('manager_id')
        country_id = country_info.isoCode.alias('country_id')

        # for club_stats
        manager_club = mi.select(mi.officialId.alias('manager_id'),
                                 mi.currentTeam.id.alias('club_id'))

        dim_country = mi.select(country_id, country_info.country.alias('name'),
                                country_info.demonym.alias('demonym')).dropDuplicates().dropna()

        manager = mi.select(manager_id, mi.name.first.alias('first_name'),
                            mi.name.last.alias('lastname'), mi.birth.date.label.alias('dob')).dropna(subset=['dob'])

        dim_country_with_manager_id = mi.select(manager_id, country_id)

        manager = manager.withColumn('dob', format_date_type_udf(col('dob')).cast(DateType()))

        dim_manager = manager.join(dim_country_with_manager_id, on='manager_id').sort('manager_id')

        dim_manager_stats = dim_manager_stats.union(dim_manager).dropDuplicates().orderBy('manager_id')
        dim_country_stats = dim_country_stats.union(dim_country).dropDuplicates().dropna().orderBy('country_id')

    # dim_manager_stats.show(100, truncate=False)
    # dim_country_stats.show(100, truncate=False)

    # ========================================================= CLUB =========================================================
    # club_info
    for info in club_info:
        dim_club = info.select(info.id.alias('club_id'), 'name', info.club.shortName.alias('short_name'),
                               info.club.abbr.alias('abbr'))

        # stadium
        stadium_info = info.select('id', explode('grounds').alias('stadium_info'))
        dim_stadium = stadium_info.select(col('stadium_info.id').alias('stadium_id'), 'stadium_info.name',
                                          'stadium_info.capacity', 'stadium_info.city',
                                          'stadium_info.location.latitude',
                                          'stadium_info.location.longitude')

        dim_club_stats = dim_club_stats.union(dim_club).dropDuplicates().orderBy('club_id')
        dim_stadium_stats = dim_stadium_stats.union(dim_stadium).dropDuplicates().orderBy('stadium_id')
    #
    # dim_club_stats.show(1000, truncate=False)
    # dim_stadium_stats.show(1000, truncate=False)

    # club_stats
    for st in club_stats:
        item = dict(st)
        season = item['season_id']
        stat = item['club_stats']

        club_stats_columns = stat.select(stat.entity.alias('stats')).collect()
        club_stats_columns = sorted(club_stats_columns)
        club_stats_detail = stat.select(explode('stats').alias('club_stats_detail'))
        if club_stats_detail.count() > 0:
            grouped_club_stats_detail = club_stats_detail.withColumn('club_id', col('club_stats_detail.owner.club.id')) \
                .withColumn('stat_name', col('club_stats_detail.name')) \
                .withColumn('stat_value', col('club_stats_detail.value')) \
                .drop('club_stats_detail')

            club_id = club_stats_detail.select(col('club_stats_detail.owner.club.id').cast('int').alias('club_id'),
                                               explode('club_stats_detail.owner.grounds').alias('stadium_info'))

            club_id = club_id.join(manager_club, on='club_id')
            club_id = club_id.withColumn('stadium_id', lit(col('stadium_info.id'))).drop('stadium_info')

            for column in club_stats_columns:
                stat_col = column[0]
                club_stat_type = grouped_club_stats_detail.groupBy(col('club_id')).agg(
                    spark_sum(when(col('stat_name') == stat_col, lit(col('stat_value')))).alias(stat_col))

                club_id = club_id.withColumn('season_id', lit(int(season))).join(club_stat_type, 'club_id')
                club_id = club_id.withColumn(stat_col, lit(club_id[stat_col])).dropDuplicates().sort('club_id')

            fct_club_stats = fct_club_stats.union(club_id).orderBy('club_id')
    # fct_club_stats.show(100, truncate=False)

    # ========================================================= PLAYER =========================================================
    # player_info
    for p_info in player_info:
        country_info = p_info.nationalTeam
        position_info = p_info.info

        dim_country = p_info.select(country_info.isoCode.alias('country_id'), country_info.country.alias('name'),
                                    country_info.demonym.alias('demonym')).dropDuplicates().dropna()
        dim_country_with_player_id = p_info.select(p_info.playerId.alias('player_id'),
                                                   country_info.isoCode.alias('country_id'))

        dim_position = p_info.select(position_info.position.alias('position_id'),
                                     position_info.positionInfo.alias('name')).dropDuplicates().dropna()
        dim_position = dim_position.withColumn('position_id', get_first_letter_udf(col('name')))
        dim_position_with_player_id = p_info.select(p_info.playerId.alias('player_id'),
                                                    position_info.position.alias('position_id'),
                                                    position_info.positionInfo.alias('name')).dropDuplicates().dropna()
        dim_position_with_player_id = dim_position_with_player_id.withColumn('position_id',
                                                                             get_first_letter_udf(col('name'))).drop(
            'name')

        player = p_info.select(
            p_info.playerId.alias('player_id'),
            p_info.name.first.alias('first_name'),
            p_info.name.last.alias('lastname'),
            # format_date_type_udf(p_info.birth.date.label).alias('dob'),
            p_info.birth.date.label.alias('dob'),
            p_info.info.shirtNum.alias('shirt_num')
        ).dropna(subset=['dob'])

        player = player.withColumn('dob', format_date_type_udf(col('dob')).cast(DateType()))
        dim_player = player.join(dim_country_with_player_id, on='player_id').join(dim_position_with_player_id,
                                                                                  on='player_id').dropDuplicates()

        dim_player_stats = dim_player_stats.union(dim_player).dropDuplicates().orderBy('player_id')
        dim_country_stats = dim_country_stats.union(dim_country).dropDuplicates().dropna().orderBy('country_id')
        dim_position_stats = dim_position_stats.union(dim_position).dropDuplicates().dropna().orderBy('position_id')
    #
    # dim_player_stats.show(100, truncate=False)
    # dim_country_stats.show(100, truncate=False)
    # dim_position_stats.show(100, truncate=False)

    # player_stats
    for ps in player_stats:
        item = dict(ps)
        season = item['season_id']
        p_stat = item['player_stats']

        if p_stat.columns:
            p_stat = p_stat.groupBy('entity').agg(collect_list('stats').alias('stats'))

            # player_stats_columns = p_stat.select(p_stat.entity.alias('stats')).collect()
            # player_stats_columns = sorted(player_stats_columns)
            player_stats_columns = fct_player_stats[fct_player_stats.columns[3:]].columns
            player_stats_detail = p_stat.select(flatten(p_stat.stats).alias('stats'))
            player_stats_detail = player_stats_detail.select(explode('stats').alias('player_stats_detail'))

            grouped_player_stats_detail = player_stats_detail.withColumn('player_id',
                                                                         col('player_stats_detail.owner.playerId')) \
                .withColumn('stat_name', upper(col('player_stats_detail.name'))) \
                .withColumn('stat_value', col('player_stats_detail.value')) \
                .drop('player_stats_detail')

            try:
                player_id = player_stats_detail.select(col('player_stats_detail.owner.playerId').alias('player_id'),
                                                       col('player_stats_detail.owner.currentTeam.club.id').alias(
                                                           'club_id'))
            except:
                player_id = player_stats_detail.select(col('player_stats_detail.owner.playerId').alias('player_id'))
                player_id = player_id.withColumn('club_id', lit(None))

            for p_stat_col in player_stats_columns:

                player_stat_type = grouped_player_stats_detail.groupBy(col('player_id')).agg(
                    spark_sum(when(col('stat_name') == p_stat_col, lit(col('stat_value')))).alias(p_stat_col))

                player_id = player_id.withColumn('season_id', lit(season)).join(player_stat_type, 'player_id')
                player_id = player_id.withColumn(p_stat_col, lit(player_id[p_stat_col])).dropDuplicates().sort(
                    'player_id')

                try:
                    player_id.select(col(p_stat_col))
                except:
                    player_id = player_id.withColumn(p_stat_col, lit(None)).dropDuplicates().sort('player_id')

            fct_player_stats = fct_player_stats.union(player_id).orderBy('player_id')
            # fct_player_stats = fct_player_stats.persist(StorageLevel.DISK_ONLY)

    # fct_player_stats.show(100, truncate=False)

    # ========================================================= AWARD =========================================================
    # award
    def get_award(award_df, aw_type):
        if aw_type == 'month':
            award_info = award_df.select(col('award_month.awardTypeId').alias('award_id'),
                                         col('award_month.award').alias('name'))
            award_info = award_info.withColumn('type', lit('month'))
        else:
            award_info = award_df.select(award_df.awardTypeId.alias('award_id'), award_df.award.alias('name'))

            award_info = award_info.withColumn('type', lit('season'))

        return award_info

    for ma in month_award:
        item = dict(ma)
        m_award = item['month_award']
        award_month_list = m_award.columns
        if award_month_list:
            for month in award_month_list:
                award_month = m_award.select(explode(month).alias('award_month'))
                month_award_info = get_award(award_month, 'month')
                dim_award_stats = dim_award_stats.union(month_award_info)

    for sa in season_award:
        item = dict(sa)
        ss_award = item['season_award']
        ss_award_list = ss_award.columns
        if ss_award_list:
            season_award_info = get_award(ss_award, 'season')
            dim_award_stats = dim_award_stats.union(season_award_info)

    dim_award_stats = dim_award_stats.dropDuplicates().orderBy('award_id')
    # dim_award_stats.show(100, truncate=False)

    # award_content
    # for mac in month_award_content:
    #     month_award_content_data = mac.select(mac.id.alias('award_content_id'), mac.date.alias('time'))
    #
    #     award_content_list = mac.select(mac.metadata).collect()
    #
    # month_award_content_data = month_award_content_data

    # object_award
    month_awarded_ids_schema = StructType([
        StructField("award_id", FloatType(), True),
        StructField("player_id", FloatType(), True),
        StructField("manager_id", FloatType(), True),
        StructField("season_id", FloatType(), True),
    ])
    season_awarded_ids_schema = StructType([
        StructField("award_id", FloatType(), True),
        StructField("player_id", FloatType(), True),
        StructField("manager_id", FloatType(), True),
        StructField("club_id", FloatType(), True),
        StructField("season_id", FloatType(), True),
    ])

    month_awarded_ids = spark.createDataFrame([], month_awarded_ids_schema)
    season_awarded_ids = spark.createDataFrame([], season_awarded_ids_schema)

    def get_awarded_ids(award_df, ss_id, aw_type):
        if aw_type == 'month':
            award_info = award_df.select(col('award_month.awardTypeId').alias('award_id'),
                                         col('award_month.player.playerId').alias('player_id'),
                                         col('award_month.official.officialId').alias('manager_id'))
            award_info = award_info.withColumn('season_id', lit(ss_id))
        else:
            award_info = award_df.select(award_df.awardTypeId.alias('award_id'),
                                         award_df.player.playerId.alias('player_id'),
                                         award_df.official.officialId.alias('manager_id'),
                                         award_df.apiTeam.id.alias('club_id'))
            award_info = award_info.withColumn('season_id', lit(ss_id))

        return award_info

    for ma in month_award:
        item = dict(ma)
        season_id = item['season_id']
        m_award = item['month_award']
        award_month_list = m_award.columns
        if award_month_list:
            for month in award_month_list:
                award_month = m_award.select(explode(month).alias('award_month'))
                month_award_info = get_awarded_ids(award_month, season_id, 'month')
                month_awarded_ids = month_awarded_ids.union(month_award_info)

    for sa in season_award:
        item = dict(sa)
        season_id = item['season_id']
        ss_award = item['season_award']
        ss_award_list = ss_award.columns
        if ss_award_list:
            season_award_info = get_awarded_ids(ss_award, season_id, 'season')
            season_awarded_ids = season_awarded_ids.union(season_award_info)

    award_id_df = dim_award_stats.select('award_id')

    fct_object_award_stats_month = award_id_df.join(month_awarded_ids, on='award_id')
    fct_object_award_stats_season = award_id_df.join(season_awarded_ids, on='award_id')
    fct_object_award_stats_unioned = fct_object_award_stats_month.unionByName(fct_object_award_stats_season,
                                                                              allowMissingColumns=True).orderBy(
        'award_id')

    # fct_object_award_stats_unioned_cols = fct_object_award_stats_unioned.columns
    fct_object_award_stat = fct_object_award_stats_unioned.withColumn('object_id',
                                                                      coalesce(col('player_id'), col('manager_id'),
                                                                               col('club_id'))).drop('player_id',
                                                                                                     'manager_id',
                                                                                                     'club_id')
    fct_object_award_stats = fct_object_award_stats.union(fct_object_award_stat)
    # fct_object_award_stats = fct_object_award_stats_unioned.withColumn('object_id', coalesce(*[col(column) for column in fct_object_award_stats_unioned_cols])).drop(*fct_object_award_stats_unioned_cols)
    # fct_object_award_stats.show(100, truncate=False)

    # ========================================================= MATCH =========================================================
    # match_info
    for m_info in match_info:
        m_info_list = m_info.columns
        if m_info_list:
            match = m_info.select(m_info.id.alias('match_id'),
                                  m_info.teams.alias('teams'),
                                  m_info.kickoff.label.alias('datetime'),
                                  m_info.attendance.alias('attendance'), m_info.gameweek.gameweek.alias('game_week'),
                                  m_info.clock.label.alias('match_length_min'),
                                  m_info.clock.secs.alias('match_length_sec'),
                                  m_info.ground.alias('stadium'))

            match = match.withColumn('attendance', lit(col('attendance')).cast(DecimalType()))
            match = match.withColumn('game_week', lit(col('game_week')).cast(DecimalType()))

            match = match.withColumn('date_played', convert_bst_to_local_date_udf(col('datetime')).cast(DateType()))
            match = match.withColumn('time', convert_bst_to_local_time_udf(col('datetime')))

            match = match.withColumn('match_length_min', format_match_length_min_udf(col('match_length_min')))
            match = match.withColumn('match_length_min', lit(col('match_length_min')).cast(DecimalType()))
            match = match.withColumn('match_length_sec', lit(col('match_length_sec')).cast(DecimalType()))

            match = match.withColumn('home_team_id', lit(col('teams').getItem(0).getField('team').getField('id')))
            match = match.withColumn('away_team_id', lit(col('teams').getItem(1).getField('team').getField('id')))
            match = match.withColumn('home_team_final_score',
                                     lit(col('teams').getItem(0).getField('score')).cast(DecimalType()))
            match = match.withColumn('away_team_final_score',
                                     lit(col('teams').getItem(1).getField('score')).cast(DecimalType()))

            match = match.withColumn('stadium_id', lit(col('stadium.id')))

            match = match.drop('teams', 'datetime', 'stadium')

            dim_match_stats = dim_match_stats.unionByName(match, allowMissingColumns=True)
    # dim_match_stats.show(10, truncate=False)

    # for idx, m_stat in enumerate(match_stats):
    #     item = dict(m_stat)
    #     season_id = item['season_id']
    #     stats = item['match_stats']
    #     club_ids_in_match = [f'{club_id}.M' for club_id in stats.columns]
    #     # club_ids_in_match = stats.columns
    #     matches_per_season = stats.count()
    #     num_matches = dim_match_stats.count()
    #     gameweeks_list = dim_match_stats.select('game_week').collect()
    #     home_team_ids_list = dim_match_stats.select('home_team_id').collect()
    #     away_team_ids_list = dim_match_stats.select('away_team_id').collect()
    #
    #     # df = dim_match_stats.withColumn('id', monotonically_increasing_id())
    #     # df = df.filter(df['id'] >= matches_per_season).drop('id')
    #
    #
    #     # for i in range(0, matches_list, matches_per_season):
    #     #     gameweeks_list = dim_match_stats.select('game_week').limit(matches_per_season).collect()
    #     #     gameweeks = [gameweek[0] for gameweek in gameweeks_list]
    #     #     gameweeks = dim_match_stats.limit(matches_per_season).select('game_week').collect()
    #
    #     # gameweeks = [i for i in range(0, len(gameweeks_list), matches_per_season)]
    #
    #
    #     # gameweeks = dim_match_stats.groupBy('game_week').count()
    #     # gameweeks = gameweeks.rdd.collectAsMap()
    #
    #     gameweeks = [gameweek[0] for gameweek in gameweeks_list]
    #     gameweeks = [gameweeks[i:i + matches_per_season] for i in range(0, len(gameweeks), matches_per_season)][idx]
    #     quantity_of_each_gameweek = {gameweek: gameweeks.count(gameweek) for gameweek in gameweeks}
    #
    #     # print(quantity_of_each_gameweek)
    #
    #     home_team_ids = [home_id[0] for home_id in home_team_ids_list]
    #     # home_team_ids = [home_team_ids[i:i + value] for value in quantity_of_each_gameweek.values() for i in range(0, len(home_team_ids), value)]
    #
    #     away_team_ids = [away_id[0] for away_id in away_team_ids_list]
    #     # away_team_ids = [away_team_ids[i:i + quantity_of_each_gameweek] for i in range(0, len(away_team_ids), quantity_of_each_gameweek)]
    #     # away_team_ids = [away_team_ids[i:i + value] for value in quantity_of_each_gameweek.values() for i in range(0, len(away_team_ids), value)]
    #
    #     start = 0
    #     home_team_ids_grouped = []
    #     away_team_ids_grouped = []
    #     for value in quantity_of_each_gameweek.values():
    #         home_team_ids_grouped.append(home_team_ids[start:start + value])
    #         away_team_ids_grouped.append(away_team_ids[start:start + value])
    #         start += value
    #
    #     # for home_team_id in home_team_ids:
    #     #     for k, v in row.asDict().items():
    #     #         if v is not None and home_team_id == int(k):
    #     #             return row[k].M[0].value
    #
    #     test = [[1.0, 2.0, 130.0, 4.0, 6.0, 7.0, 9.0, 26.0, 12.0, 20.0],
    #             [12.0, 131.0, 23.0, 11.0, 131.0, 25.0, 15.0, 127.0, 34.0, 10.0, 38.0, 21.0],
    #             [23.0, 26.0, 1.0, 130.0, 7.0, 2.0, 4.0, 6.0, 12.0, 20.0, 9.0]]
    #     # test = [[1.0, 2.0, 130.0, 4.0, 6.0, 7.0, 9.0, 26.0, 12.0, 20.0]]
    #     def process_row(row):
    #         home_team_formation_used = []
    #         for k, v in row.asDict().items():
    #             if v is not None:
    #                 # for i, home_team_id_list in enumerate(test):
    #                 #     print(i)
    #                 #     if float(k) in home_team_id_list:
    #                 #         print(float(k))
    #                 #         # if home_team_id_list[home_team_id_list.index(float(k))] == float(k):
    #                 #         home_team_formation_used.append(row[k].M[0].value)
    #                 #         break
    #                 # else:
    #                 #     continue
    #                 # break
    #                 for i, home_team_id_list in enumerate(test):
    #                     print(i)
    #                     if float(k) in home_team_id_list:
    #                         print(float(k))
    #                         # if home_team_id_list[home_team_id_list.index(float(k))] == float(k):
    #                         home_team_formation_used.append(row[k].M[0].value)
    #                         break
    #                 else:
    #                     home_team_formation_used.extend(
    #                         [row[k].M[0].value for k, v in row.asDict().items() if v is not None])
    #                     break
    #                     # continue
    #                 # break
    #
    #         # formation_used = [row[k].M[0].value for k, v in row.asDict().items() if v is not None]
    #         # home_team_formation_used = [row[k].M[0].value for k, v in row.asDict().items() if v is not None]
    #         # home_team_formation_used = [next(row[k].M[0].value for home_team_id in home_team_ids if home_team_id == k) for k, v in row.asDict().items() if v is not None]
    #         # for home_team_id in home_team_ids_grouped:
    #         #     print(home_team_id)
    #
    #         # home_team_formation_used = [row[k].M[0].value for k, v in row.asDict().items() for home_team_id in home_team_ids_grouped if v is not None if int(k) in home_team_id if home_team_id[home_team_id.index(int(k))] == int(k)]
    #         # home_team_formation_used = [row[k].M[0].value for k, v in row.asDict().items() for home_team_id in test if v is not None if int(k) in home_team_id if home_team_id[home_team_id.index(int(k))] == int(k)]
    #         # home_team_formation_used = [row[k].M[0].value for k, v in row.asDict().items() if v is not None and any(
    #         #     int(k) in home_team_id and home_team_id[home_team_id.index(int(k))] == int(k) for home_team_id in test)]
    #         # home_team_formation_used = [[row[k].M[0].value for k, v in row.asDict().items() if v is not None and int(k) in home_team_id and home_team_id[
    #         #                                  home_team_id.index(int(k))] == int(k)] for home_team_id in test]
    #         # home_team_formation_used = [row[k].M[0].value for k, v in row.asDict().items() if v is not None for
    #         #                             home_team_id in test if int(k) in home_team_id and home_team_id.index(int(k)) == 0]
    #         # away_team_formation_used = [row[k].M[0].value for k, v in row.asDict().items() for away_team_id in away_team_ids_grouped if v is not None if int(k) in away_team_id if away_team_id[away_team_id.index(int(k))] == int(k)]
    #         # period_one_scores = [next((x.value for x in v.M if x.name == 'first_half_goals'), 0) for k, v in row.asDict().items() if v is not None]
    #         print(home_team_formation_used)
    #         # print(period_one_scores)
    #         # print(test)
    #
    #     stats.foreach(process_row)
    #
    #     # non_null_cols = []
    #     # # for col_name in stats.columns:
    #     # for i in range(0, stats.count()):
    #     #     if i == 0:
    #     #         each_round = stats.limit(1)
    #     #     else:
    #     #         each_round = stats.limit(i).subtract(stats.limit(i - 1))
    #     #
    #     #     for col_name in each_round.columns:
    #     #         if each_round.filter(each_round[col_name].isNull()).count() == 0:
    #     #             non_null_cols.append(col_name)
    #     #
    #     #     output = each_round.select(*non_null_cols)
    #     #     output.show(100)
    #
    #     # for club_id in club_ids_in_match:
    #     #     only_club_id = int(club_id.replace('.M', ''))
    #     #     # match_stat = stats.select(explode(club_id).alias('match_stat'))
    #     #     match_stat = stats.select(col(club_id).alias('match_stat')).dropna()
    #     #
    #     #     match_stat = match_stat.withColumn('home_team_formation_used', lit(col('match_stat').getItem(0).getField('value')))
    #     #     match_stat = match_stat.withColumn('away_team_formation_used', lit(col('match_stat').getItem(0).getField('value')))
    #         # match_stat = match_stat.withColumn('home_team_formation_used', lit(col('stat_value')))
    #         # match_stat = match_stat.select(col('match_stat.value').alias('home_team_formation_used')).where(col('match_stat.name') == 'formation_used')
    #
    #         # match_stat = match_stat.select(col('match_stat.name').alias('stat_name'), col('match_stat.value').alias('stat_value'))
    #         # match_stat = match_stat.filter(col('stat_name') == 'formation_used')
    #         # # home_team_ids = dim_match_stats.select('home_team_id').collect()
    #         # match_ids_list = [match_id[0] for match_id in dim_match_stats.select('match_id').collect()]
    #         # match_ids_str = ','.join([str(match_id) for match_id in match_ids_list])
    #         #
    #         # home_team_ids = [home_id[0] for home_id in dim_match_stats.select('home_team_id').collect()]
    #         # home_team_ids_str = ','.join([str(home_id) for home_id in home_team_ids])
    #         #
    #         # away_team_ids = [away_id[0] for away_id in dim_match_stats.select('away_team_id').collect()]
    #         # away_team_ids_str = ','.join([str(away_id) for away_id in away_team_ids])
    #         #
    #         # # match_stat = match_stat.withColumn('home_team_id', lit(only_club_id))
    #         # match_stat = match_stat.withColumn('match_id', split(lit(match_ids_str), ','))
    #         #
    #         # match_stat = match_stat.withColumn('home_team_id', split(lit(home_team_ids_str), ','))
    #         # match_stat = match_stat.withColumn('away_team_id', split(lit(away_team_ids_str), ','))
    #         #
    #         # match_stat = match_stat.select(explode('match_id').alias('match_id'), 'stat_name', 'stat_value')
    #         # # match_stat = match_stat.select(explode('home_team_id').alias('home_team_id'), 'away_team_id')
    #         # # match_stat = match_stat.select(explode('away_team_id').alias('away_team_id'))
    #         #
    #         # # match_stat.show(100, truncate=False)
    #         #
    #         # # dim_match_stats = dim_match_stats.unionByName(match_stat, allowMissingColumns=True)
    #         # # dim_match_stats = dim_match_stats.union(match_stat, )
    #         #
    #         # joined_df = dim_match_stats.join(match_stat, on='match_id')
    #         #
    #         # team_ids = joined_df.select('match_id', 'home_team_id', 'away_team_id', 'stat_value')
    #         #
    #         # team_ids = team_ids.withColumn('home_team_formation_used',
    #         #                                # when((col('home_team_id').isin(home_team_ids)), team_ids.stat_value).otherwise(334))
    #         #                                when((col('home_team_id') == only_club_id), team_ids.stat_value).otherwise(0))
    #         # team_ids = team_ids.withColumn('away_team_formation_used',
    #         #                                # when((col('away_team_id').cast('string').isin(away_team_ids)), team_ids.stat_value).otherwise(4231))
    #         #                                when((col('away_team_id') == only_club_id), team_ids.stat_value).otherwise(0))
    #         #
    #         # team_ids = team_ids.drop('home_team_id', 'away_team_id')
    #         #
    #         # joined_df = joined_df.join(team_ids, on='match_id')
    #         # dim_match_stats = dim_match_stats.withColumn('home_team_formation',
    #         #                                    when((col('stat_name') == 'formation_used') &
    #         #                                         (col('home_team_id').isin(home_team_ids)), lit(col('stat_value')))
    #         #                                    )
    #         #
    #         # dim_match_stats = dim_match_stats.withColumn('away_team_formation',
    #         #                                    when((col('stat_name') == 'formation_used') &
    #         #                                         (col('away_team_id').isin(away_team_ids)), lit(col('stat_value')))
    #         #                                    )
    #         # dim_match_stats = dim_match_stats
    #
    #         # dim_match_stats.show(100)
    #         # match_stat.show(100)
    #         # team_ids.show(100)
    #         # match_df.join(match_stat, on='')
    #
    #
    #         # stadium_info = info.select('id', explode('grounds').alias('stadium_info'))
    #         # dim_stadium = stadium_info.select(col('stadium_info.id').alias('stadium_id'), 'stadium_info.name',
    #         #                                       'stadium_info.capacity', 'stadium_info.city',
    #         #                                       'stadium_info.location.latitude',
    #         #                                       'stadium_info.location.longitude')

    # match_stats
    # fct_match_stats_schema_test = StructType([
    #     StructField("match_id", FloatType(), True),
    #     StructField("club_id", FloatType(), True),
    #     StructField("duel_lost", FloatType(), True),
    #     StructField("fk_foul_lost", FloatType(), True),
    #     StructField("formation_used", FloatType(), True),
    # ])
    # fct_match_stats_test = spark.createDataFrame([], fct_match_stats_schema_test)
    # df = spark.createDataFrame([
    #     (1, 75281, "Todo: formation_used", "formation_used", 433.0),
    #     (1, 75281, "Todo: fk_foul_lost", "fk_foul_lost", 8.0),
    #     (1, 75281, "Todo: duel_lost", "duel_lost", 45.0),
    #     (38, 75281, "Todo: formation_used", "formation_used", 433.0),
    #     (38, 75281, "Todo: fk_foul_lost", "fk_foul_lost", 9.0),
    #     (38, 75281, "Todo: duel_lost", "duel_lost", 35.0),
    #     (1, 75291, "Todo: formation_used", "formation_used", 433.0),
    #     (1, 75291, "Todo: fk_foul_lost", "fk_foul_lost", 2.0),
    #     (1, 75291, "Todo: duel_lost", "duel_lost", 15.0),
    #     (38, 75291, "Todo: formation_used", "formation_used", 442.0),
    #     (38, 75291, "Todo: fk_foul_lost", "fk_foul_lost", 11.0),
    #     (38, 75291, "Todo: duel_lost", "duel_lost", 25.0),
    # ])
    #
    # df = df.withColumnRenamed("_1", "club_id")
    # df = df.withColumnRenamed("_2", "match_id")
    # df = df.withColumnRenamed("_3", "description")
    # df = df.withColumnRenamed("_4", "name")
    # df = df.withColumnRenamed("_5", "value")
    #
    # match_id = df.select('match_id', 'club_id')
    #
    # match_stats_columns = fct_match_stats[fct_match_stats.columns[3:]].columns
    #
    # for m_stat_col in match_stats_columns:
    #     # match_stat_type = df.groupBy(col('match_id')).agg(
    #     #     first(when(upper(col('name')) == m_stat_col, lit(col('value')))).alias(m_stat_col))
    #     # pivoted_df = df.groupBy("match_id", "club_id").pivot("name").agg(first("value"))
    #     pivoted_df = df.groupBy("match_id", "club_id").pivot("name").agg(
    #         first(when(upper(col('name')) == m_stat_col, lit(col('value')))))
    #     # df_transformed = df.groupBy("club_id", "match_id").pivot("name").agg(first("value"))
    #     # df_joined = df.join(pivoted_df, on=['match_id', 'club_id'])
    #
    #     match_id = match_id.withColumn('season_id', lit(int(489))).join(pivoted_df, on='match_id')
    #     # match_id = match_id.withColumn(m_stat_col, lit(col(m_stat_col))).dropDuplicates()
    # fct_match_stats_test = fct_match_stats_test.union(match_id)
    # fct_match_stats_test.show()

    # for ms in match_stats:
    #     item = dict(ms)
    #     season_id = item['season_id']
    #     m_stats = item['match_stats']
    #
    #     match_entity = m_stats.select(m_stats.entity).collect()
    #     match_entity_df = spark.createDataFrame(match_entity)
    #     match_entity_df = match_entity_df.select('entity.*')
    #     entity_exprs = [f"`{stat_col}` as `{stat_col}`" for stat_col in match_entity_df.columns]
    #     match_entity_df = match_entity_df.selectExpr(*entity_exprs)
    #
    #     match_stats = m_stats.select(m_stats.stats).collect()
    #     match_stats_df = spark.createDataFrame(match_stats)
    #     match_stats_df = match_stats_df.select('stats.*')
    #     stats_exprs = [f"`{stat_col}` as `{stat_col}`" for stat_col in match_stats_df.columns]
    #     match_stats_df = match_stats_df.selectExpr(*stats_exprs)
    #
    #     club_ids_in_match = [f'{club_id}.M' for club_id in match_stats_df.columns]
    #     match_stats_columns = fct_match_stats[fct_match_stats.columns[3:]].columns
    #     match_info_detail = match_entity_df.select(match_entity_df.id.alias('match_id'),
    #                                                explode(match_entity_df.teams).alias('teams'))
    #     match_info_detail = match_info_detail.withColumn('club_id', col('teams.team.id')).drop('teams')
    #
    #     for club_id in club_ids_in_match:
    #         only_club_id = int(club_id.replace('.M', ''))
    #
    #         # match_stats_detail = match_stats_df.select(explode(club_id).alias('match_stats_detail'))
    #         match_stats_detail = match_stats_df.select(explode(club_id).alias('match_stats_detail')).collect()
    #         match_stats_detail_df = spark.createDataFrame(match_stats_detail)
    #         match_stats_detail_df = match_stats_detail_df.select('match_stats_detail.*')
    #         stat_detail_exprs = [f"`{stat_col}` as `{stat_col}`" for stat_col in match_stats_detail_df.columns]
    #         match_stats_detail_df = match_stats_detail_df.selectExpr(*stat_detail_exprs)
    #         match_stats_detail_df = match_stats_detail_df.withColumn('club_id', lit(only_club_id))
    #
    #         grouped_match_stats_detail = match_info_detail.join(match_stats_detail_df, on='club_id')
    #
    #         # grouped_match_stats_detail.show(100)
    #         # grouped_match_stats_detail.show(100, truncate=False)
    #         # match_stat = m_stats.select(col(club_id).alias('match_stat')).dropna()
    #
    #         # club_id = match_info_detail.withColumn('club_id', col('teams.team.id')).drop('teams')
    #         # match_stat_type = grouped_match_stats_detail.groupBy('match_id').agg(collect_list('name').alias('stats'))
    #         # player_stats_detail = m_stats.select(flatten(m_stats.stats).alias('stats'))
    #         # pivoted_df = grouped_match_stats_detail.groupBy("match_id", "club_id").pivot("name").agg(first("value"))
    #         # pivoted_df = pivoted_df.select('match_id', 'club_id', *match_stats_columns)
    #         # pivoted_df = pivoted_df.drop('club_id')
    #
    #
    #         for m_stat_col in match_stats_columns:
    #             grouped_match_stats_detail_filtered = grouped_match_stats_detail.filter(upper(col('name')) == m_stat_col)
    #             pivoted_df = grouped_match_stats_detail_filtered.groupBy("match_id", "club_id").pivot("name").agg(first("value"))
    #
    #             # match_stat_type = grouped_match_stats_detail_filtered.groupBy(col('match_id')).agg(
    #             #     first(when(upper(col('name')) == m_stat_col, lit(col('value')))).alias(m_stat_col))
    #             # pivoted_df = grouped_match_stats_detail.groupBy("match_id", "club_id").pivot("name").agg(
    #             #             first(when(upper(col('name')) == m_stat_col, lit(col('value')))))
    #             # pivoted_df = pivoted_df.select()
    #             # pivoted_df = pivoted_df.drop('club_id')
    #             # pivoted_df.show(100, truncate=False)
    #             # match_stat_type.show(100)
    #
    #             # pivoted_df = grouped_match_stats_detail.filter(upper(col('name')) == m_stat_col).groupBy("match_id", "club_id").pivot("name").agg(first("value"))
    #             # df_joined = grouped_match_stats_detail.join(pivoted_df, on=['match_id', 'club_id'])
    #
    #             # match_info_detail = match_info_detail.withColumn('season_id', lit(int(season_id))).join(match_stat_type, on='match_id')
    #             match_info_detail = match_info_detail.withColumn('season_id', lit(int(season_id))).join(pivoted_df, on='match_id')
    #             # match_info_detail = match_info_detail.withColumn(m_stat_col, lit(col(m_stat_col))).dropDuplicates()
    #             #
    #             # fct_match_stats = fct_match_stats.unionByName(match_info_detail, allowMissingColumns=True).dropna().orderBy('match_id')
    #             match_info_detail.show(100)

    data_transformed.update({
        "mandatory_tables": table_name_list,
        "columns": {
            'dim_season_columns': dim_season_columns,
            'dim_stadium_columns': dim_stadium_columns,
            'dim_club_columns': dim_club_columns,
            'dim_country_columns': dim_country_columns,
            'dim_position_columns': dim_position_columns,
            'dim_player_columns': dim_player_columns,
            'dim_manager_columns': dim_manager_columns,
            'dim_award_columns': dim_award_columns,
            # 'dim_award_content_columns': dim_award_content_columns,
            'dim_match_columns': dim_match_columns,
            # 'dim_match_stat_columns': dim_match_stat_columns,
            'fct_club_stat_columns': fct_club_stat_columns,
            'fct_player_stat_columns': fct_player_stat_columns,
            'fct_object_award_columns': fct_object_award_columns,
        },
        "column_type": {
            'dim_season_column_type': dim_season_column_type,
            'dim_stadium_column_type': dim_stadium_column_type,
            'dim_club_column_type': dim_club_column_type,
            'dim_country_column_type': dim_country_column_type,
            'dim_position_column_type': dim_position_column_type,
            'dim_player_column_type': dim_player_column_type,
            'dim_manager_column_type': dim_manager_column_type,
            'dim_award_column_type': dim_award_column_type,
            'dim_match_column_type': dim_match_column_type,
            'fct_club_stat_column_type': fct_club_stat_column_type,
            'fct_player_stat_column_type': fct_player_stat_column_type,
            'fct_object_award_column_type': fct_object_award_column_type,
        },
        "data": {
            'dim_season': dim_season_stats,
            'dim_stadium': dim_stadium_stats,
            'dim_club': dim_club_stats,
            'dim_country': dim_country_stats,
            'dim_position': dim_position_stats,
            'dim_player': dim_player_stats,
            'dim_manager': dim_manager_stats,
            'dim_award': dim_award_stats,
            # 'dim_award_content': dim_award_content_stats,
            'dim_match': dim_match_stats,
            'fct_club_stat': fct_club_stats,
            'fct_player_stat': fct_player_stats,
            'fct_object_award': fct_object_award_stats
            # 'fct_match_stat': fct_match_stats,
        }
    })
    return data_transformed


def test_transformed_data(transformed_data):
    print('=========================================== START TESTING TRANSFORMED DATA ===========================================')
    objects_to_test = []
    failed_tables = []

    MANDATORY_TABLES = transformed_data['mandatory_tables']
    MANDATORY_DFS = [SparkDFDataset(transformed_data['data'][table]) for table in MANDATORY_TABLES]
    MANDATORY_TABLE_COLS = [transformed_data['columns'][f'{table}_columns'] for table in MANDATORY_TABLES]
    MANDATORY_TABLE_COL_TYPE = [[field.dataType for field in transformed_data['column_type'][f'{table}_column_type'].fields] for table in MANDATORY_TABLES]

    objects_to_test.append({
        'tables': MANDATORY_TABLES,
        'dfs': MANDATORY_DFS,
        'table_cols': MANDATORY_TABLE_COLS,
        'table_col_type': MANDATORY_TABLE_COL_TYPE
    })

    for obj in objects_to_test:
        tables = obj['tables']
        dfs = obj['dfs']
        table_cols = obj['table_cols']
        table_col_types = obj['table_col_type']

        for table, df, table_col, table_col_type in zip(tables, dfs, table_cols, table_col_types):
            check_table_columns_to_match_set_result = check_table_columns_to_match_set(table, df, table_col)
            check_table_column_type_result = check_table_columns_type(table, df, table_col_type)

            if not check_table_columns_to_match_set_result:
                failed_tables.append(f'Check table columns to match set result: {table}')

            if not check_table_column_type_result:
                failed_tables.append(f'Check table column type result: {table}')

    if failed_tables:
        raise Exception(f'The following tables failed the test: {failed_tables}')

    print('=========================================== END TESTING TRANSFORMED DATA ===========================================')


def load_data(transformed_data):
    # print('Loaded data to snowflake')
    conn = snowflake.connector.connect(
        user='KHale',
        password=SNOWFLAKE_PWD,
        account='el88601.ap-southeast-1',
        database='EPL',
        schema='warehouse'
    )

    cursor = conn.cursor()

    for table, df in transformed_data['data'].items():

        match table:
            case 'fct_player_stat':
                arr_idx = [1, 3]
                stat_idx_from = 3
            case 'fct_club_stat':
                arr_idx = [1, 2, 3, 4]
                stat_idx_from = 4
            case 'fct_object_award':
                arr_idx = [1, 2, 3]
                stat_idx_from = 0
            case _:
                arr_idx = [1]
                stat_idx_from = 1

        new_data_idx_from = 0

        # sql = f"INSERT INTO {table} VALUES ({'%s, '*(len(df.columns) - 1) + '%s'})"
        insert_to_temp_table_sql = f"INSERT INTO temp_table VALUES ({'%s, ' * (len(df.columns) - 1) + '%s'})"
        drop_temp_table_sql = f"drop table temp_table"

        update_old_data_sql = f'''
            merge into {table} AS T1 using temp_table AS T2 on
            ({', '.join([f'T1.${i}' for i in arr_idx])}) = ({', '.join([f'T2.${i}' for i in arr_idx])})
            when matched then update set {', '.join([f'T1.{df.columns[i]} = T2.{df.columns[i]}' for i in range(stat_idx_from, len(df.columns))])}
            when not matched then insert values ({', '.join([f'T2.{df.columns[i]}' for i in range(new_data_idx_from, len(df.columns))])})
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
            # cursor.execute(sql, data)

        cursor.execute(update_old_data_sql)
        cursor.execute(drop_temp_table_sql)

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
    exe_time = format_time(sec)
    print(exe_time)
