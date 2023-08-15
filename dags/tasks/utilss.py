import re
from bs4 import BeautifulSoup
import requests
import json
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from datetime import datetime, date
import pytz


def get_first_letter(text):
    words = re.split(r'[ /]', text)
    first_letters = [word[0] for word in words]
    first_letters = ''.join(first_letters)
    return first_letters


def format_date_type(date_string):
    date_object = datetime.strptime(date_string, "%d %B %Y").date()
    return str(date_object)


def get_academic_year(date: date, start_month: int = 9) -> str:
    if date.month < start_month:
        academic_year_start = date.year - 1
    else:
        academic_year_start = date.year

    academic_year_end = str(academic_year_start + 1)[2:]
    academic_year = f"{academic_year_start}-{academic_year_end}"
    return academic_year


def format_time(sec):
    sec = round(sec)
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    formatted_time = f"{h:02d}:{m:02d}:{s:02d}"
    return formatted_time


def get_bs_content(url):
    request = requests.get(url)
    content = request.content
    soup = BeautifulSoup(content, 'lxml')
    return soup


def extract_json_keys(json_str):
    json_keys = list(json.loads(json_str).keys())
    return json_keys

def convert_bst_to_local(bst_time_str):
    # Define the timezones
    bst = pytz.timezone('Europe/London')
    vietnam = pytz.timezone('Asia/Ho_Chi_Minh')

    # Define the time in BST
    bst_time_str = bst_time_str.replace('BST', '').replace('GMT', '').strip()
    time_format = "%a %d %b %Y, %H:%M"
    time_bst = datetime.strptime(bst_time_str, time_format)
    time_bst = bst.localize(time_bst)

    # Convert to Vietnamese time
    time_vietnam = time_bst.astimezone(vietnam)
    time_vietnam = time_vietnam.strftime(time_format)

    return time_vietnam
def convert_bst_to_local_date(bst_time_str):
    local_date = convert_bst_to_local(bst_time_str).split(',')[0]
    local_date = ' '.join(local_date.split(' ')[1:])
    date_object = datetime.strptime(local_date, "%d %b %Y").date()
    return str(date_object)
def convert_bst_to_local_time(bst_time_str):
    local_time = convert_bst_to_local(bst_time_str).split(',')[1]
    return local_time

def format_match_length_min(time_str):
    time_lst = [int(m) for m in time_str.replace('+', '').replace("'00", "").split(' ')]
    formatted_time = sum(time_lst)
    return formatted_time

def has_column(df, col_name):
    return col_name in df.columns

get_first_letter_udf = udf(get_first_letter, StringType())
format_date_type_udf = udf(format_date_type, StringType())
convert_bst_to_local_date_udf = udf(convert_bst_to_local_date, StringType())
convert_bst_to_local_time_udf = udf(convert_bst_to_local_time, StringType())
format_match_length_min_udf = udf(format_match_length_min, StringType())
# extract_json_keys_udf = udf(extract_json_keys, StringType())
