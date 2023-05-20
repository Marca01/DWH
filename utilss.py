import re
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from datetime import datetime, date

def getFirstLetter(text):
    words = re.split(r'[ /]', text)
    first_letters = [word[0] for word in words]
    first_letters = ''.join(first_letters)
    return first_letters

def formatDateType(date_string):
    date_object = datetime.strptime(date_string, "%d %B %Y").date()
    return str(date_object)

def getAcademicYear(date: date, start_month: int = 9) -> str:
    if date.month < start_month:
        academic_year_start = date.year - 1
    else:
        academic_year_start = date.year

    academic_year_end = str(academic_year_start + 1)[2:]
    academic_year = f"{academic_year_start}-{academic_year_end}"
    return academic_year

def formatTime(sec):
    sec = round(sec)
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    formatted_time = f"{h:02d}:{m:02d}:{s:02d}"
    return formatted_time

getFirstLetterUdf = udf(getFirstLetter, StringType())
formatDateTypeUdf = udf(formatDateType, StringType())