import re
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def getFirstLetter(text):
    words = re.split(r'[ /]', text)
    first_letters = [word[0] for word in words]
    first_letters = ''.join(first_letters)
    return first_letters

getFirstLetterUdf = udf(getFirstLetter, StringType())