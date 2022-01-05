from pyspark.sql import SparkSession 
import sys
import os


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))



from schemas.gcam_schema import GCAM_schema
from etl.parse_gcam import parse_gcam_codebook

from utils.utils import get_project_root


# gcam_codebook_url = 'http://data.gdeltproject.org/documentation/GCAM-MASTER-CODEBOOK.TXT'

# r = requests.get(gcam_codebook_url, stream=True)
# if not r.ok:
#     print(f'request returned with code {r.status_code}')



spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('gcam_test') \
    .getOrCreate()


# gcam_codebook = 'file:///Users/jackwittbold/Desktop/Springboard_Data_Engineering/Capstone_Master/gdelt_repository/codebooks/gcam_master_codebook.txt'
gcam_codebook = 'file:///Users/jackwittbold/Desktop/Springboard_Data_Engineering/Capstone_Master/gdelt_repo/gdelt/resources/gcam_master_codebook.txt'



gcam_rdd = spark.sparkContext.textFile(gcam_codebook)

header = gcam_rdd.first()

gcam_rdd_body = gcam_rdd.filter(lambda row: row != header)

gcam_parsed = gcam_rdd_body.map(lambda line: parse_gcam_codebook(line))

gcam_df = spark.createDataFrame(gcam_parsed, schema=GCAM_schema)

gcam_df.show()

print(gcam_df.count())

print(get_project_root())