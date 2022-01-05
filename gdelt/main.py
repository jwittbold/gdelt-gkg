from pyspark.sql import SparkSession, SQLContext
from schemas.gcam_schema import GCAM_schema
from etl.parse_gcam import parse_gcam_codebook
from etl.parse_gkg import parse_gkg
from schemas.gkg_schema import gkg_schema

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('parse_gcam') \
    .getOrCreate()



##########################################################################
######################            GCAM TEST            ###################
##########################################################################


gcam_codebook = 'file:///Users/jackwittbold/Desktop/Springboard_Data_Engineering/Capstone_Master/gdelt_repository/codebooks/gcam_master_codebook.txt'

gcam_rdd = spark.sparkContext.textFile(gcam_codebook)

header = gcam_rdd.first()

gcam_rdd_body = gcam_rdd.filter(lambda row: row != header)

gcam_parsed = gcam_rdd_body.map(lambda line: parse_gcam_codebook(line))

gcam_df = spark.createDataFrame(gcam_parsed, schema=GCAM_schema)

gcam_df.show()

print(gcam_df.count())



##########################################################################
######################             GKG TEST            ###################
##########################################################################

gkg_file = '/test/20210401001500.gkg.csv'

raw_gkg = spark.sparkContext.textFile(gkg_file)
parsed = raw_gkg.map(lambda Row: parse_gkg(Row))

df = spark.createDataFrame(parsed, schema=gkg_schema)
df.printSchema()

df.show()
print(df.count())

##########################################################################
######################          GKG TRANS TEST         ###################
##########################################################################

gkg_translation_file = '/test/20210909034500.translation.gkg.csv'

raw_trans_gkg = spark.sparkContext.textFile(gkg_translation_file)
trans_parsed = raw_trans_gkg.map(lambda Row: parse_gkg(Row))

trans_df = spark.createDataFrame(trans_parsed, schema=gkg_schema)
# trans_df.printSchema()

trans_df.show()
print(trans_df.count())