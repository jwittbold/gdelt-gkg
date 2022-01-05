# from pyspark.sql import SparkSession
# import requests
# from gcam_schema import GCAM_schema

# import sys
# sys.path.append("..")
# from gdelt.schemas.gcam_schema import GCAM_schema

# import sys
# import os
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from schemas.gcam_schema import GCAM_schema




# gcam_codebook_url = 'http://data.gdeltproject.org/documentation/GCAM-MASTER-CODEBOOK.TXT'

# r = requests.get(gcam_codebook_url, stream=True)
# if not r.ok:
#     print(f'request returned with code {r.status_code}')



# spark = SparkSession \
#     .builder \
#     .master('local[*]') \
#     .appName('parse_gcam') \
#     .getOrCreate()


# gcam_codebook = 'file:///Users/jackwittbold/Desktop/Springboard_Data_Engineering/Capstone_Master/gdelt_repository/codebooks/gcam_master_codebook.txt'

# /Users/jackwittbold/Desktop/Springboard_Data_Engineering/Capstone_Master/gdelt_repo/codebooks/gcam_master_codebook.txt



# gcam_df = spark.read.text(gcam_codebook)
# gcam_df.show(2, truncate=False)


class GcamCodebook:

    def __init__(self):

        self.variable = str('')
        self.dictionary_id = int(0)
        self.dimension_id = int(0)
        self.value_type = str('')
        self.language_code = str('')
        self.dict_human_name = str('')
        self.dim_human_name = str('')
        self.dict_citation = str('')


    def __init__(self, variable, dictionary_id, dimension_id, value_type, language_code, 
            dict_human_name, dim_human_name, dict_citation):

            self.variable = variable
            self.dictionary_id = dictionary_id
            self.dimension_id = dimension_id
            self.value_type = value_type
            self.language_code = language_code
            self.dict_human_name = dict_human_name
            self.dim_human_name = dim_human_name
            self.dict_citation = dict_citation


    def values(self):

        return self.variable, self.dictionary_id, self.dimension_id, self.value_type, self.language_code, \
            self.dict_human_name, self.dim_human_name, self.dict_citation



def parse_gcam_codebook(line):

    val = line.split('\t', -1)
    if len(val) == 8:

        if val != '':

            try:
                gcam_codes = GcamCodebook(variable = str(val[0]),
                                        dictionary_id = int(val[1]),
                                        dimension_id = int(val[2]),
                                        value_type = str(val[3]),
                                        language_code = str(val[4]),
                                        dict_human_name = str(val[5]),
                                        dim_human_name = str(val[6]),
                                        dict_citation = str(val[7])
                                        )
            except Exception as e:
                print(e)
                    
        return gcam_codes.values()


# gcam_rdd = spark.sparkContext.parallelize([r.content])
# gcam_rdd = spark.sparkContext.textFile(gcam_codebook)

# header = gcam_rdd.first()

# gcam_rdd_body = gcam_rdd.filter(lambda row: row != header)

# gcam_parsed = gcam_rdd_body.map(lambda line: parse_gcam_codebook(line))

# gcam_df = spark.createDataFrame(gcam_parsed, schema=GCAM_schema)

# gcam_df.show()

# print(gcam_df.count())

