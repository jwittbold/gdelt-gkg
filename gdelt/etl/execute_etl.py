import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
import glob
import datetime


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.toml_config import config
from schemas.gkg_schema import gkg_schema
from etl.parse_gkg import gkg_parser
from etl.parse_downloads import download_parser


local_fs_prefix = 'file://'

# BLOB_BASE = f"{config['AZURE']['PREFIX']}{config['AZURE']['CONTAINER']}@{config['AZURE']['STORAGE_ACC']}{config['AZURE']['SUFFIX']}"
DOWNLOAD_PATH = config['PROJECT']['DOWNLOAD_PATH']

DOWNLOAD_METRICS = f"{config['SCRAPER']['DOWNLOAD_METRICS']}"
PIPELINE_METRICS = f"{config['ETL']['PATHS']['PIPELINE_METRICS']}"


# build spark
spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('write_transformed_gkg') \
    .getOrCreate()



class GkgBatchWriter:

    def __init__(self):
        self.config = config
        self.gkg_glob = str('')
        self.gkg_dict = dict({})
        self.period = str('')
        self.raw_input_path = str('')
        self.file_name = str('')
        self.translingual = bool()
        self.numeric_date_str = str('')
        self.date_time = str('')
        self.year = str('')
        self.month = str('')
        self.day = str('')
        self.hour = str('')
        self.minute = str()
        self.second = str('')
        self.gkg_version = str('')



    def batch_processor(self):

        local_fs_prefix = 'file://'

        processor = GkgBatchWriter()
        input_path = self.config['ETL']['PATHS']['IN_PATH']   

        processor.period = self.config['UPDATE']['PERIOD']
        
        processor.gkg_glob = glob.glob(f'{input_path}/*.csv')

        processed_gkg = GkgBatchWriter().check_processed()

            
        for gkg_file in sorted(processor.gkg_glob):

            file_name = gkg_file.split('/')[-1]

            if file_name not in processed_gkg:

                processor.raw_input_path = input_path
                processor.file_name = gkg_file.split('/')[-1].rstrip('.csv')
                processor.numeric_date_str = gkg_file.split('/')[-1].split('.')[0]
                processor.date_time = datetime.datetime.strptime(processor.numeric_date_str, '%Y%m%d%H%M%S')
                processor.year = gkg_file.split('/')[-1].split('.')[0][:4]
                processor.month = gkg_file.split('/')[-1].split('.')[0][4:6]
                processor.day = gkg_file.split('/')[-1].split('.')[0][6:8]
                processor.hour = gkg_file.split('/')[-1].split('.')[0][8:10]
                processor.minute = gkg_file.split('/')[-1].split('.')[0][10:12]
                processor.second = gkg_file.split('/')[-1].split('.')[0][12:14]
                processor.gkg_version = processor.file_name[14:]


                if processor.period == '15min':
                    gkg_key = f'{processor.year}{processor.month}{processor.day}{processor.hour}{processor.minute}|{processor.gkg_version}'
                    etl_mode = '15min'

                elif processor.period == 'hourly':
                    gkg_key = f'{processor.year}{processor.month}{processor.day}{processor.hour}|{processor.gkg_version}'
                    etl_mode = 'hourly'

                elif processor.period == 'daily':
                    gkg_key = f'{processor.year}{processor.month}{processor.day}|{processor.gkg_version}'
                    etl_mode = 'daily'

                else:
                    raise ValueError("UPDATE PERIOD must be set to 'daily', 'hourly', or '15min' within config.toml file.")

                if gkg_key not in processor.gkg_dict:

                    processor.gkg_dict[gkg_key] = ''

                if gkg_key in processor.gkg_dict.keys():
                    pass

                for key in processor.gkg_dict:

                    if processor.period == '15min':
                        prefix = key.split('|')[0]
                        suffix = key[13:]

                    elif processor.period == 'hourly':
                        prefix = key.split('|')[0]
                        suffix = key[11:]

                    elif processor.period == 'daily':
                        prefix = key.split('|')[0]
                        suffix = key[9:]
                    
                    else:
                        raise ValueError("UPDATE PERIOD must be set to 'daily', 'hourly', or '15min' within config.toml file.")

                    if gkg_file.startswith(f'{processor.raw_input_path}/{prefix}') and suffix == processor.gkg_version:

                        # concat paths to for use as large rdd
                        processor.gkg_dict[key] += f'{local_fs_prefix}{gkg_file},'

            else:
                print('GKG files already processed, passing.')

        return processor.gkg_dict.values()

    
    def check_processed(self):

        if os.path.exists(PIPELINE_METRICS):
            pipeline_metrics_df = spark.read.format('parquet').load(f'{local_fs_prefix}{PIPELINE_METRICS}/*.parquet') 
            unique_metrics_df = pipeline_metrics_df.dropDuplicates(['file_name', 'gkg_timestamp'])

            processed_gkg_array = unique_metrics_df.select('file_name').rdd.flatMap(lambda row: row).collect()
            
            return processed_gkg_array

        else:
            return []



    def write_batch(self):

        etl_out_path = self.config['ETL']['PATHS']['OUT_PATH']

        rdd_paths = GkgBatchWriter().batch_processor()

        writer = GkgBatchWriter()
        writer.period = self.config['UPDATE']['PERIOD']

        processed_gkg = GkgBatchWriter().check_processed()


        
        for value in rdd_paths:

            single_path = value.split(',')[0]

            writer.file_name = single_path.split('/')[-1]
            writer.numeric_date_str = value.split('/')[-1].split('.')[0]
            writer.date_time = datetime.datetime.strptime(writer.numeric_date_str, '%Y%m%d%H%M%S')
            writer.year = value.split('/')[-1].split('.')[0][:4]
            writer.month = value.split('/')[-1].split('.')[0][4:6]
            writer.day = value.split('/')[-1].split('.')[0][6:8]
            writer.hour = value.split('/')[-1].split('.')[0][8:10]
            writer.minute = value.split('/')[-1].split('.')[0][10:12]
            writer.second = value.split('/')[-1].split('.')[0][12:14]
            writer.gkg_version = writer.file_name[14:]

            if writer.gkg_version == '.translation.gkg.csv':
                writer.translingual = True
                version = 'translingual'
            else:
                writer.translingual = False
                version = 'eng'



            ########################################################################################
            ###########################      GKG BLOCK FROM URLS     ############################### 
            ######################################################################################## 

            gkg_paths = value[:-1]
            gkg_block_rdd = spark.sparkContext.textFile(gkg_paths)
            parsed = gkg_block_rdd.map(lambda line: gkg_parser(line))
            gkg_block_df = spark.createDataFrame(parsed, schema=gkg_schema)


            # print(gkg_paths)


            ########################################################################################
            #####################      WRITE OUT 15min / hourly mode     ########################### 
            ########################################################################################   

            if writer.period == '15min' or writer.period == 'hourly':
                print(f'Writing GKG records in {writer.period} mode')
                gkg_block_df\
                    .coalesce(1) \
                    .write \
                    .mode('append') \
                    .parquet(f'{local_fs_prefix}{etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}/')
                print(f'Successfully wrote ** {version} ** DF in {writer.period} mode to path: {etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}')




                ########################################################################################
                ##################     JOIN DOWNLOAD METRICS TO GKG METRICS     ######################## 
                ######################################################################################## 
                gkg_batch_df = spark.read \
                                .format('parquet') \
                                .schema(gkg_schema) \
                                .load(f'{local_fs_prefix}{etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}/*.parquet')
                

                gkg_metrics = gkg_batch_df \
                                    .select([
                                        F.col('GkgRecordId.Date').alias('gkg_record_id_a'), \
                                        F.col('GkgRecordId.Translingual').alias('translingual_a')]) \
                                        .withColumn('etl_timestamp', F.lit(datetime.datetime.now().isoformat(timespec='seconds', sep=' ')).cast(TimestampType())) \
                                    .groupBy('gkg_record_id_a', 'translingual_a', 'etl_timestamp') \
                                    .count() \
                                    .withColumnRenamed('count', 'total_rows')
                                                        

                download_metrics_df = spark.read.parquet(f'{local_fs_prefix}{DOWNLOAD_METRICS}/*.parquet')


                joined_metrics = download_metrics_df.join(F.broadcast(gkg_metrics),
                                                                    on=[ download_metrics_df.gkg_record_id == gkg_metrics.gkg_record_id_a,
                                                                        download_metrics_df.translingual == gkg_metrics.translingual_a],
                                                                    how='right') 
                                                                                                                            
                pipeline_metrics = joined_metrics.select('gkg_record_id', 'translingual', 'file_name', 'gkg_timestamp', 
                                                        'local_download_time', 'etl_timestamp', 'csv_size_mb', 'total_rows') 
                
                
                ########################################################################################
                ##################          WRITE OUT PIPELINE METRICS          ######################## 
                ######################################################################################## 
                
                pipeline_metrics.coalesce(1) \
                    .write \
                    .mode('append') \
                    .format('parquet') \
                    .save(f'{local_fs_prefix}{PIPELINE_METRICS}') 
                



            ########################################################################################
            ##########################      WRITE OUT DAILY MODE     ############################### 
            ########################################################################################     
            
            else:
                print(f'Writing GKG records in {writer.period} mode')
                gkg_block_df \
                        .repartition(5) \
                        .write \
                        .option('maxRecordsPerFile', 30000) \
                        .mode('append') \
                        .parquet(f'{local_fs_prefix}{etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}/')
                print(f'Successfully wrote ** {version} ** DF in {writer.period} mode to path: {etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}')
                


                ########################################################################################
                ##################     JOIN DOWNLOAD METRICS TO GKG METRICS     ######################## 
                ######################################################################################## 
                
                gkg_batch_df = spark.read \
                                .format('parquet') \
                                .schema(gkg_schema) \
                                .load(f'{local_fs_prefix}{etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}/*.parquet') 
                

                gkg_metrics = gkg_batch_df \
                                    .select([
                                        F.col('GkgRecordId.Date').alias('gkg_record_id_a'), \
                                        F.col('GkgRecordId.Translingual').alias('translingual_a')]) \
                                        .withColumn('etl_timestamp', F.lit(datetime.datetime.now().isoformat(timespec='seconds', sep=' ')).cast(TimestampType())) \
                                    .groupBy('gkg_record_id_a', 'translingual_a', 'etl_timestamp') \
                                    .count() \
                                    .withColumnRenamed('count', 'total_rows') 
                                                        

                download_metrics_df = spark.read.parquet(f'{local_fs_prefix}{DOWNLOAD_METRICS}/*.parquet')

                
                # rewrite as a right join broadcast batch df
                joined_metrics = gkg_metrics.join(F.broadcast(download_metrics_df), 
                                                                on=[ download_metrics_df.gkg_record_id == gkg_metrics.gkg_record_id_a,
                                                                    download_metrics_df.translingual == gkg_metrics.translingual_a],
                                                                how='left') 

                # joined_metrics = download_metrics_df.join(F.broadcast(gkg_metrics),
                #                                                     on=[ download_metrics_df.gkg_record_id == gkg_metrics.gkg_record_id_a,
                #                                                         download_metrics_df.translingual == gkg_metrics.translingual_a],
                #                                                     how='right') 
                
                                                                                                                                        
                pipeline_metrics = joined_metrics.select('gkg_record_id', 'translingual', 'file_name', 'gkg_timestamp', 
                                                        'local_download_time', 'etl_timestamp', 'csv_size_mb', 'total_rows') 

                
                
                ########################################################################################
                ##################          WRITE OUT PIPELINE METRICS          ######################## 
                ######################################################################################## 
                pipeline_metrics.coalesce(1) \
                    .write \
                    .mode('append') \
                    .format('parquet') \
                    .save(f'{local_fs_prefix}{PIPELINE_METRICS}') 




GkgBatchWriter().write_batch()



print('AS READ IN AS PARQUET WITH DROP DUPLICATES')
pipeline_metrics_df = spark.read.format('parquet').load(f'{local_fs_prefix}{PIPELINE_METRICS}/*.parquet') 
unique_metrics_df = pipeline_metrics_df.dropDuplicates(['file_name', 'gkg_timestamp'])
unique_metrics_df.show(truncate=False)
print(pipeline_metrics_df.count())
print(unique_metrics_df.count())