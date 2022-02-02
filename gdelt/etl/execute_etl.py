import os
import sys
import shutil
import logging
import glob
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.toml_config import config
from schemas.gkg_schema import gkg_schema
from etl.parse_gkg import gkg_parser



FS_PREFIX = config['FS']['PREFIX']
# BLOB_BASE = f"{config['AZURE']['PREFIX']}{config['AZURE']['CONTAINER']}@{config['AZURE']['STORAGE_ACC']}{config['AZURE']['SUFFIX']}"
DOWNLOAD_PATH = config['ETL']['PATHS']['DOWNLOAD_PATH']
DOWNLOAD_METRICS = config['SCRAPER']['DOWNLOAD_METRICS']
PIPELINE_METRICS_TEMP = config['ETL']['PATHS']['PIPELINE_METRICS_TEMP']
PIPELINE_METRICS_FINAL = config['ETL']['PATHS']['PIPELINE_METRICS_FINAL']

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

        try:
            processor = GkgBatchWriter()
            input_path = self.config['ETL']['PATHS']['RAW_IN']   

            processor.period = self.config['BATCH']['PERIOD']
            
            processor.gkg_glob = glob.glob(f'{input_path}/*.csv')

            print(f'Checking for previously processed GKG files...')
            processed_gkg = GkgBatchWriter().check_processed()[0]

                
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
                        raise ValueError("BATCH PERIOD must be set to 'daily', 'hourly', or '15min' within config.toml file.")

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
                            raise ValueError("BATCH PERIOD must be set to 'daily', 'hourly', or '15min' within config.toml file.")

                        if gkg_file.startswith(f'{processor.raw_input_path}/{prefix}') and suffix == processor.gkg_version:

                            # concat paths for use as large RDD
                            processor.gkg_dict[key] += f'{FS_PREFIX}{gkg_file},'

                else:
                    print('GKG files already processed, passing.')

            return processor.gkg_dict.values()
        
        except Exception as e:
            print(f'Encountered exception when processing batch:\n{e}')

    
    def check_processed(self):

        try:
            if [f for f in os.listdir(PIPELINE_METRICS_FINAL) if not f.startswith('.')] != []:
                print('Pipeline metrics FINAL exists, reading from FINAL folder')
                pipeline_metrics_temp_df = spark.read.format('parquet').load(f'{FS_PREFIX}{PIPELINE_METRICS_FINAL}/*.parquet') 
                unique_metrics_df = pipeline_metrics_temp_df.dropDuplicates(['file_name', 'gkg_timestamp'])
                processed_gkg_array = unique_metrics_df.select('file_name').rdd.flatMap(lambda row: row).collect()
                
                return processed_gkg_array, True

            elif [f for f in os.listdir(PIPELINE_METRICS_TEMP) if not f.startswith('.')] != []:
                print('Pipeline metrics TEMP exists, reading from TEMP folder')
                pipeline_metrics_temp_df = spark.read.format('parquet').load(f'{FS_PREFIX}{PIPELINE_METRICS_TEMP}/*.parquet') 
                unique_metrics_df = pipeline_metrics_temp_df.dropDuplicates(['file_name', 'gkg_timestamp'])
                processed_gkg_array = unique_metrics_df.select('file_name').rdd.flatMap(lambda row: row).collect()
                
                # print('returning processed gkg array and "FALSE"')
                return processed_gkg_array, False

            else:
                print(f'No record of processed GKG files. Processing GKG and creating pipeline metrics...')
                return [],[]

        except Exception as e:
            print(f'\nEncountered exception while checking for processed files in pipeline metrics folder:\n{e}')
            

    def create_pipeline_metrics(self, fs, etl_out_path, version, year, month, day):

        try:
            gkg_batch_df = spark.read \
                            .format('parquet') \
                            .schema(gkg_schema) \
                            .load(f'{FS_PREFIX}{etl_out_path}/{version}/{year}/{month}/{day}/*.parquet')
            

            gkg_metrics = gkg_batch_df \
                                .select([
                                    F.col('GkgRecordId.Date').alias('gkg_record_id_a'), \
                                    F.col('GkgRecordId.Translingual').alias('translingual_a')]) \
                                    .withColumn('etl_timestamp', F.lit(datetime.datetime.now().isoformat(timespec='seconds', sep=' ')) \
                                    .cast(TimestampType())) \
                                .groupBy('gkg_record_id_a', 'translingual_a', 'etl_timestamp') \
                                .count() \
                                .withColumnRenamed('count', 'total_rows')
            
                                                    

            download_metrics_df = spark.read.parquet(f'{FS_PREFIX}{DOWNLOAD_METRICS}/*.parquet')


            joined_metrics = download_metrics_df.join(F.broadcast(gkg_metrics),
                                                                on=[ download_metrics_df.gkg_record_id == gkg_metrics.gkg_record_id_a,
                                                                    download_metrics_df.translingual == gkg_metrics.translingual_a],
                                                                how='inner')
                                     
            pipeline_metrics_temp_df = joined_metrics.select('gkg_record_id', 'translingual', 'file_name', 'gkg_timestamp', 
                                                    'local_download_time', 'etl_timestamp', 'csv_size_mb', 'total_rows')

            # write PIPELINE METRICS TEMP    
            pipeline_metrics_temp_df.coalesce(1) \
                .write \
                .mode('append') \
                .format('parquet') \
                .save(f'{FS_PREFIX}{PIPELINE_METRICS_TEMP}') 
        
        except Exception as e:
            print(f'Encountered exception while attempting to create pipeline metrics:\n{e}')
        
    

    def write_batch(self):


        try:
            etl_out_path = self.config['ETL']['PATHS']['TRANSFORMED_OUT']

            rdd_paths = GkgBatchWriter().batch_processor()

            writer = GkgBatchWriter()
            writer.period = self.config['BATCH']['PERIOD']

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



                ########################################################################################
                #####################      WRITE OUT 15min / hourly mode     ########################### 
                ########################################################################################   

                if writer.period == '15min' or writer.period == 'hourly':
                    print(f'Writing ** {version.upper()} ** GKG records in {(writer.period).upper()} mode')
                    gkg_block_df\
                        .coalesce(1) \
                        .write \
                        .mode('append') \
                        .parquet(f'{FS_PREFIX}{etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}/')
                    print(f'Successfully wrote ** {version.upper()} ** GKG files in {(writer.period).upper()} mode to path: {etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}')

                    # create pipeline metrics DF
                    GkgBatchWriter().create_pipeline_metrics(fs=FS_PREFIX, etl_out_path=etl_out_path, version=version, year=writer.year, month=writer.month, day=writer.day)

                

                ########################################################################################
                ##########################      WRITE OUT DAILY MODE     ############################### 
                ########################################################################################     
                
                else:
                    print(f'Writing ** {version.upper()} ** GKG records in {(writer.period).upper()} mode')
                    gkg_block_df \
                            .repartition(5) \
                            .write \
                            .option('maxRecordsPerFile', 30000) \
                            .mode('append') \
                            .parquet(f'{FS_PREFIX}{etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}/')
                    print(f'Successfully wrote ** {version.upper()} ** GKG files in {(writer.period).upper()} mode to path: {etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}')
                    
                    # create pipeline metrics DF
                    GkgBatchWriter().create_pipeline_metrics(fs=FS_PREFIX, etl_out_path=etl_out_path, version=version, year=writer.year, month=writer.month, day=writer.day)

            # for unit testing purposes
            return True

        except Exception as e:
            print(f'Encountered exception while writing batch:\n{e}')

            # for unit testing purposes
            return False


if __name__ == '__main__':


    ########################################################################################
    ###########################      WRITE OUT BATCH GKG     ############################### 
    ######################################################################################## 
    

    GkgBatchWriter().write_batch()


    # first time processing GKG files the read comes from pipeline temp folder
    # subsequent reads come from pipeline final folder

    try:
        if GkgBatchWriter().check_processed()[1] == True:
            for f in os.listdir(PIPELINE_METRICS_FINAL):
                if not f.startswith('.'):
                    full_file_name = os.path.join(PIPELINE_METRICS_FINAL, f)
                    if os.path.isfile(full_file_name):
                        shutil.copy(full_file_name, PIPELINE_METRICS_TEMP)

            print('joining from final pipeline metrics df')
            pipeline_metrics_temp_df = spark.read.format('parquet').load(f'{FS_PREFIX}{PIPELINE_METRICS_TEMP}/*.parquet') 
            unique_metrics_df = pipeline_metrics_temp_df.dropDuplicates(['file_name', 'gkg_timestamp'])
            # for DEBUG purposes - showing count including duplicates
            print(f'DEBUG: Pipeline metrics temp df count: {pipeline_metrics_temp_df.count()}')

        elif GkgBatchWriter().check_processed()[1] == False:
            print('joining from temp pipeline metrics df')
            pipeline_metrics_temp_df = spark.read.format('parquet').load(f'{FS_PREFIX}{PIPELINE_METRICS_TEMP}/*.parquet') 
            unique_metrics_df = pipeline_metrics_temp_df.dropDuplicates(['file_name', 'gkg_timestamp'])       
            # for DEBUG purposes - showing count including duplicates
            print(f'DEBUG: Pipeline metrics temp df count: {pipeline_metrics_temp_df.count()}')
    


        ########################################################################################
        ##################      WRITE OUT FINAL PIPELINE METRICS DF     ######################## 
        ########################################################################################     

        # first time processing GKG files the read comes from pipeline temp folder
        # subsequent reads come from pipeline final folder

        print('Writing *** PIPELINE METRICS FINAL DF ***...')
        unique_metrics_df.coalesce(1) \
            .write \
            .mode('overwrite') \
            .format('parquet') \
            .save(f'{FS_PREFIX}{PIPELINE_METRICS_FINAL}')


        print('*** PIPELINE METRICS FINAL DF ***')
        pipeline_metrics_final_df = spark.read.format('parquet').load(f'{FS_PREFIX}{PIPELINE_METRICS_FINAL}/*.parquet')
        pipeline_metrics_final_df.sort('gkg_timestamp').show(300, truncate=False)
        print(f'Total GKG records: {pipeline_metrics_final_df.count()}')

        print(f'Deleting temp files in pipeline metrics temp folder...')
        for f in os.listdir(PIPELINE_METRICS_TEMP):
            os.remove(os.path.join(PIPELINE_METRICS_TEMP, f))
        print(f'Deleted all files in pipeline metrics temp folder.')


    except Exception as e:
        print(f'Encountered exception while attempting to write out FINAL PIPELINE METRICS DF:\n{e}')