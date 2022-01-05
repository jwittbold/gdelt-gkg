import os
import sys
import requests
import zipfile
import datetime
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.toml_config import config
from etl.parse_downloads import download_parser
from etl.parse_gkg_url import parse_url
from schemas.pipeline_metrics_schema import metrics_schema
from schemas.gkg_url_schema import url_schema
import glob 



logging.basicConfig(level=logging.INFO)


DOWNLOAD_PATH = config['PROJECT']['DOWNLOAD_PATH']
EXTRACT_PATH = config['PROJECT']['EXTRACT_PATH']



spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('gkg_parallel_download') \
    .getOrCreate()




class Scraper:

    def __init__(self, config):

        self.config = config
        self.url_array = []

    def url_values(self):
        return tuple(self.url_array)


    def get_last_download(spark):

        # if download directory empty or seemingly empty but contains hidden files, return None
        # last download is set to start date as specified in config file
        if [f for f in os.listdir(DOWNLOAD_PATH) if not f.startswith('.')] == []:
            return None


        else:
            local_glob = sorted(glob.glob(f'{DOWNLOAD_PATH}/*.csv'))

            download_dir_rdd = spark.sparkContext.parallelize(local_glob)
            download_dir_parsed = download_dir_rdd.map(lambda item: download_parser(item))
            downloads_df = spark.createDataFrame(download_dir_parsed, schema=metrics_schema)
            # print('PIPELINE METRICS DF')
            # downloads_df.show(truncate=False)


            last_download_date = downloads_df.select(F.max(F.col('gkg_record_id')))
            # last_download_date.show()
            latest_download = last_download_date.first()[0]
            latest_download_timestamp = datetime.datetime.strptime(latest_download, '%Y%m%d%H%M%S')
            # print(latest_download_timestamp)

            return latest_download_timestamp


    
    def download_metrics(spark):


        if [f for f in os.listdir(DOWNLOAD_PATH) if not f.startswith('.')] == []:
            return None


        else:
            local_glob = sorted(glob.glob(f'{DOWNLOAD_PATH}/*.csv'))

            download_dir_rdd = spark.sparkContext.parallelize(local_glob)
            download_dir_parsed = download_dir_rdd.map(lambda item: download_parser(item))
            downloads_df = spark.createDataFrame(download_dir_parsed, schema=metrics_schema)
            # print('PIPELINE METRICS DF')
            # downloads_df.show(truncate=False)


            print('WRITING DOWNLOAD METRICS DF')
            downloads_df \
                .coalesce(1) \
                .write \
                .mode('overwrite') \
                .parquet(f"file://{config['SCRAPER']['DOWNLOAD_METRICS']}")
                # .parquet(f"file://{config['ETL']['PATHS']['PIPELINE_METRICS']}")

            print('READING PIPELINE METRICS BACK IN')
            download_metrics_df = spark.read.parquet(f"file://{config['SCRAPER']['DOWNLOAD_METRICS']}/*.parquet")

            download_metrics_df.orderBy(F.col('gkg_timestamp').desc()).show(truncate=False)
            print(download_metrics_df.count())



    def check_exists(url):

        local_glob = sorted(glob.glob(f'{DOWNLOAD_PATH}/*.csv'))

        download_file_name = url.split('/')[-1].rstrip('.zip')
        file_uri = f'{DOWNLOAD_PATH}/{download_file_name}'

        if file_uri in local_glob:
        
            print(f'GKG file: {download_file_name} already downloaded. Skipping.')
            return True

        else:
            return False


    def download_extract(self, url: str):
        """
        Calls check_exists method to check if file has already been downloaded. 
        If not, downloads url, extracts zipped file, removes .zip file.
        : param url :
        : type str :
        """

        if Scraper.check_exists(url) == False:
        # download files
            print(f'downloading: {url}')
            local_filename = url.split('/')[-1]
            local_filename = os.path.join(DOWNLOAD_PATH, local_filename)
            r = requests.get(url, stream=True)
            if not r.ok:
                print(f'request returned with code {r.status_code}')
                return None
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size= 1024 * 1024):
                    if chunk: 
                        f.write(chunk)
            
            # extract files 
            local_file = url.split('/')[-1]
            local_file = os.path.join(DOWNLOAD_PATH, local_file)
            print(f'extracting: {local_file}')
            zip_ref = zipfile.ZipFile(local_file, 'r')
            zip_ref.extractall(EXTRACT_PATH)
            zip_ref.close()
            os.remove(local_file)




    def feed_parser(self, spark, start_date, end_date, from_last) -> tuple(str()):
        """
        Parses GDELT GKG urls from list of feeds for specified date range
        : param feed : List of GDELT feeds containing list of GDELT files.
        : type URL : str
        : param start_date : Date in the format of 'xxxx-xx-xx', e.g., '2021-01-01'
        : type start_date : str
        : param end_date : Date in the format of 'xxxx-xx-xx', e.g., '2021-01-01', or 'now', which is converted to datetime.now() 
        : type end_date : str
        : returns gkg_url_array : Returns a tuple of GDELT GKG urls for specified date range.
        : type gkg_url_array : tuple
        """

        feeds = ['http://data.gdeltproject.org/gdeltv2/lastupdate.txt',
                'http://data.gdeltproject.org/gdeltv2/lastupdate-translation.txt',
                'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt',
                'http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt'
                ]
        
        gkg_scraper = Scraper(config)

        last_download_date = Scraper.get_last_download(spark)


        start_timestamp = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        if last_download_date == None:
            last_download_date = start_timestamp

        if end_date ==  'now':
            end_timestamp_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            end_timestamp = datetime.datetime.strptime(end_timestamp_str, '%Y-%m-%d %H:%M:%S')
        else:
            end_timestamp = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.timedelta(minutes=15)
            print(f'DOWNLOADING GKG RECORDS UP TO THIS DATE: {end_timestamp}')
            
        print(f'THIS IS THE LAST DOWNLOAD DATE: {last_download_date}')
        

        if from_last == True:
            start_date = last_download_date
            print(f'THIS IS THE START DATE BASED ON LAST DOWNLOADED RECORD: {start_date}')
            print(f'CONFIG FILE START TIMESTAMP: {start_timestamp}')


        if datetime.datetime.utcnow() - last_download_date < datetime.timedelta(minutes=15):


            feeds = feeds[0],feeds[1]
            print(feeds)
            print('DOWNLOADING FROM LAST UPDATE FILE LIST')


            for feed in feeds:

                response = requests.get(feed)
                # check headers here with check headers function
                response_str = str(response.content)

                for line in response_str.split("\\n"):
                    if not line:
                        continue
                    if line == '' or line == "'":
                        pass
                    if line.startswith("b'"):
                        line = line[2:]
                        # if other suffixes are desired remove pass and alter line.endswith() code below
                        pass

                    if line.endswith('.translation.gkg.csv.zip') or line.endswith('.gkg.csv.zip'):


                        target_url = line.split(' ')[2]

                        parsed_date = target_url[37:50]
                        parsed_timestamp = datetime.datetime.strptime(parsed_date, '%Y%m%d%H%M%S')
                        
                        if from_last == True:
                            start_timestamp = last_download_date

                        if parsed_timestamp < start_timestamp or parsed_timestamp > end_timestamp:          
                            pass
                        
                        else:
                            gkg_scraper.url_array.append(target_url)


        # IF CURRENT TIME - LAST DOWNLOAD TIME > 15MIN:
        else:
            print('TIME DELTA GREATER THAN 15 MIN')
            if from_last == True:
                start_date = last_download_date
                print(f'THIS IS THE START DATE BASED ON LAST DOWNLOADED RECORD: {start_date}')
                print(f'CONFIG FILE START TIMESTAMP: {start_timestamp}')
            feeds = feeds[2], feeds[3]
            print(feeds)
            print('DOWNLOADING FROM MASTER FILE LIST')



            for feed in feeds:

                response = requests.get(feed)
                # check headers here with check headers function
                response_str = str(response.content)

                for line in response_str.split("\\n"):
                    if not line:
                        continue
                    if line == '' or line == "'":
                        pass
                    if line.startswith("b'"):
                        line = line[2:]
                        pass

                    if line.endswith('.translation.gkg.csv.zip') or line.endswith('.gkg.csv.zip'):
                        

                        target_url = line.split(' ')[2]

                        parsed_date = target_url[37:50]
                        parsed_timestamp = datetime.datetime.strptime(parsed_date, '%Y%m%d%H%M%S')
                        
                        if from_last == True:
                            start_timestamp = last_download_date

                        if parsed_timestamp < start_timestamp or parsed_timestamp > end_timestamp:          
                            pass
                        
                        else:
                            gkg_scraper.url_array.append(target_url)
                            # print(target_url)


        return gkg_scraper.url_values()



scraper_values = Scraper(config).feed_parser(spark=spark,
                                            start_date=config['SCRAPER']['START_DATE'],
                                            end_date=config['SCRAPER']['END_DATE'],
                                            from_last=config['SCRAPER']['FROM_LAST'])



url_tuple = scraper_values
url_rdd = spark.sparkContext.parallelize(url_tuple)



url_rdd.foreach(lambda url: Scraper(config).download_extract(url))

downloaded_gkg = url_rdd.map(lambda x: parse_url(x))
downloaded_gkg_df = spark.createDataFrame(downloaded_gkg, schema=url_schema)

print('THIS IS THE DOWNLOADED GKG URL DF')
downloaded_gkg_df.show(truncate=False)
print(downloaded_gkg_df.count())


Scraper.download_metrics(spark)



# Scraper.check_exists(spark)



        
        # local_filename = url.split('/')[-1].rstrip('.zip')
        # file_uri = f'{DOWNLOAD_PATH}/{local_filename}'

        # if file_uri in Scraper.check_exists():
        
        #     print('GKG File already downloaded')




# +----------------------------------+--------------+-------------------+------------+-----------+-------------------+------------+-------------+----------+
# |file_name                         |gkg_date_str  |gkg_timestamp      |translingual|csv_size_mb|local_download_time|etl_complete|etl_timestamp|total_rows|
# +----------------------------------+--------------+-------------------+------------+-----------+-------------------+------------+-------------+----------+
# |20210930000000.gkg.csv            |20210930000000|2021-09-30 00:00:00|false       |17.581224  |2021-10-01 12:58:25|false       |null         |0         |
# |20210930000000.translation.gkg.csv|20210930000000|2021-09-30 00:00:00|true        |17.047162  |2021-10-01 12:58:24|false       |null         |0         |
# |20210930001500.gkg.csv            |20210930001500|2021-09-30 00:15:00|false       |20.529647  |2021-10-01 12:58:31|false       |null         |0         |
# |20210930001500.translation.gkg.csv|20210930001500|2021-09-30 00:15:00|true        |22.192322  |2021-10-01 12:58:31|false       |null         |0         |
# |20210930003000.gkg.csv            |20210930003000|2021-09-30 00:30:00|false       |20.756230  |2021-10-01 12:58:39|false       |null         |0         |
# |20210930003000.translation.gkg.csv|20210930003000|2021-09-30 00:30:00|true        |16.803040  |2021-10-01 12:58:40|false       |null         |0         |
# |20210930004500.gkg.csv            |20210930004500|2021-09-30 00:45:00|false       |19.090138  |2021-10-01 12:58:41|false       |null         |0         |
# |20210930004500.translation.gkg.csv|20210930004500|2021-09-30 00:45:00|true        |21.448328  |2021-10-01 12:58:44|false       |null         |0         |
# |20210930010000.gkg.csv            |20210930010000|2021-09-30 01:00:00|false       |19.812149  |2021-10-01 12:58:44|false       |null         |0         |
# |20210930010000.translation.gkg.csv|20210930010000|2021-09-30 01:00:00|true        |21.288236  |2021-10-01 12:58:48|false       |null         |0         |
# |20210930011500.gkg.csv            |20210930011500|2021-09-30 01:15:00|false       |16.458882  |2021-10-01 12:58:46|false       |null         |0         |
# |20210930011500.translation.gkg.csv|20210930011500|2021-09-30 01:15:00|true        |20.408245  |2021-10-01 12:58:50|false       |null         |0         |
# |20210930013000.gkg.csv            |20210930013000|2021-09-30 01:30:00|false       |17.588040  |2021-10-01 12:58:49|false       |null         |0         |
# |20210930013000.translation.gkg.csv|20210930013000|2021-09-30 01:30:00|true        |23.782540  |2021-10-01 12:58:53|false       |null         |0         |
# |20210930014500.gkg.csv            |20210930014500|2021-09-30 01:45:00|false       |18.620469  |2021-10-01 12:58:51|false       |null         |0         |
# |20210930014500.translation.gkg.csv|20210930014500|2021-09-30 01:45:00|true        |20.548345  |2021-10-01 12:58:56|false       |null         |0         |
# |20210930020000.gkg.csv            |20210930020000|2021-09-30 02:00:00|false       |17.896328  |2021-10-01 12:58:53|false       |null         |0         |
# |20210930020000.translation.gkg.csv|20210930020000|2021-09-30 02:00:00|true        |18.797424  |2021-10-01 12:59:01|false       |null         |0         |
# |20210930021500.gkg.csv            |20210930021500|2021-09-30 02:15:00|false       |16.274574  |2021-10-01 12:59:10|false       |null         |0         |
# |20210930021500.translation.gkg.csv|20210930021500|2021-09-30 02:15:00|true        |19.568697  |2021-10-01 12:59:05|false       |null         |0         |
# +----------------------------------+--------------+-------------------+------------+-----------+-------------------+------------+-------------+----------+
# only showing top 20 rows


