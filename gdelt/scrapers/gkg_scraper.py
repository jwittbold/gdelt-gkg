import os
import sys
import requests
import zipfile
import datetime
import logging
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.toml_config import config
from etl.parse_downloads import download_parser
from etl.parse_gkg_url import parse_url
from schemas.pipeline_metrics_schema import metrics_schema
from schemas.gkg_url_schema import url_schema


logging.basicConfig(level=logging.INFO)


# Scraper ETL paths
FS_PREFIX = config['FS']['PREFIX']
DOWNLOAD_PATH = config['ETL']['PATHS']['DOWNLOAD_PATH']
EXTRACT_PATH = config['ETL']['PATHS']['EXTRACT_PATH']


# build spark
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
        """
        Runs spark job create DF from files in downloads folder. Returns timestamp of last downloaded file.
        :params spark: SparkSession
        :type spark: instance of SparkSession
        :returns: timestamp of last downloaded GKG file, or None if directory is empty 
        """

        # if download directory empty or seemingly empty but contains hidden files, return None
        if [f for f in os.listdir(DOWNLOAD_PATH) if not f.startswith('.')] == []:
            return None

        else:
            # create glob of GKG files contained in download folder
            gkg_glob = sorted(glob.glob(f'{DOWNLOAD_PATH}/*.csv'))

            # create RDD from GKG files in download directory, parse downloads, create DF with metrics schema
            download_dir_rdd = spark.sparkContext.parallelize(gkg_glob)
            download_dir_parsed = download_dir_rdd.map(lambda item: download_parser(item))
            downloads_df = spark.createDataFrame(download_dir_parsed, schema=metrics_schema)

            # get last download date from files within downloads directory
            last_download_date = downloads_df.select(F.max(F.col('gkg_record_id')))
            # last_download_date.show()
            latest_download = last_download_date.first()[0]
            latest_download_timestamp = datetime.datetime.strptime(latest_download, '%Y%m%d%H%M%S')
            # print(latest_download_timestamp)

            return latest_download_timestamp

    
    def download_metrics(spark):
        """
        Spark job creates DF of download metrics. Writes download metrics DF as parquet file to download metrics folder.
        :param spark: SparkSession
        :type spark: instance of SparkSession
        :returns: None if download directory is empty 
        """

        # if download directory empty or seemingly empty but contains hidden files, return None
        if [f for f in os.listdir(DOWNLOAD_PATH) if not f.startswith('.')] == []:
            return None

        else:
            # create glob of all GKG files
            gkg_glob = sorted(glob.glob(f'{DOWNLOAD_PATH}/*.csv'))

            # apply metrics schema and create DF
            download_dir_rdd = spark.sparkContext.parallelize(gkg_glob)
            download_dir_parsed = download_dir_rdd.map(lambda item: download_parser(item))
            downloads_df = spark.createDataFrame(download_dir_parsed, schema=metrics_schema)
            
            # write download metrics DF. File is overwritten with each download run so that it always shows 
            # current state of GKG files in downloads folder
            print('Writing ** Download Metrics ** DF...')
            downloads_df \
                .coalesce(1) \
                .write \
                .mode('overwrite') \
                .parquet(f"{FS_PREFIX}{config['SCRAPER']['DOWNLOAD_METRICS']}")

            print('Current ** Download Metrics ** DF:')
            download_metrics_df = spark.read.parquet(f"{FS_PREFIX}{config['SCRAPER']['DOWNLOAD_METRICS']}/*.parquet")

            download_metrics_df.orderBy(F.col('gkg_timestamp').desc()).show(truncate=False)
            print(f'Total downloaded GKG files: {download_metrics_df.count()}')


    def check_exists(url):
        """
        Compares file to be downloaded to files within download directory. If exists, it skips it.
        :param url: The URL to check
        :type url: str
        :returns: bool
        """
        
        gkg_glob = sorted(glob.glob(f'{DOWNLOAD_PATH}/*.csv'))

        download_file_name = url.split('/')[-1].rstrip('.zip')
        file_uri = f'{DOWNLOAD_PATH}/{download_file_name}'

        if file_uri in gkg_glob:
        
            print(f'GKG file: {download_file_name} already downloaded. SKIPPING.')
            return True

        else:
            return False


    def download_extract(self, url: str):
        """
        Calls check_exists method to check if file has already been downloaded. 
        If not, downloads url, extracts zipped file, removes .zip file.
        :param url: The URL(s) to download and extract
        :type url: str
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
        :param start_date: Date in the format of 'xxxx-xx-xx', e.g., '2021-01-01'
        :type start_date: str
        :param end_date: Date in the format of 'xxxx-xx-xx', e.g., '2021-01-01', or 'now', which is converted to datetime.now() 
        :type end_date: str
        :param from_last: Set to true or false in config.toml, tells scraper to whether to start download from last known download date
        :type from_last: bool
        :returns gkg_url_array: Returns a tuple of GDELT GKG URLs for specified date range.
        :type gkg_url_array: tuple
        """
        
        # create Scraper instance
        gkg_scraper = Scraper(config)

        # get last download date 
        last_download_date = Scraper.get_last_download(spark)

        # create start_timestamp from start_date parameter
        start_timestamp = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        if last_download_date == None:
            last_download_date = start_timestamp
            print(f'Download directory does not contain any GKG files. Starting to download GKG files from: {start_timestamp} UTC')

        # create timestamp from end_date parameter
        if end_date == 'now':
            end_timestamp_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            end_timestamp = datetime.datetime.strptime(end_timestamp_str, '%Y-%m-%d %H:%M:%S')
            print(f'\nDownloading GKG records for date range: {start_timestamp} to {end_timestamp} (UTC Now)')
        else:
            end_timestamp = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.timedelta(minutes=15)
            print(f'Downloading GKG records for date range: {start_timestamp} to {end_timestamp}')
            
        print(f'\nLast Download Date: {last_download_date}')
        
        # checks if desired GKG files should download starting from last downloaded file or 
        # from a different start_date specified in config.toml -- for downloading different GKG date ranges
        if from_last == True:
            start_date = last_download_date
            print(f'Setting download start date to last downloaded record: {start_date}')
            print(f'config.toml START_DATE: {start_timestamp}')
        else:
            start_date = start_date
            print(f'Download starting from START_DATE in config.toml, NOT downloading from date of last downloaded GKG. \
                \nDownload starting from: {start_timestamp}')


        def collect_gkg_urls(feeds):
            """
            Uses Requests library to get content from GDELT GKG feeds. Logic identifies GKG URLs within specified date
            range and appends collected URLs to instance of Scraper class URL array.
            :param feeds: The GDELT GKG feeds to connect to and collect URLs from
            :type feeds: List of URL strings specified in config.toml
            :returns: Doesn't return anything but does append URLs to URL array
            """
            try:
                response = requests.get(feed)
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

                    # only collect .gkg and .translation.gkg files
                    if line.endswith('.translation.gkg.csv.zip') or line.endswith('.gkg.csv.zip'):

                        # isolate numeric date portion of GKG file name to create timestamp
                        target_url = line.split(' ')[2]
                        parsed_date = target_url[37:50]
                        parsed_timestamp = datetime.datetime.strptime(parsed_date, '%Y%m%d%H%M%S')
                        
                        if from_last == True:
                            start_timestamp = last_download_date
                        else:
                            start_timestamp = start_timestamp

                        if parsed_timestamp < start_timestamp or parsed_timestamp > end_timestamp:          
                            pass
                        
                        # append desired GKG URLs to url_array
                        else:
                            gkg_scraper.url_array.append(target_url)

            except Exception as e:
                print(f'Encounted exception when collecting URLs from GKG feeds: {e}')
                

        # DOWNLOAD FROM LAST_UPDATE // if elapsed time since last download < 15min:
        if datetime.datetime.utcnow() - last_download_date < datetime.timedelta(minutes=15):

            # set download feeds to last update file URLs
            feeds = config['FEEDS']['LAST_UPDATE']

            print("\n*** Last download within 15 minutes, downloading from 'LAST UPDATE' URLs... ***")
            print(f'URLS: {feeds}\n')

            # iterate over feeds to retrieve GKG URLs within specified date range
            for feed in feeds:

                # call collect_gkg_urls() method to get GKG URLs
                collect_gkg_urls(feeds)


        # DOWNLOAD FROM MASTER // if elapsed time since last download > 15min:
        else:
            print('\n*** Elapsed time since last download greater than 15 minutes ***')
            if from_last == True:
                start_date = last_download_date

            else:
                start_date = start_date
                print(f'Download starting from START_DATE in config.toml, NOT downloading from date of last downloaded GKG. \
                    \nDownload starting from: {start_timestamp}')


            # set download feeds to master file URLs
            feeds = config['FEEDS']['MASTER']

            print("\nDownloading from 'MASTER' URLs...")
            print(f'URLS: {feeds}\n')

            # iterate over feeds to retrieve GKG URLs within specified date range
            for feed in feeds:

                # call collect_gkg_urls() method to 
                collect_gkg_urls(feeds)

        # returns tuple of GKG URLs to be downloaded
        return gkg_scraper.url_values()



if __name__ == '__main__':

    # set scraper params to config file params
    scraper_values = Scraper(config).feed_parser(spark=spark,
                                                start_date=config['SCRAPER']['START_DATE'],
                                                end_date=config['SCRAPER']['END_DATE'],
                                                from_last=config['SCRAPER']['FROM_LAST'])

    # create RDD from scraper_values tuple
    url_tuple = scraper_values
    url_rdd = spark.sparkContext.parallelize(url_tuple)

    # call download_extract() method for each value within RDD
    url_rdd.foreach(lambda url: Scraper(config).download_extract(url))

    # parse values from RDD to create DF of URLs downloaded (for debug purposes only, DF is not written out) 
    # will show duplicates for files for dates equal to last download date (1 or 2 previously downloaded files)
    # those files are in fact skipped
    downloaded_gkg = url_rdd.map(lambda x: parse_url(x))
    downloaded_gkg_df = spark.createDataFrame(downloaded_gkg, schema=url_schema)
    print("\nCandidate GKG URLs for download - for DEBUG purposed only - files already downloaded will be marked 'SKIPPING' above.")
    downloaded_gkg_df.show(truncate=False)
    print(f'Number of candidate GKG files for current download: {downloaded_gkg_df.count()}')

    # Write download_metrics DF 
    Scraper.download_metrics(spark)