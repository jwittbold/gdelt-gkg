import os
import datetime
from decimal import Decimal


class GkgDirectoryParser:

    def __init__(self):
        self.local_path = str('')
        self.file_name = str('')
        self.gkg_record_id = str('')
        self.gkg_time = datetime.datetime.strptime('19000101000000', '%Y%m%d%H%M%S')
        self.gkg_version = str('')
        self.translingual = bool()
        self.csv_size = Decimal(0)
        self.download_time = datetime.datetime.strptime('19000101000000', '%Y%m%d%H%M%S')
        self.etl_complete = False
        self.etl_timestamp = None
        self.total_rows = int(0)

    def values(self):
        return self.file_name, self.gkg_record_id, self.gkg_time, self.translingual, self.csv_size, \
            self.download_time



def download_parser(gkg_file):

    gkg_path = GkgDirectoryParser()

    gkg_path.file_name = str(gkg_file.split('/')[-1])
    gkg_path.gkg_record_id = str(gkg_file.split('/')[-1].split('.')[0])
    gkg_path.gkg_time = datetime.datetime.strptime(gkg_path.gkg_record_id, '%Y%m%d%H%M%S')
    gkg_path.gkg_version = str(gkg_path.file_name[14:])
    if gkg_path.gkg_version == '.translation.gkg.csv':
        gkg_path.translingual = True
    else:
        gkg_path.translingual = False
    gkg_path.csv_size = Decimal(os.stat(gkg_file).st_size / 1000000)
    gkg_ctime = os.stat(gkg_file).st_ctime
    download_time_str = datetime.datetime.fromtimestamp(gkg_ctime).strftime('%Y%m%d%H%M%S')
    gkg_path.download_time = datetime.datetime.strptime(download_time_str, '%Y%m%d%H%M%S')

    return gkg_path.values()