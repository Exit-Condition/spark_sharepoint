from datetime import date
import datetime
import sharepy
import os
from pyspark.sql import SparkSession, SQLContext

os.chdir("C:\\Users\\parix\\Downloads")

spark = SparkSession.builder\
    .appName("Sharepoint-Load")\
    .enableHiveSupport()\
    .getOrCreate()

# spark.sql.warehouse.dir  # config param for the warehouse directory

spark.sparkContext.setLogLevel("INFO")


class SharepointLoad:
    def __init__(self, load_type="incremental", site="7hyxw3.sharepoint.com", file=None, sess=None,
                 data=None, file_type='csv'):
        self.download_type = load_type
        self.site = site
        self.sess = sess
        self.file = file
        self.data = data
        self.file_type = file_type

    def con_setup(self, username: str, password: str):
        _username = username
        _password = password

        sess = sharepy.connect(self.site, _username, _password)
        self.sess = sess

    def download_file(self, site: str, i_file_path: str, o_file_path: str):
        self.sess.getfile(site + i_file_path, filename=o_file_path)
        self.file = o_file_path

    def save_file(self, target_file_loc, database_name: str, table_name: str):
        # to save the downloaded file this location will be  similar to where you would create the table later
        _db = database_name
        _tb = table_name
        _loc = target_file_loc
        df = spark.read.csv(self.file)
        df.write.mode("Overwrite").csv(_loc)

    def create_table(self, database_name: str, table_name: str, column_specs=''):
        # Create new table from the above downloaded file, column specs is defining columns with their data types
        # while creating table, if file type is csv and is already saved in hdfs col
        _db = database_name
        _tb = table_name
        _col = column_specs
        _type = self.file_type

        if _type.upper() == "CSV":
            if _col == '':
                df = spark.read.csv(self.file)
                print(df.show())
                file_name = os.path.basename(self.file).rsplit('.')[0]
                temp_view_name = 'temp_' + file_name
                df.createOrReplaceTempView(temp_view_name)
                df_2 = spark.sql("SELECT * from {}".format(temp_view_name))
                print(df_2.show())
                spark.sql(f"""create table if not exists {_db}.{_tb}
                              as (select * from {temp_view_name})
                          """)
                print(f"Table created {_db}.{_tb}")
            else:
                # Create external table if not exists sample_db.sample_table (id int, name varchar(50),
                # city varchar(100)) row format delimited
                spark.sql(f"""create external table if not exists {_db}.{_tb} ({_col})
                              row format delimited
                              fields terminated by ","
                              stored as textfile
                              location '{self.file}'
                              TBLPROPERTIES("skip.header.line.count"= "1")
                           """)
                print(f"table created {_db}.{_tb}")
        elif _type.upper() == "STRING":
            df = self.data
            print(df.show())
            file_name = os.path.basename(self.file).rsplit('.')[0]
            temp_view_name = 'temp_' + file_name
            df.createOrReplaceTempView(temp_view_name)
            df_2 = spark.sql("SELECT * from {}".format(temp_view_name))
            print(df_2.show())
            spark.sql(f"""create external table {_db}.{_tb} 
                          location '{self.file}' 
                          as (select * from {temp_view_name})
                       """)
            print("table created {_db}.{_tb}".format(_col=_col, _db=_db, _tb=_tb))
        else:
            print("Invalid file type")

    def load_table(self, database_name: str, table_name: str):  # use this to update the table
        _down_type = self.download_type
        _db = database_name
        _tb = table_name
        print(_down_type)
        if _down_type.upper() == "INCREMENTAL":
            df = spark.read.csv(self.file)
            df.createOrReplaceTempView("temp")
            spark.sql(f"""insert into {_db}.{_tb} (select * from temp)""")
        elif _down_type.upper() == "STATIC":
            self.save_file(f"/sync/prd_42124_edw_maxis_b/workspace/{_db}.db/{_tb}/data", _db, _tb)
        print(_db, ".", _tb, " updated successfully")


current_time = date.today() - datetime.timedelta(days=1)
a = SharepointLoad()

sp_username = 'admin@7hyxw3.onmicrosoft.com'
sp_password = 'ZQ7aw5gJZRSgy7m'
sp_site = "https://7hyxw3.sharepoint.com/sites/SparkInput/"
sp_file_name = 'Shared%20Documents/test_sharepoint.csv'
local_file_name = 'C:\\Users\\parix\\Downloads\\SharePoint\\out_test_sharepoint.csv'

a.con_setup(sp_username, sp_password)

try:
    a.download_file(sp_site, sp_file_name, local_file_name)
    a.create_table(database_name='sharepoint_poc', table_name='test_sharepoint')
    a.load_table(database_name='sharepoint_poc', table_name='test_sharepoint')
except Exception as e:
    print("Exception occurred - {}".format(str(e)))
