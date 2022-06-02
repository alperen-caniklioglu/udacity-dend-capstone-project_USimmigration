import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col
from datetime import datetime, timedelta
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, quarter, dayofyear
import inspect
import sql_queries
import time


# create spark session build function
def create_spark_session():
    '''
        - Creates spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()
    return spark


def check_empty_df(dataframe_name):
    '''
        - Checks if dataframe is populated after a load operation
        - Raises exception if dataframe is empty
        
            Args:
                dataframe_name (dataframe): spark dataframe
        
    '''
    print(f"running function: {inspect.stack()[0][3]}...")
    has_passed=False
    num_rows_df = dataframe_name.count() # number of rows
    print(f"number of rows:{num_rows_df}")
    if num_rows_df > 0:
        has_passed=True
        print("dataframe is not empty.")
        print("quality check:passed!")
    else:
        print("ERROR:dataframe is empty!")
        raise Exception("quality check for dataframe:failed!")
        
def check_df_cols(dataframe_name,checkvalue_columns):
    '''
        - Checks if the amount columns in the dataframe matches the expected amount
        - Raises exception if it doesn't match
        
            Args:
                dataframe_name (dataframe): spark dataframe
                checkvalue_columns (int): expected number of columns
        
    '''
    print(f"running function: {inspect.stack()[0][3]}...")
    has_passed=False
    num_cols_df=len(dataframe_name.columns)
    print(f"number of columns:{num_cols_df}")
    if num_cols_df == checkvalue_columns:
        has_passed=True
        print("dataframe has expected number of columns.")
        print("quality check:passed!")
    else:
        print(f"ERROR:number of columns does not match expected number of columns. expected:{checkvalue_columns}; found:{num_cols_df}")
        raise Exception("quality check for dataframe:failed!")

def stage_source_data(spark,source_data_dict):
    '''
        - Reads a dictionary including processing properties
        - Stages source data
        
            Args:
                spark: spark session
                source_data_dict (int): dictionary including source data and processing props for each source data
        
    '''
    
    print(f"running function: {inspect.stack()[0][3]}...")
    start_time=time.time()
    print("-"*50)
    for key, value in source_data_dict.items():
    # create read command
        if value.get('data_format')=='csv':
            read_cmd=f"{value.get('df_name')} = spark.read.option(\'mode\',\'DROPMALFORMED\').option(\'header\',True).{value.get('data_format')}(\'{value.get('path')}\',sep=\'{value.get('separator_in_source')}\')"
        else:
            read_cmd=f"{value.get('df_name')} = spark.read.option(\'mode\',\'DROPMALFORMED\').option(\'header\',True).{value.get('data_format')}(\'{value.get('path')}\')"

        # create view command
        create_view_cmd = f"{value.get('df_name')}.createOrReplaceTempView(\"{value.get('view_name')}\")"

        # create check command for dataframe dimensions
        check_rows_cmd = f"check_empty_df({value.get('df_name')})"
        check_cols_cmd = f"check_df_cols({value.get('df_name')},{value.get('expected_num_cols')})"
        # stage source data
        try:
            print(f"loading source data: {value.get('path')} into dataframe: {value.get('df_name')}")
            print(f"executing command: {read_cmd}")
            exec(read_cmd)
            print(f"{value.get('df_name')} has been created.")
        except Exception as e:
            print(e)

        # create temp view     
        try:
            print(f"creating temp view: {value.get('view_name')} for dataframe: {value.get('df_name')}")
            print(f"executing command: {create_view_cmd}")
            exec(create_view_cmd)
            print(f"temp view: {value.get('view_name')} has been created.")
        except Exception as e:
            print(e)

        # check dataframes
        try:
            print(f"checking if dataframe:{value.get('df_name')} is empty...")
            print(f"executing command: {check_rows_cmd}")
            exec(check_rows_cmd)
            print(f"checking number of columns of dataframe:{value.get('df_name')}")
            print(f"executing command: {check_cols_cmd}")
            exec(check_cols_cmd)
            print(f"staging of source data: {value.get('path')} complete.")
            print("-"*50)
        except Exception as e:
            print(e)
        end_time=time.time()
        duration=end_time-start_time
        print(f"execution of function complete: {inspect.stack()[0][3]}; duration: {duration} seconds.")
        print("-"*50)

# function to read & load map files
def load_code_label_map(spark,target_basepath, labels_file, record_delimeter, code_maps_exp_num_cols_dict):
    '''
        - Reads a given description file and extracts fields
        - Generates map files
        - Writes generated maps into csv files with given delimeter
        - Calls quality check function with specified expected column amount value
        
            Args:
                spark: spark session
                target_basepath (string): target directory for generated map files
                labels_file (string): source data to be read
                record_delimeter (string): delimeter to be used in target csv file of maps
                code_maps_exp_num_cols_dict (dict): dictionary including the amount of expected columns for  each map file
        
    '''
    print(f"running function: {inspect.stack()[0][3]}...")
    start_time=time.time()
    print("-"*50)
    print(f"target directory path for files to be generated: {target_basepath}")
    print(f"source file to be processed: {labels_file}")
    print(f"delimeter to be used as generating csv files: \'{record_delimeter}\'")
    print(f"dictionary specifying number of expected columns for each dataframe: {code_maps_exp_num_cols_dict}")
    print("-"*50)
    
    # read labels_file and perform initial clean (remove '\t')
    with open(labels_file) as f:
        file_content = f.read()
        file_content = file_content.replace('\t', '')
        
    def map_codes(file, idx):
        '''
        - Reads label file
        - Extracts values under a given index value
        - Creates key, value pairs from region of interest
        - Returns a dictionary
        
            Args:
                file (file): file object to read
                idx (str): index to be searched
    '''
        file_content2 = file_content[file_content.index(idx):]
        file_content2 = file_content2[:file_content2.index(';')].split('\n')
        file_content2 = [i.replace("'", "") for i in file_content2]
        code_dic = [i.split('=') for i in file_content2[1:]]
        code_dic = dict([i[0].strip(), i[1].strip()] for i in code_dic if len(i) == 2)
        return code_dic
    
    # extract labels
    print(f"extracting data from {labels_file}")
    i94cit_res = map_codes(file_content, "i94cntyl")
    i94port = map_codes(file_content, "i94prtl")
    i94mode = map_codes(file_content, "i94model")
    i94addr = map_codes(file_content, "i94addrl")
    i94visa = {'1':'Business',
        '2': 'Pleasure',
        '3' : 'Student'}
    print(f"extracting data from {labels_file} complete")
    print("-"*50)
    
    file_delimeter=record_delimeter
    
    # process citizenship/residence file and create map
    i94cit_res_label_target_file_name = "i94cit_res.csv"
    i94cit_res_label_target_file = os.path.join(target_basepath,i94cit_res_label_target_file_name)
    print(f"preparing file: {i94cit_res_label_target_file}...")
    # write headers into file
    with open (i94cit_res_label_target_file,'w') as f_i94cit_res:
        f_i94cit_res.write('country_code' + file_delimeter + 'country_name\n')
    # write values into file
    with open (i94cit_res_label_target_file,'a') as f_i94cit_res:
        #f_i94cit_res.write('country_code' + file_delimeter + 'country_name\n')
        for key, value in i94cit_res.items():
            country_code = key
            country_name = value
            i94cit_res_line = file_delimeter.join((country_code, country_name))
            i94cit_res_line = i94cit_res_line + "\n"
            f_i94cit_res.write(i94cit_res_line)
    print(f"{i94cit_res_label_target_file} ready.")
    # load data into dataframe
    print(f"loading {i94cit_res_label_target_file} into dataframe...")
    df_cit_res=spark.read.option('mode','DROPMALFORMED').option('header',True).csv(i94cit_res_label_target_file,sep=";")
    # read number of expected columns for the specified dataframe
    df_cit_res_expnumcols = code_maps_exp_num_cols_dict.get('df_cit_res')
    # check dataframe dimensions
    check_empty_df(df_cit_res)
    check_df_cols(df_cit_res,df_cit_res_expnumcols)
    # create temp view 
    df_cit_res=df_cit_res.na.drop("any")
    df_cit_res.createOrReplaceTempView("map_cit_res")
    # clean invalid labels for country records / allocate a common id for those
    df_cit_res=spark.sql("""
    with cte_1 as (select cast(country_code as int) as country_id,
    case when lower(country_name) like '%collapsed%' 
    or lower(country_name) like '%invalid%' 
    or lower(country_name) like '%no country code%' 
    then 'Unknown Country' else country_name end as country_name 
    from map_cit_res)
    select distinct case when country_name = 'Unknown Country' then 9999 else country_id end as country_id, country_name from cte_1
    """)
    # create temp view 
    df_cit_res.createOrReplaceTempView("map_cit_res")
    print(f"loading {i94cit_res_label_target_file} into dataframe complete.")
    print("-"*50)    
        
    # process addr file and create map
    # read and save i94addr labels
    i94addr_label_target_file_name = "i94addr.csv"
    i94addr_label_target_file = os.path.join(target_basepath,i94addr_label_target_file_name)
    print(f"preparing file: {i94addr_label_target_file}...")
    # write headers into file
    with open (i94addr_label_target_file,'w') as f_i94addr:
        f_i94addr.write('state_code' + file_delimeter + 'state_name\n')
    with open (i94addr_label_target_file,'a') as f_i94addr:
        #f_i94addr.write('state_code' + file_delimeter + 'state_name\n')
        for key, value in i94addr.items():
            state_code = key
            state_name = value
            i94addr_line = file_delimeter.join((state_code, state_name))
            i94addr_line = i94addr_line + "\n"
            f_i94addr.write(i94addr_line)
    print(f"{i94addr_label_target_file} ready.")
    # load data into dataframe
    print(f"loading {i94addr_label_target_file} into dataframe...")
    df_addr=spark.read.option('mode','DROPMALFORMED').option('header',True).csv(i94addr_label_target_file,sep=";")
    # read number of expected columns for the specified dataframe
    df_addr_expnumcols = code_maps_exp_num_cols_dict.get('df_addr')
    # check dataframe dimensions
    check_empty_df(df_addr)
    check_df_cols(df_addr,df_addr_expnumcols)
    # create temp view
    df_addr=df_addr.na.drop("any")
    df_addr.createOrReplaceTempView("map_addr")
    print(f"loading {i94addr_label_target_file} into dataframe complete.")
    print("-"*50)
            
    # process mode/transport file and create map
    # read and save i94mode labels
    i94mode_label_target_file_name = "i94mode.csv"
    i94mode_label_target_file = os.path.join(target_basepath,i94mode_label_target_file_name)
    # write headers into file
    print(f"preparing file: {i94mode_label_target_file}...")
    with open (i94mode_label_target_file,'w') as f_i94mode:
        f_i94mode.write('transport_code' + file_delimeter + 'transport_type\n')
    with open (i94mode_label_target_file,'a') as f_i94mode:
        #f_i94mode.write('transport_code' + file_delimeter + 'transport_type\n')
        for key, value in i94mode.items():
            transport_code = key
            transport_type = value
            i94mode_line = file_delimeter.join((transport_code, transport_type))
            i94mode_line = i94mode_line + "\n"
            f_i94mode.write(i94mode_line)
    print(f"{i94mode_label_target_file} ready.")
    # load data into dataframe
    print(f"loading {i94mode_label_target_file} into dataframe...")
    df_mode=spark.read.option('mode','DROPMALFORMED').option('header',True).csv(i94mode_label_target_file,sep=";")
    # read number of expected columns for the specified dataframe
    df_mode_expnumcols = code_maps_exp_num_cols_dict.get('df_mode')
    # check dataframe dimensions
    check_empty_df(df_mode)
    check_df_cols(df_mode,df_mode_expnumcols)
    # drop rows with null values
    df_mode=df_mode.na.drop("any")
    # create temp view 
    df_mode.createOrReplaceTempView("map_transport_mode")
    print(f"loading {i94mode_label_target_file} into dataframe complete.")
    print("-"*50)
    
    # create csv file for i94port and create map
    i94port_label_target_file_name = "i94port.csv"
    i94port_label_target_file = os.path.join(target_basepath,i94port_label_target_file_name)
    print(f"preparing file: {i94port_label_target_file}...")
    # write header into file
    with open (i94port_label_target_file,'w') as f_i94port:
        f_i94port.write('port_code' + file_delimeter + 'port_name' + file_delimeter + 'port_state\n')
    with open (i94port_label_target_file,'a') as f_i94port:
        #f_i94port.write('port_code' + file_delimeter + 'port_name' + file_delimeter + 'port_state\n')
        for key, value in i94port.items():
            value_arr = value.split(",")
            if len(value_arr)==2:
                port_code = key
                port_name = value_arr[0]
                port_statecode = value_arr[1].strip(" ")
                port_line = file_delimeter.join((port_code, port_name, port_statecode))
                port_line = port_line + "\n"
                f_i94port.write(port_line)
    print(f"{i94port_label_target_file} ready.")
    # load data into dataframe
    print(f"loading {i94port_label_target_file} into dataframe...")
    df_port=spark.read.option('mode','DROPMALFORMED').option('header',True).csv(i94port_label_target_file,sep=";")
    # read number of expected columns for the specified dataframe
    df_port_expnumcols = code_maps_exp_num_cols_dict.get('df_port')
    # check dataframe dimensions
    check_empty_df(df_port)
    check_df_cols(df_port,df_port_expnumcols)
    # create temp view
    df_port=df_port.na.drop("any")
    df_port.createOrReplaceTempView("map_port")
    print(f"loading {i94port_label_target_file} into dataframe complete.")
    print("-"*50)
                
       
    # create csv file for i94visa and create map          
    i94visa_label_target_file_name = "i94visa.csv"
    i94visa_label_target_file = os.path.join(target_basepath,i94visa_label_target_file_name)
    print(f"preparing file: {i94visa_label_target_file}...")
    # write header into file
    with open (i94visa_label_target_file,'w') as f_i94visa:
        f_i94visa.write('visa_code' + file_delimeter + 'visa_type' + '\n')
    with open (i94visa_label_target_file,'a') as f_i94visa:
        #f_i94visa.write('visa_code' + file_delimeter + 'visa_type' + '\n')
        for key, value in i94visa.items():
            visa_code = key
            visa_type = value
            visa_line = file_delimeter.join((visa_code, visa_type))
            visa_line = visa_line + "\n"
            f_i94visa.write(visa_line)
    print(f"{i94visa_label_target_file} ready.")
    # load data into dataframe
    print(f"loading {i94visa_label_target_file} into dataframe...")
    df_visa=spark.read.option('mode','DROPMALFORMED').option('header',True).csv(i94visa_label_target_file,sep=";")
    # read number of expected columns for the specified dataframe
    df_visa_expnumcols = code_maps_exp_num_cols_dict.get('df_visa')
    # check dataframe dimensions
    check_empty_df(df_visa)
    check_df_cols(df_visa,df_visa_expnumcols)
    # create temp view
    df_visa=df_visa.na.drop("any")
    df_visa.createOrReplaceTempView("map_visa")
    print(f"loading {i94visa_label_target_file} into dataframe complete.")
    print("-"*50)
    
    end_time=time.time()
    duration=end_time-start_time
    print(f"execution of function complete: {inspect.stack()[0][3]}; duration: {duration} seconds.")
    print('-'*50)
    
def create_target_tables(spark, target_directory, props_dictionary):
    '''
        - Reads a props dictionary including target tables of star schema and related processing props
        - Creates tables
        - Calls quality check functions
        - Writes tables into parquet files
        
            Args:
                spark: spark session
                target_directory (string): target directory for generated parquet files
                props_dictionary (dict): dictionary containing target object names, sql queries to perform and other properties
        
    '''
    print(f"running function: {inspect.stack()[0][3]}...")
    start_time=time.time()
    print("-"*50)
    # create udf for tiemstamp conversion
    get_datetime = udf(lambda vdt:(datetime(1960,1,1)+timedelta(days=vdt)).isoformat())
    spark.udf.register("get_datetime", get_datetime)
    
    # create target tables
    processing_props_dict=props_dictionary
    spark_warehouse_path = target_directory
    print(f"specified target directory:{spark_warehouse_path}")
    for table,processing_props in processing_props_dict.items():
        print(f"processing table: {table}")
        file_path=os.path.join(spark_warehouse_path,processing_props.get('target_file_name'))
        print(f"specified processing properties:{processing_props}")
        
        # exclusive processing for arrival date: data enriched before creating a new dataframe
        if table == 'df_dim_arrival_date':
            df=spark.sql("select distinct cast (arrdate as int) as arrdate from staging_immigration_table")
            df=df.withColumn("arrdate_conv", get_datetime(col("arrdate")))
            df.createOrReplaceTempView(processing_props.get('view_name'))
            df=spark.sql(processing_props.get('sql_query'))
            df.createOrReplaceTempView(processing_props.get('view_name'))
        else:
            df=spark.sql(processing_props.get('sql_query'))
            df.createOrReplaceTempView(processing_props.get('view_name'))
        print(f"{table} created. running quality check...")
        check_empty_df(df)
        check_df_cols(df,processing_props.get('expected_num_cols'))
        print(f"writing {table} into {file_path}")    
        df.write.mode("overwrite").parquet(file_path)
        print(f"processing complete: {table}")
        print("-"*50)
    print("-"*50)
    end_time=time.time()
    duration=end_time-start_time
    print(f"execution of function complete: {inspect.stack()[0][3]}; duration: {duration} seconds.")
    print('-'*50)
    
def main():
    """
    - Main function which executes functions stage_source_data, load_code_label_map and create_target_tables
    
    """
    print(f"running function: {inspect.stack()[0][3]}...")
    immigration_data_basepath="/home/workspace/sas_data"
    immigration_data_path=os.path.join(immigration_data_basepath,'*.parquet')

    # demographics data
    demographics_data_path="/home/workspace/us-cities-demographics.csv"

    # labels data
    labels_data = "/home/workspace/I94_SAS_Labels_Descriptions.SAS"

    # set target path for spark warehouse
    spark_warehouse_path="/home/workspace/spark-warehouse"
    
    # dictionary to be used as staging source data
    source_data_dict = {'immigration_data':{ 'path':immigration_data_path,
                                        'df_name':'df_staging_immigration',
                                        'view_name':'staging_immigration_table',
                                        'expected_num_cols':28,
                                       'data_format':'parquet'},
                    'demographics_data':{'path':demographics_data_path,
                                         'df_name':'df_staging_demographics',
                                         'view_name':'staging_demographics_table',
                                         'expected_num_cols':12,
                                         'separator_in_source':";",
                                         'data_format':'csv'}}
    
    # dictionary to check number of columns in each map dataframe
    code_maps_exp_num_cols = {'df_cit_res':2,
                           'df_addr':2,
                           'df_mode':2,
                           'df_port':3,
                           'df_visa':2}
    
    # dictionary specifying processing parameters as creating dimension and fact tables
    processing_props_dict={'df_dim_arrival_location':{'view_name':'dim_arrival_location',
                                             'sql_query':sql_queries.df_dim_arrival_location_sql,
                                            'expected_num_cols':4,
                                            'target_file_name':'arrival_location.parquet'},
                  'df_dim_demographics':{'view_name':'dim_demographics',
                                         'sql_query':sql_queries.df_dim_demographics_sql,
                                        'expected_num_cols':17,
                                        'target_file_name':'demographics.parquet'},
                  'df_dim_origin_country':{'view_name':'dim_origin_country',
                                           'sql_query':sql_queries.df_dim_origin_country_sql,
                                        'expected_num_cols':2,
                                        'target_file_name':'origin_country.parquet'},
                  'df_dim_arrival_date':{'view_name':'dim_arrival_date',
                                         'sql_query':sql_queries.df_dim_arrival_date_sql,
                                        'expected_num_cols':10,
                                        'target_file_name':'arrival_date.parquet'},
                  'df_dim_junk_visa_transport':{'view_name':'dim_junk_visa_transport',
                                                'sql_query':sql_queries.df_dim_junk_visa_transport_sql,
                                        'expected_num_cols':5,
                                        'target_file_name':'junk_visa_transport.parquet'},
                  'df_fact_immigration':{'view_name':'fact_immigration',
                                         'sql_query':sql_queries.df_fact_immigration_sql,
                                        'expected_num_cols':11,
                                        'target_file_name':'immigration.parquet'}}
    
    
    # build spark session
    spark = create_spark_session()
    
    # stage source data
    print("PHASE:STAGING")
    stage_source_data(spark, source_data_dict)
    print("PHASE: STAGING complete.")
    
    # extract & load code maps
    print("PHASE: LOADING MAPS")
    target_csv_delimeter=";"
    load_code_label_map(spark, spark_warehouse_path, labels_data,target_csv_delimeter,code_maps_exp_num_cols)
    print("PHASE: LOADING MAPS complete.")
    
    # create target tables
    print("PHASE: CREATING TARGET TABLES")
    create_target_tables(spark,spark_warehouse_path,processing_props_dict)
    print("PHASE: CREATING TARGET TABLES complete.")
    
    print(f"function: {inspect.stack()[0][3]} complete")
    
if __name__ == '__main__':
    main()
    