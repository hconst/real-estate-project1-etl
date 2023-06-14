import boto3
import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from io import StringIO



def get_s3_file_key(s3_client, s3_bucket, source_key): # search for a .csv file in specified folder
    response_list = s3_client.list_objects(Bucket=s3_bucket, Prefix=source_key) 
    for object in response_list['Contents']:
        if object['Key'].endswith('.csv'):
            return object['Key']
    return None

def get_file_data(s3_client, s3_bucket, file_key): # take the found file and make it a dataframe
    response_get_object = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
    data = response_get_object['Body']
    data_csv = pd.read_csv(data, sep='\t')
    return pd.DataFrame(data_csv)

def get_db_connection(): # connect to our database
    return psycopg2.connect(
        host='your-host',
        port='your-port',
        database='your-database',
        user='your-username',
        password='your-password'
    )

def send_data_to_db(data_df, conn): # send our data to the database
    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)
    table_name = 'properties_data'
    dtype = {
        'purpose': sqlalchemy.types.VARCHAR(255),
        'address': sqlalchemy.types.VARCHAR(255),
        'region': sqlalchemy.types.VARCHAR(255),
        'size_m2': sqlalchemy.types.INTEGER,
        'design': sqlalchemy.types.VARCHAR(255),
        'price_czk': sqlalchemy.types.INTEGER,
        'price_for_m2': sqlalchemy.types.INTEGER,
        'link': sqlalchemy.types.VARCHAR(255),
        'dump_date': sqlalchemy.types.VARCHAR(255),
        'file_name': sqlalchemy.types.VARCHAR(255)
    }
    data_df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype)

def archive_data(s3_client, s3_bucket, file_key, data_df): # archivation of the data loaded in db
    archive_data_key = 'transformed_data/archive/' + file_key.split('/')[-1]
    transformed_data_key = 'transformed_data/to_process/'+file_key.split('/')[-1]

    file_buffer = StringIO()
    data_df.to_csv(file_buffer, index=False, sep='\t')
    file_content = file_buffer.getvalue()
    s3_client.put_object(Bucket = s3_bucket, Key = archive_data_key, Body = file_content)

    s3_client.delete_object(Bucket=s3_bucket, Key=transformed_data_key)    

def db_load():
    s3_client = boto3.client('s3')
    s3_bucket = 'properties-etl'
    source_key = 'transformed_data/to_process/'

    file_key = get_s3_file_key(s3_client, s3_bucket, source_key)
    if not file_key:
        print("No file found")
        return
    data_df = get_file_data(s3_client, s3_bucket, file_key)
    conn = get_db_connection()
    send_data_to_db(data_df, conn)
    conn.close()
    archive_data(s3_client, s3_bucket, file_key, data_df)
