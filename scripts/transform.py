import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
from zoneinfo import ZoneInfo
import numpy as np

def get_s3_file_key(s3_client, s3_bucket, source_key): # we search for a file ending with .csv in specific location on S3
    response_list = s3_client.list_objects(Bucket=s3_bucket, Prefix=source_key)
    for object in response_list['Contents']:
        if object['Key'].endswith('.csv'):
            return object['Key']
    return None

def fetch_data(s3_client, s3_bucket, file_key): # access the file that was previously found and make a df of it, also making the archive df for archiving the original non-edited file
    response_get = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
    content = response_get['Body']
    properties_csv = pd.read_csv(content, sep='\t')
    properties_df_archive = pd.DataFrame(properties_csv, index=None)
    properties_df = pd.DataFrame(properties_csv, index=None)

    return properties_df, properties_df_archive

def clean_and_transform_data(properties_df, file_key):

    czech_regions = [
    'Praha',
    'Jihocesky kraj',
    'Jihomoravsky kraj',
    'Karlovarsky kraj',
    'Kralovehradecky kraj',
    'Liberecky kraj',
    'Moravskoslezsky kraj',
    'Olomoucky kraj',
    'Pardubicky kraj',
    'Plzensky kraj',
    'Stredocesky kraj',
    'Ustecky kraj',
    'Kraj Vysocina',
    'Zlinsky kraj'
    ]

    # keywords for cleaning wrong prices
    rent_wrong_price_keywords = [
    'Pronajem kancelare',
    'Pronajem nebytoveho prostoru',
    'Pronajem chaty, chalupy',
    'Pronajem domu',
    'Pronajem pozemku'
    ]
    # keywords for cleaning wrong prices
    sale_wrong_price_keywords = [
    'Prodej bytu',
    'Prodej domu',
    'Prodej nebytoveho prostoru',
    'Prodej pozemku',
    'Prodej chaty, chalupy',
    'Prodej garaze',
    'Prodej kancelare'
    ]

    properties_df = properties_df.drop_duplicates(subset=['link']) # drop duplicates based on the same link
    properties_df= properties_df.replace('\u00A0', ' ', regex=True) # replacing the no-break space

    # price_czk cleaning
    properties_df = properties_df[~properties_df['price_czk'].str.contains('EUR')] # delete EURO price, these are properties in Slovakia
    properties_df['price_czk'] = properties_df['price_czk'].str.replace('Kc', '') # delete Kc string
    properties_df['price_czk'] = properties_df['price_czk'].apply(lambda x: int(''.join(c for c in str(x) if c.isdigit()))) # column 'price_czk' to int
    rent_wrong_price_keywords = ['Pronajem kancelare' , 'Pronajem nebytoveho prostoru', 'Pronajem chaty, chalupy', 'Pronajem domu', 'Pronajem pozemku'] #k eywords for cleaning wrong prices
    sale_wrong_price_keywords = ['Prodej bytu', 'Prodej domu', 'Prodej nebytoveho prostoru', 'Prodej pozemku', 'Prodej chaty, chalupy', 'Prodej garaze', 'Prodej kancelare'] # keywords for cleaning wrong prices
    properties_df = properties_df.drop(properties_df[properties_df['price_czk'] < 500].index) # deleting wrong prices
    properties_df = properties_df.drop(properties_df[(properties_df['purpose'].str.contains('|'.join(rent_wrong_price_keywords))) & (properties_df['price_czk'] <= 1000)].index) # deleting wrong prices
    properties_df = properties_df.drop(properties_df[(properties_df['purpose'].str.contains('|'.join(sale_wrong_price_keywords))) & (properties_df['price_czk'] <= 20000)].index) # deleting wrong prices

    # size_m2 cleaning
    properties_df['size_m2'] = properties_df['size_m2'].str.replace('m2', '') # delete m2 string
    properties_df['size_m2'] = pd.to_numeric(properties_df['size_m2'], errors='coerce')
    properties_df['size_m2'] = properties_df['size_m2'].fillna(0).astype(int)
    #properties_df['size_m2'] = properties_df['size_m2'].astype(int) # column 'size_m2' to int

    def extract_last_two_words(string): 
        words = string.split()
        if len(words) >= 2:
            last_two_words = ' '.join(words[-2:])
            return last_two_words.rstrip(',')
        else:
            return ''

    # appply the transformation to create the region column and modify the address column
    properties_df['region'] = properties_df['address'].apply(lambda x: extract_last_two_words(x) if 'kraj' in x.lower() else 'Praha')
    properties_df['address'] = properties_df['address'].apply(lambda x: ' '.join(x.split()[:-2]).rstrip(',') if 'kraj' in x.lower() else x)
    properties_df = properties_df[properties_df['region'].isin(czech_regions)]


    # Calculate price_per_m2
    properties_df['price_per_m2'] = np.where(properties_df['size_m2'] != 0,
                                            properties_df['price_czk'] / properties_df['size_m2'],
                                            np.nan)

    properties_df['price_per_m2'] = properties_df['price_per_m2'].fillna(0)
    properties_df['price_per_m2'] = np.ceil(properties_df['price_per_m2']).astype(int)
    properties_df['price_per_m2'] = properties_df['price_per_m2'].replace(0 , None)
    properties_df = properties_df.drop(properties_df[(properties_df['purpose'].str.contains('Prodej pozemku')) & (properties_df['price_per_m2'] > 80000)].index) # deleting wrong prices


    # add dump_date column with UTC +2
    cet_timezone = ZoneInfo('Europe/Paris')
    current_timestamp = datetime.now(tz=cet_timezone)
    timestamp_format = "%Y_%m_%d_%H%M%S"  # example format: 2023_05_27_143015
    formatted_timestamp = current_timestamp.strftime(timestamp_format)
    properties_df['dump_date'] = formatted_timestamp
    properties_df['file_name'] =  file_key.split('/')[-1]

    # reindex the DataFrame with the new column order
    new_column_order = ['purpose', 'address', 'region', 'size_m2', 'design', 'price_czk', 'price_per_m2',  'link', 'dump_date', 'file_name']
    properties_df = properties_df.reindex(columns=new_column_order)



    return properties_df

def upload_to_s3(s3_client, s3_bucket, file_key, properties_df, properties_df_archive): # part where we upload files to S3
    raw_archive_key = 'raw_data/archive/processed_'+file_key.split('/')[-1]
    transformed_data_key = 'transformed_data/to_process/transformed_'+file_key.split('/')[-1].split('raw_')[1]
    raw_to_process_key = 'raw_data/to_process/'+file_key.split('/')[-1]

    # create a StringIO object, make it a csv and save to our transform folder, ready to be loaded to db
    file_buffer = StringIO()
    properties_df.to_csv(file_buffer, index=False, sep='\t')
    file_content = file_buffer.getvalue()
    s3_client.put_object(Bucket = s3_bucket, Key = transformed_data_key, Body = file_content)

    # create s StringIO object out of our original raw data file to save it in our archive folder for raw data
    old_file_buffer = StringIO()
    properties_df_archive.to_csv(old_file_buffer, index=False, sep='\t')
    old_file_content = old_file_buffer.getvalue()
    s3_client.put_object(Bucket = s3_bucket, Key = raw_archive_key, Body = old_file_content)

    s3_client.delete_object(Bucket = s3_bucket, Key = raw_to_process_key) # we delete the raw data file since we moved it to archive

def transformation():
    s3_client = boto3.client('s3')
    s3_bucket = 'properties-etl'
    source_key = 'raw_data/to_process/'

    file_key = get_s3_file_key(s3_client, s3_bucket, source_key)
    if file_key is None:
        print('No CSV file')
        return
    
    properties_df, properties_df_archive = fetch_data(s3_client, s3_bucket, file_key)
    properties_df = clean_and_transform_data(properties_df, file_key)

    upload_to_s3(s3_client, s3_bucket, file_key, properties_df, properties_df_archive)