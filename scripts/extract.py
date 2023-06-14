import requests
from bs4 import BeautifulSoup
import pandas as pd
from unidecode import unidecode
from io import StringIO
import time


def get_page_content(page_number): #this part takes url, defines session, page and the html/json  soup

    URL = f'https://www.bezrealitky.cz/vyhledat?page={page_number}'
    session = requests.Session()
    page = session.get(URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup

def extract_property_data(soup): 

    properties_list = []
    results = soup.find('div', class_='box mb-last-0')
    ads = results.find_all('article', class_='PropertyCard_propertyCard__qPQRK') # here i define the part of webpage that I need, with all ads

    for ad in ads: # loop through the card of ads for details for every ad
        purpose = ad.find(class_='PropertyCard_propertyCardLabel__lnHZu mb-2 text-caption text-grey-dark fw-medium text-uppercase text-truncate').text.strip()
        address = ad.find(class_='PropertyCard_propertyCardAddress__yzOdb text-subheadline text-truncate').text.strip()
        details = ad.find_all('li', class_='FeaturesList_featuresListItem__SugGi')
        size_m2 = None
        design = None
        for detail in details: # this is a tricky one, both size_m2 and design are in the same 'container', however both values are optional, so there has to be an for loop
            value = detail.text.strip()
            if 'm²' in value:
                size_m2 = value
            elif 'm²' not in value:
                design = value
        price_czk = ad.find(class_='PropertyPrice_propertyPriceAmount___dwT2').text.strip()
        link_element = ad.find('a')
        link = link_element.get('href')

        properties_list.append([purpose, address, size_m2, design, price_czk, link]) 
    return properties_list

def clean_data(properties_list):

    properties_df = pd.DataFrame(properties_list, columns=['purpose', 'address', 'size_m2', 'design', 'price_czk', 'link'])
    properties_df = properties_df.applymap(lambda x: unidecode(x) if isinstance(x, str) else x) # change czech-specific letters to international standard
    properties_df.reset_index(drop=True, inplace=True)
    return properties_df

def df_to_csv(properties_df): #create a StringIO object, write the dataframe to the object and pass it for being saved to S3

    file_buffer = StringIO() 
    properties_df.to_csv(file_buffer, index=False, sep='\t')
    file_content = file_buffer.getvalue()
    return file_content


def run_extract():
    
    all_properties = []
    for i in range (1,150): # there are about 1270 webpages of content on the website
        soup = get_page_content(i)
        properties_list = extract_property_data(soup)
        all_properties.extend(properties_list)
        time.sleep(2) # prevent time out

    properties_df = clean_data(all_properties)
    file_content = df_to_csv(properties_df)

    return file_content