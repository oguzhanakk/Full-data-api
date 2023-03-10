import pandas as pd
import numpy as np

# Currency datalarını kolayca çekebilmek için kullanılan API'ler
import yfinance as yf
import yfinance as yfin
import nasdaqdatalink
from pandas_datareader import data as pdr
import http.client

# Scraping için kullanılan modüller.
import requests
import csv, os, json

# Scraping, data düzenleme vb konular için ihtiyacımız olan modüller.
from datetime import datetime
import datetime as dt
import time

# Datalari database'e yüklemek için kullanacagimiz kütüphane.
import psycopg2

import warnings
from dotenv import load_dotenv
print('Packages are imported')

load_dotenv()
HOST = os.environ.get("DB_HOST")
DATABASE = os.environ.get("DB_DATABASE")
USER = os.environ.get("DB_USER")
PASSWORD = os.environ.get("DB_PASSWORD")
PORT = os.environ.get("DB_PORT")
SCHEMA = os.environ.get("DB_SCHEMA")
TABLE = os.environ.get("DB_TABLE")
API_KEY = os.environ.get("API_KEY")

print('Environment variables are imported')

warnings.filterwarnings("ignore")

#api_key="rvBysw1xnVJHM-8-2hb4" #My special api key Oguzhan Akkoyunlu
#Bitcoin api
btcdf = nasdaqdatalink.get('BCHAIN/MKPRU', collapse='daily',api_key = API_KEY)
btcdf = btcdf.loc['2019':]
btcdf = btcdf.rename(columns={'Value': 'BTCUSDT'})
# btcdf.to_csv('data/btcdata.csv')

"""
#Big mac directory api but done wrong.
bigmacindex = nasdaqdatalink.get('ODA/TUR_PCPI', collapse='monthly')
bigmacindex = bigmacindex.loc['2018':'2022']
bigmacindex = bigmacindex.rename(columns={'Value': 'bigmac_index'})
# bigmacindex.to_csv('data/bigmac.csv')
"""

#Usd-try euro-try api data
yfin.pdr_override()
today = datetime.today().strftime("%Y-%m-%d")
usdtrydf = pdr.get_data_yahoo("USDTRY=X", start="2019-01-01", end=f"{today}")
usdtrydf = usdtrydf[["Open"]].rename(columns={"Open": "USD_Open"})

yfin.pdr_override()
today = datetime.today().strftime("%Y-%m-%d")
eurtrydf = pdr.get_data_yahoo("EURTRY=X", start="2019-01-01", end=f"{today}")
eurtrydf = eurtrydf[["Open"]].rename(columns={"Open": "EUR_Open"})
#usdtrydf.to_excel('usdttry.xlsx')
#eurtrydf.to_excel('eurtrydf.xlsx')

#Brent petrol api data
yfin.pdr_override()
today2 = datetime.today().strftime("%Y-%m-%d")
brent_petrol = pdr.get_data_yahoo("BZ=F", start="2019-01-01", end=f"{today2}")
brent_petrol = brent_petrol.iloc[:, 0:4]
brent_petrol = brent_petrol.rename(columns = {'High' : 'B_petrol_max', 'Low' : 'B_petrol_min', 'Open' : 'B_petrol_open', 'Close' : 'B_petrol_close'})
# brent_petrol.to_csv('data/brent_petrol.csv')

#Consumer prices inflation data API
url = 'https://www.tcmb.gov.tr/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Istatistikler/Enflasyon+Verileri/Tuketici+Fiyatlari'
tufedata = pd.read_html(url)

tufedata = tufedata[0].set_index('Unnamed: 0')
tufedata.index = pd.to_datetime(tufedata.index)
tufedata = tufedata.iloc[:,0:1]
# tufedata.to_csv('data/tufe.csv')

#Producer prices inflation data api
url = 'https://www.tcmb.gov.tr/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Istatistikler/Enflasyon+Verileri/Uretici+Fiyatlari'
ufedata = pd.read_html(url, header=1)
ufedata = ufedata[0].set_index('Unnamed: 0')
ufedata.index = pd.to_datetime(ufedata.index)
ufedata = ufedata.iloc[:, 1:2]
# ufedata.to_csv('data/ufe.csv')


url = 'https://evds2.tcmb.gov.tr/EVDSServlet'
headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'tr,en-US;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Content-Length': '335',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': 'JSESSIONID=AB47C151C7361FC25F807370A6DA979C; SessionCookie=!YJddNMVNvLJoHCLqkygjtJLA5IKF7VwJ7R4k0yS4t2PCcWRzFV7STFgxAIWWgh2K20iYJ9JW1MzK0m4=; TS013c5758=015d31d69106bc33991ec7ac1bdd27e333baccf5b0abac74fe56f0bd093dcf80bcc3b6ba27f4397b549056fa01b7330447f1c6b22ed24c7265b1e0ef0ba32aded3a36d34a149e61d691c41d16fbd0026de1a3ef173',
    'Host': 'evds2.tcmb.gov.tr',
    'Origin': 'https://evds2.tcmb.gov.tr',
    'Referer': 'https://evds2.tcmb.gov.tr/index.php?/evds/dashboard/4985',
    'sec-ch-ua': '" Not A;Brand;v="99", "Chromium";v="101", "Google Chrome";v="101"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': 'Windows',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
}
data = 'skip=0&take=12&select=TP_TG2_Y01-0&startDate=01-01-2010&obsCountEnabled=&obsCount=&endDate=30-05-2022&categories=5968&mongoAdresses=&database=mongo&graphic=true&thousand=1&decimal=2&frequency=MONTH&aggregationType=avg&formula=0&graphicType=0&userId=&numberFormat=0&datagroupString=bie_mbgven2&customFormula=%5B%5D&excludedSeries=%5B%5D'
r = requests.post(url, headers=headers, data=data,  verify=False)
jres = json.loads(r.text)
items = jres.get('items', '')
tgedf = pd.DataFrame(items)
tgedf['Tarih'] = tgedf['Tarih'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)
tgedf.Tarih = tgedf.Tarih.apply(lambda x: datetime.fromtimestamp(x))
tgedf['Tarih'] = pd.to_datetime(tgedf['Tarih'])
tgedf = tgedf.set_index('Tarih')
tgedf = tgedf[['TP_TG2_Y01']].rename(columns={'TP_TG2_Y01': 'Tuketici_Guven_Endeksi'})
# tgedf.to_csv('data/tge.csv')

url = 'https://evds2.tcmb.gov.tr/EVDSServlet'
headers = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'tr,en-US;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Content-Length': '405',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': 'JSESSIONID=AB47C151C7361FC25F807370A6DA979C; SessionCookie=!ym10hHZGJnGHKCTqkygjtJLA5IKF7fBnFceq3l+NY9f8RmzdaGzQtuzkQ6ZmZN7PzqpqYjd5h6s5+So=; TS013c5758=015d31d6917684480c52e16e4311db393944e7dbd820d72565b32df0b7c6571689e22770323c425712c8425d5b85bfd1e8652e8b4cf624f61c16d7701245568fc966b552c506e182077758793570ec3a8eea0a9b43',
    'Host': 'evds2.tcmb.gov.tr',
    'Origin': 'https://evds2.tcmb.gov.tr',
    'Referer': 'https://evds2.tcmb.gov.tr/index.php?/evds/portlet/jHQqZSuUssg%3D/tr',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': 'Windows',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
}

data1 = 'orderby=Tarih+desc&thousand=1&decimal=1&frequency=MONTH&aggregationType=last%23last%23last&formula=0%231%233&graphicType=0&skip=0&take=20&sort=Tarih%23true&select=TP_HKFE01-0%23TP_HKFE01-1%23TP_HKFE01-3&startDate=01-01-2010&endDate=01-03-2022&obsCountEnabled=false&obsCount=&categories=5949&mongoAdresses=evds&userId=&datagroupString=bie_hkfe&dateFormatValue=yyyy-ww&customFormula=null&excludedSeries=null'
data2 = 'orderby=Tarih+desc&thousand=1&decimal=1&frequency=MONTH&aggregationType=last%23last%23last&formula=0%231%233&graphicType=0&skip=20&take=20&sort=Tarih%23true&select=TP_HKFE01-0%23TP_HKFE01-1%23TP_HKFE01-3&startDate=01-01-2010&endDate=01-03-2022&obsCountEnabled=false&obsCount=&categories=5949&mongoAdresses=evds&userId=&datagroupString=bie_hkfe&dateFormatValue=yyyy-ww&customFormula=null&excludedSeries=null'

kfedf = pd.DataFrame()
for data in [data1, data2]:
    r = requests.post(url, headers=headers, data=data,  verify=False)

jres = json.loads(r.text)
items = jres.get('items', '')

kfedata = pd.DataFrame(items)
kfedata.Tarih = pd.to_datetime(kfedata.Tarih)
kfedata = kfedata.set_index('Tarih').drop('UNIXTIME', axis=1)

kfedata = kfedata[['TP_HKFE01']].rename(columns={'TP_HKFE01': 'Konut_Fiyat_Endeksi'})

kfedf = kfedf.append(kfedata)

# kfedf.to_csv('data/kfedata.csv')


#Not used
url = 'https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/timeseries/stringency_index_avg.csv'
stringencydf = pd.read_csv(url, index_col=0, parse_dates=[0])
stringencydf = stringencydf[stringencydf.country_name == 'Turkey']
stringencydf = stringencydf.T.iloc[5:, :]
stringencydf.index = pd.to_datetime(stringencydf.index)
stringencydf = stringencydf.rename(columns={172: 'Stringency Index'})
# stringencydf.to_csv('data/stringency_index.csv')


#Not used
dogalgaz_data = pd.read_excel(
    'https://data.ibb.gov.tr/dataset/02bdc2d6-94bb-4e31-816e-528bc9d98703/resource/d5fe41b0-3848-4548-9ac7-6e4756c3027b/download/ilce-baznda-yllara-gore-doalgaz-tuketim-miktar-tr-en.xlsx')
dogalgaz_data['Tarih'] = pd.to_datetime(dogalgaz_data['Yıl'].astype('str') + '-' + dogalgaz_data['Ay_No'].astype('str'))
dogalgaz_data = dogalgaz_data.drop(['Yıl', 'Ay_No'], axis=1).set_index('Tarih')
# dogalgaz_data.to_csv('data/dogalgaz.csv')

#Weather old data
weather_df = pd.read_excel("weather_data.xlsx")
'''
#Weather new data
conn = http.client.HTTPSConnection("api.collectapi.com")
headers = {
    'content-type': "application/json",
    'authorization': "apikey 5lAQ7VOTn1fRQUuXXCFb85:3VriN3VsQTKBfSDWsHVZhh"
    }
date = []
max = []
min = []
degree = []
for city in ['ankara', 'istanbul', 'izmir']:
    conn.request("GET", f"/weather/getWeather?data.lang=tr&data.city={city}", headers=headers)

    res = conn.getresponse()
    data = res.read()
    jres = json.loads(data)
    date = jres['result'][0]['date']
    max.append(jres['result'][0]['max'])
    min.append(jres['result'][0]['min'])
    degree.append(jres['result'][0]['degree'])

date1 = pd.to_datetime(date, format="%d.%m.%Y")
list = [date1,int(float(max[0])),int(float(min[0])),int(float(degree[0])),0,int(float(max[1])),int(float(min[1])),int(float(degree[1])),0,int(float(max[2])),int(float(min[2])),int(float(degree[2])),0]
weather_df = weather_df.append(pd.Series(list, index=weather_df.columns), ignore_index=True)
'''

#Combine all data in one dataframe
alldf = pd.merge_asof(pd.merge_asof(pd.merge_asof(pd.merge_asof(pd.merge_asof(pd.merge_asof(
    pd.merge_asof(pd.merge_asof(btcdf, ufedata.sort_index(), left_index=True, right_index=True, allow_exact_matches=False),
    tufedata.sort_index(), left_index=True, right_index=True, allow_exact_matches=False),
    brent_petrol.sort_index(), left_index=True, right_index=True, allow_exact_matches=False),
    tgedf.sort_index(),
    left_index=True,
    right_index=True,
    allow_exact_matches=False),
    kfedata.sort_index(), left_index=True,
    right_index=True,
    allow_exact_matches=False),
    weather_df, left_index=True, right_index=True,
    allow_exact_matches=False,by='Date'),
    usdtrydf.sort_index(), left_index=True, right_index=True,
    allow_exact_matches=False),
    eurtrydf.sort_index(), left_index=True, right_index=True, allow_exact_matches=False)

#A loop written to go through all the boxes in excel can be used if needed.
"""
a = 0
for j in range(0,len(alldf.columns)):
    for i in range(0,len(alldf[alldf.columns[j]])):
        if np.isnan(alldf[alldf.columns[j]][i]):
            alldf[alldf.columns[j]][i] = 5
"""

#To put missing value data
#petrol_start_variable
list = [55.8,56.1,55.53,55.72,51.83,51.92,53.90,54.12,54.38,54.60]
x=0
for i in range(3,8):
    for j in range(0,2):
        alldf[alldf.columns[i]][j] = list[x]
        x+=1
        
#usd-eur start variable
alldf[alldf.columns[18]][0] = 5.26
alldf[alldf.columns[19]][0] = 6.02

alldf = alldf.resample('m').mean()
#print(alldf)

#alldf.to_excel("Monthly_General_data.xlsx") #For everymonth

print("Success") #Success message (If this returned, the code worked.)

def postgre_insert(all_data):
        
        conn = None
        # Connect to the database
        print("before connection")
        conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT)
        print("after connection")
        print("connection is created")

        # Create a cursor object
        cur = conn.cursor()
        print("cursor is created")
        
        print('all_data:')
        print(all_data)

        # Create schema if not exists
        cur.execute(f'''CREATE SCHEMA IF NOT EXISTS {SCHEMA}''')
        
        # Create table
        cur.execute(f'''CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE}
                        (id SERIAL PRIMARY KEY, BTCUSDT numeric, YI_UFE numeric, TUFE_Yillik numeric, 
                        B_petrol_open numeric, B_petrol_max numeric, B_petrol_min numeric, B_petrol_close numeric,
                        Tuketici_Guven_Endeksi numeric, Konut_Fiyat_Endeksi numeric, B_wea_ank_max numeric, B_wea_ank_min numeric, 
                        B_wea_ank_mean numeric, B_wea_ist_max numeric, B_wea_ist_min numeric,B_wea_ist_mean numeric, 
                        B_wea_izm_max numeric, B_wea_izm_min numeric, B_wea_izm_mean numeric,USD_Open numeric, 
                        EUR_Open numeric,Date text)''')

        # Add trends to db
        for i, row in all_data.iterrows():
            cur.execute(f"INSERT INTO {SCHEMA}.{TABLE} (BTCUSDT, YI_UFE, TUFE_Yillik, B_petrol_open, B_petrol_max, B_petrol_min, B_petrol_close, Tuketici_Guven_Endeksi, Konut_Fiyat_Endeksi, B_wea_ank_max, B_wea_ank_min, B_wea_ank_mean, B_wea_ist_max, B_wea_ist_min, B_wea_ist_mean, B_wea_izm_max, B_wea_izm_min, B_wea_izm_mean, USD_Open, EUR_Open, Date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (row['BTCUSDT'], row['YI_UFE'], row['TUFE_Yillik'], row['B_petrol_open'], row['B_petrol_max'], row['B_petrol_min'], row['B_petrol_close'], row['Tuketici_Guven_Endeksi'], row['Konut_Fiyat_Endeksi'], row['B_wea_ank_max'], row['B_wea_ank_min'], row['B_wea_ank_mean'], row['B_wea_ist_max'], row['B_wea_ist_min'], row['B_wea_ist_mean'], row['B_wea_izm_max'], row['B_wea_izm_min'], row['B_wea_izm_mean'], row['USD_Open'], row['EUR_Open'], row['Date'])) 


        # Commit the changes to the database
        conn.commit()
        print("changes are commited")

        # Close the cursor and connection
        cur.close()
        conn.close()
        print("cursor and connection are closed")
    
postgre_insert(alldf)