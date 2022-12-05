import pandas as pd
import numpy as np

# Currency datalarını kolayca çekebilmek için kullanılan API'ler
import nasdaqdatalink
import investpy
from pandas_datareader import data as pdr

# Scraping için kullanılan modüller.
import requests
import json

# Scraping, data düzenleme vb konular için ihtiyacımız olan modüller.
from datetime import datetime
import datetime as dt
import time

# Datalari database'e yüklemek için kullanacagimiz kütüphane.
import sqlalchemy as db

#api_key="rvBysw1xnVJHM-8-2hb4" #my special api key Oguzhan Akkoyunlu

btcdf = nasdaqdatalink.get('BCHAIN/MKPRU', collapse='daily',api_key = "rvBysw1xnVJHM-8-2hb4")
btcdf = btcdf.loc['2019':]
btcdf = btcdf.rename(columns={'Value': 'BTCUSDT'})
# btcdf.to_csv('data/btcdata.csv')

bigmacindex = nasdaqdatalink.get('ODA/TUR_PCPI', collapse='monthly')
bigmacindex = bigmacindex.loc['2018':'2022']
bigmacindex = bigmacindex.rename(columns={'Value': 'bigmac_index'})
# bigmacindex.to_csv('data/bigmac.csv')

url = 'https://www.tcmb.gov.tr/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Istatistikler/Enflasyon+Verileri/Uretici+Fiyatlari'
tufedata = pd.read_html(url)

tufedata = tufedata[0].set_index('Unnamed: 0')
tufedata = tufedata.drop('Yıllık Değişim.1', axis=1)
tufedata.index = pd.to_datetime(tufedata.index)

url = 'https://www.tcmb.gov.tr/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Istatistikler/Enflasyon+Verileri/Uretici+Fiyatlari'

ufedata = pd.read_html(url, header=1)
ufedata = ufedata[0].set_index('Unnamed: 0')
ufedata.index = pd.to_datetime(ufedata.index)
ufedata = ufedata.iloc[:, 3:4]
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



url = 'https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/timeseries/stringency_index_avg.csv'

stringencydf = pd.read_csv(url, index_col=0, parse_dates=[0])

stringencydf = stringencydf[stringencydf.country_name == 'Turkey']
stringencydf = stringencydf.T.iloc[5:, :]
stringencydf.index = pd.to_datetime(stringencydf.index)
stringencydf = stringencydf.rename(columns={172: 'Stringency Index'})


# stringencydf.to_csv('data/stringency_index.csv')



dogalgaz_data = pd.read_excel(
    'https://data.ibb.gov.tr/dataset/02bdc2d6-94bb-4e31-816e-528bc9d98703/resource/d5fe41b0-3848-4548-9ac7-6e4756c3027b/download/ilce-baznda-yllara-gore-doalgaz-tuketim-miktar-tr-en.xlsx')

dogalgaz_data['Tarih'] = pd.to_datetime(dogalgaz_data['Yıl'].astype('str') + '-' + dogalgaz_data['Ay_No'].astype('str'))
dogalgaz_data = dogalgaz_data.drop(['Yıl', 'Ay_No'], axis=1).set_index('Tarih')

# dogalgaz_data.to_csv('data/dogalgaz.csv')

"""
df = pd.DataFrame()
enddate = datetime.today()
startdate = 0
for j in range(1, 2):
    citydf1 = pd.DataFrame()
    startdate = enddate - dt.timedelta(days=7)
    startst = startdate.strftime('%Y-%m-%d')
    endst = enddate.strftime('%Y-%m-%d')
    for city in ['istanbul', 'izmir', 'ankara']:
        citydf = pd.DataFrame()
        url = f'https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={part}&appid={"735203eac3d456db9e712ddd752cb680"}'
        r = requests.get(url, verify=False)
        jres = json.loads(r.text)

try:
    for i in range(len(jres['data']['weather'])):
        my_dict = {
        f'{city}_date': jres['data']['weather'][i]['date'],
        f'{city}_maxtempC': jres['data']['weather'][i]['maxtempC'],
        f'{city}_mintempC': jres['data']['weather'][i]['mintempC'],
        f'{city}_avgtempC': jres['data']['weather'][i]['avgtempC'],
        }
    #citydf = pd.concat([citydf,pd.DataFrame(my_dict,index=[i])
    citydf = citydf.append(pd.DataFrame(my_dict, index=[i]))
except:
    print("error")

citydf1 = pd.concat([citydf1, citydf], axis=1)
df = df.append(citydf1)

enddate = enddate - pd.DateOffset(months=1)

df = df.drop_duplicates()
df = df.drop(['izmir_date', 'ankara_date'], axis=1).rename(columns={'istanbul_date': 'date'}).set_index('date')
df.index = pd.to_datetime(df.index)
df = df.sort_index()
# df.to_csv('data/weather.csv')
"""

today = dt.datetime.today().strftime("%d/%m/%Y")

usdtrydf = pdr.get_data_yahoo("USDTRY=X", start="2019-01-01", end=f"{today}")
usdtrydf = usdtrydf[["Close"]].rename(columns={"Close": "USD_Close"})

eurtrydf = pdr.get_data_yahoo("EURTRY=X", start="2019-01-01", end=f"{today}")
eurtrydf = eurtrydf[["Close"]].rename(columns={"Close": "EUR_Close"})

#usdtrydf.to_csv("data/usdtry.csv")
#eurtrydf.to_csv("data/eurtrydf.csv")


alldf = pd.merge_asof(pd.merge_asof(pd.merge_asof(pd.merge_asof(pd.merge_asof(pd.merge_asof(pd.merge_asof(pd.merge_asof(
    pd.merge_asof(btcdf, tufedata.sort_index(), left_index=True, right_index=True, allow_exact_matches=False),
    bigmacindex.sort_index(), left_index=True, right_index=True, allow_exact_matches=False),
                                                                                                          ufedata.sort_index(),
                                                                                                          left_index=True,
                                                                                                          right_index=True,
                                                                                                          allow_exact_matches=False),
                                                                                            tgedf.sort_index(),
                                                                                            left_index=True,
                                                                                            right_index=True,
                                                                                            allow_exact_matches=False),
                                                                              kfedata.sort_index(), left_index=True,
                                                                              right_index=True,
                                                                              allow_exact_matches=False),
                                                                stringencydf.sort_index(), left_index=True,
                                                                right_index=True, allow_exact_matches=False),
                                                  df.sort_index(), left_index=True, right_index=True,
                                                  allow_exact_matches=False),
                                    usdtrydf.sort_index(), left_index=True, right_index=True,
                                    allow_exact_matches=False),
                      eurtrydf.sort_index(), left_index=True, right_index=True, allow_exact_matches=False)

print("Success")


# def insert_db(data:pd.DataFrame, table_name:str, schema_name:str, if_exists:str='append'):
#     username = 'hakan_ergun'
#     password = 'grmDASAnalytics2022'
#     host = 'grmtr-das-pgs.cryxxorv0vci.eu-west-1.rds.amazonaws.com'
#     port = '5457'

#     engine = db.create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/scraping')
#     data.to_sql(table_name, engine, schema=schema_name,if_exists=if_exists,index=False)
