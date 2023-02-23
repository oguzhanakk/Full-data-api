import requests
import datetime
import pandas as pd
import csv, os, json
from dotenv import load_dotenv
print('Packages are imported')

load_dotenv()

API_KEY_2 = os.environ.get("API_KEY")
# API key
api_key = API_KEY_2

# City codes for Istanbul, Izmir and Ankara
city_codes = ["İstanbul", "Ankara", "İzmir"]

# Start and end dates
start_date = datetime.datetime.strptime("2023-01-09", "%Y-%m-%d").date()
end_date = datetime.datetime.strptime("2023-02-03", "%Y-%m-%d").date()

data_list = []
            
for city_code in city_codes:
    url = f"http://api.worldweatheronline.com/premium/v1/past-weather.ashx?q={city_code}&date={start_date}&enddate={end_date}&tp=24&format=json&key={api_key}"
    response = requests.get(url)

    data = response.json()
    
    for weather_data in data['data']['weather']:
        date = weather_data['date']
        max_temp = weather_data['maxtempC']
        min_temp = weather_data['mintempC']
        avg_temp = weather_data['avgtempC']
        
        data_list.append([date, city_code, max_temp, min_temp, avg_temp])

df = pd.DataFrame(data_list, columns=["Tarih", "Şehir", "Max Sıcaklık", "Min Sıcaklık", "Ortalama Sıcaklık"])
df.to_excel('weather_kalan.xlsx')
