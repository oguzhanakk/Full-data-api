import datetime
from pandas_datareader import data as pdr

# Bugünün tarihini alın ve bir string olarak biçimlendirin
today = datetime.datetime.today().strftime("%m/%d/%Y")

# USD/TRY döviz kuru için geçmiş verileri çekin ve 'usdtrydf' adlı bir veri kümesine kaydedin
usdtrydf = pdr.get_data_yahoo("USDTRY=X", start="2019-01-01", end=f"{today}")
usdtrydf = usdtrydf[["Close"]].rename(columns={"Close": "USD_Close"})

# EUR/TRY döviz kuru için geçmiş verileri çekin ve 'eurtrydf' adlı bir veri kümesine kaydedin
eurtrydf = pdr.get_data_yahoo("EURTRY=X", start="2019-01-01", end=f"{today}")
eurtrydf = eurtrydf[["Close"]].rename(columns={"Close": "EUR_Close"})

# Veri kümelerini CSV dosyalarına kaydetmeyi burada ekleyebilirsiniz
usdtrydf.to_csv("usdtry.csv")
eurtrydf.to_csv("eurtrydf.csv")
