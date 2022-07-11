import pandas as pd
import requests
import csv
import pandas
import json

url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"

stock_names = ["AAN", "AAON", "AAT", "AAWW", "ABCB"]

count = 0
for stock_name in stock_names:
    querystring = {"ticker_symbol": f"{stock_name}", "years": "5", "format": "json"}

    headers = {
        "X-RapidAPI-Key": "9cfca117aamsh3414264bc0e7e99p153e0ejsn9d1bd0ce9bc5",
        "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    data = response.text
    df = pd.DataFrame(json.loads(data)["historical prices"])
    df.to_csv(f"{stock_name}.csv")
# csvwriter = csv.writer(csvfile)
# stock_data = json.loads(response.text)["historical prices"]
# sno = 0
# for stock in stock_data:
# 	for k,v in stock.items():
# 		csvwriter.writerow([sno, k, v])
# 	sno += 1
# count += 1
