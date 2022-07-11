import pandas as pd
import requests
import json

url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"

stock_names = ["AAN", "AAON", "AAT", "AAWW", "ABCB", "ABG", "ABM", "ABTX", "ACA", "ACLS",
               "ADC", "ADTN", "ADUS", "AEIS", "AEL", "AGO", "AGYS", "AHH", "AIN", "AIR",
               "AIT", "AJRD", "AKR", "ALEX", "ALG", "ALGT", "ALRM", "AMBC", "AMCX", "AMEH",
               "AMN", "AMPH", "AMSF", "AMWD", "ANDE"]

for stock_name in stock_names:
    querystring = {"ticker_symbol": f"{stock_name}", "years": "5", "format": "json"}

    headers = {
        "X-RapidAPI-Key": "9cfca117aamsh3414264bc0e7e99p153e0ejsn9d1bd0ce9bc5",
        "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    data = response.text
    df = pd.DataFrame(json.loads(data)["historical prices"])
    df.to_csv(f"Stocks/{stock_name}.csv")