import json
from flask import jsonify
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType
from flask import Flask, request

spark = SparkSession.builder.master("local")

stock_names = ["AAN", "AAON", "AAT", "AAWW", "ABCB", "ABG", "ABM", "ABTX", "ACA", "ACLS",
               "ADC", "ADTN", "ADUS", "AEIS", "AEL", "AGO", "AGYS", "AHH", "AIN", "AIR",
               "AIT", "AJRD", "AKR", "ALEX", "ALG", "ALGT", "ALRM", "AMBC", "AMCX", "AMEH",
               "AMN", "AMPH", "AMSF", "AMWD", "ANDE"]

spark = SparkSession.builder \
    .master("local") \
    .appName("Stock Analysis") \
    .getOrCreate()

# schema
# query1 = {"Date":
#               {"Stocks":
#                    {"Positive_Percentage_Stock": "",
#                     "Negative_Percentage_Stock": ""},
#                "Percentage_Value": {
#                    {"Positive_Percentage_Stock": "",
#                     "Negative_Percentage_Stock": ""}
#                }}}

app = Flask(__name__)


@app.route("/result", methods=["POST", "GET"])
def result():
    return fun()


def fun():
    df_stocks = None
    for stock_name in stock_names:
        df = spark.read.option("header", "true").csv(f"Stocks/{stock_name}.csv", inferSchema=True)  # first way
        df = df.withColumn("Stock_names", lit(stock_name))
        if df_stocks is None:
            df_stocks = df
        else:
            df_stocks = df_stocks.union(df)
    df_stocks.createOrReplaceTempView("data")
    spark.sql("create temporary view temp1 as (select Date, max((High - Open)/Open * 100) as Positive from data group by "
              "date)")
    spark.sql("create temporary view temp2 as (select Date, min((Open - Low)/Low * 100) as Negative from data group by "
              "date)")
    spark.sql("create temporary view temp3 as (select Date, stock_names as Positive_Stock_Names, Stock_moved_Percentage as Positive_Percentage from data where "
              "Stock_moved_Percentage in (select Positive from temp1 where data.Date == temp1.Date))")
    spark.sql("create temporary view temp4 as (select Date, stock_names as Negative_Stock_Names, Stock_moved_Percentage as Negative_Percentage from data where "
              "Stock_moved_Percentage in (select Negative from temp2 where data.Date == temp2.Date))")
    result = spark.sql("select temp3.Date, temp3.Positive_Stock_Names, temp3.Positive_Percentage, temp4.Negative_Stock_Names, "
              "temp4.Negative_Percentage from temp3 inner join temp4 on temp3.Date == temp4.Date order by temp3.Date")

    result.show()
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


if __name__ == '__main__':
    app.run(debug=True, port=2001)
