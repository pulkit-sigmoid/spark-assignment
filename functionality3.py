import json
from flask import jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

from flask import Flask

spark = SparkSession.builder.master("local")

stock_names = ["AAN", "AAON", "AAT", "AAWW", "ABCB", "ABG", "ABM", "ABTX", "ACA", "ACLS",
               "ADC", "ADTN", "ADUS", "AEIS", "AEL", "AGO", "AGYS", "AHH", "AIN", "AIR",
               "AIT", "AJRD", "AKR", "ALEX", "ALG", "ALGT", "ALRM", "AMBC", "AMCX", "AMEH",
               "AMN", "AMPH", "AMSF", "AMWD", "ANDE"]

spark = SparkSession.builder \
    .master("local") \
    .appName("Stock Analysis") \
    .getOrCreate()

app = Flask(__name__)


@app.route("/result", methods=["POST", "GET"])
def result():
    return fun()


def fun():
    df_stocks = None
    for stock_name in stock_names:
        df = spark.read.option("header", "true").csv(f"Stocks/{stock_name}.csv", inferSchema=True)  # first way
        df = df.withColumn("Stock_names", lit(stock_name))
        df = df.withColumn("Stock_moved_Percentage", (((col("Close") - col("Open")) / col("Open")) * 100))
        if df_stocks is None:
            df_stocks = df
        else:
            df_stocks = df_stocks.union(df)
    df_stocks.createOrReplaceTempView("data")
    spark.sql("create temporary view temp1 as (select Date, Stock_names, lag(Close, 1, 0) over (order by Date) as "
              "Previous_Date_Close_Price, Open as Current_Date_Open_Price from data)")
    spark.sql(
        "create temporary view temp2 as (select Stock_names, max(Previous_Date_Close_Price - Current_Date_Open_Price) as Max_Gap, "
        "min(Previous_Date_Close_Price - Current_Date_Open_Price) as Min_Gap from temp1 group by Stock_names)")
    result = spark.sql(
        "select Stock_names, (Previous_Date_Close_Price - Current_Date_Open_Price) as Max_Min_Gap from temp1 where "
        "(Previous_Date_Close_Price - Current_Date_Open_Price)=(select min(Min_Gap) from temp2) or (Previous_Date_Close_Price - Current_Date_Open_Price)=(select max(Max_Gap) from temp2)")
    result.show()
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


if __name__ == '__main__':
    app.run(debug=True, port=2003)
