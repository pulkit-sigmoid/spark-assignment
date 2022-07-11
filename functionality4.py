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
    df_stocks.show()
    df_stocks.createOrReplaceTempView("data")
    spark.sql("create temporary view temp1 as (select Stock_names, min(Date) as Min_Date from data group by Stock_names)")
    spark.sql("create temporary view temp2 as (select temp1.Stock_names, temp1.Min_Date, data.Open from temp1 inner join "
              "data on temp1.Min_Date = data.Date)")
    spark.sql("create temporary view temp3 as (select temp2.Stock_names, max(data.High - temp2.Open) as Max_Moved from data inner join "
              "temp2 on data.Stock_names=temp2.Stock_names group by temp2.Stock_names)")
    result = spark.sql("select Stock_names, Max_Moved from temp3 where Max_Moved=(select max(Max_Moved) from temp3)")
    result.show()
    return jsonify(json.loads(result.toPandas().to_json()))
    # return {}

if __name__ == '__main__':
    app.run(debug=True, port=2004)