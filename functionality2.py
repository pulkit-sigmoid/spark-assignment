import json
from flask import jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

from flask import Flask

spark = SparkSession.builder.master("local")

stock_names = ["AAN", "AAON", "AAT", "AAWW", "ABCB"]

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
        df = spark.read.option("header", "true").csv(f"{stock_name}.csv", inferSchema=True)  # first way
        df = df.withColumn("Stock_names", lit(stock_name))
        df = df.withColumn("Stock_moved_Percentage", (((col("Close") - col("Open")) / col("Open")) * 100))
        if df_stocks is None:
            df_stocks = df
        else:
            df_stocks = df_stocks.union(df)
    df_stocks.createOrReplaceTempView("data")

    spark.sql("create temporary view temp as (select Date, max(Volume) as Max_Volume from data group by Date)")
    result = spark.sql("select temp.Date, Max_Volume, Stock_names from temp inner join data on temp.Date==data.Date")

    result.show()
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


if __name__ == '__main__':
    app.run(debug=True, port=2002)
