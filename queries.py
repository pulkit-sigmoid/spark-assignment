# query1 can be improved by using rank .
# All other  qureies are written efficiently. 
# Try to reduce the number of views created to solve the queries.
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


df_stocks = None

# creating common dataframe for csv files


for stock_name in stock_names:
    df = spark.read.option("header", "true").csv(f"Stocks/{stock_name}.csv", inferSchema=True)  # first way       
    df = df.withColumn("Stock_names", lit(stock_name))
    if df_stocks is None:
        df_stocks = df
    else:
        df_stocks = df_stocks.union(df)
df_stocks.createOrReplaceTempView("data")


@app.route("/query1", methods=["POST", "GET"])
def query1():
    return fun1()


def fun1():
    spark.sql("create temporary view temp1 as (select Date, max((High - Open)/Open * 100) as Positive from data group by "
        "date)")
    spark.sql("create temporary view temp2 as (select Date, min((Low - Open)/Open * 100) as Negative from data group by "
              "date)")
    spark.sql("create temporary view temp3 as (select Date, stock_names as Positive_Stock_Names, (High - Open)/Open * 100 as Positive_Percentage from data where "
              "(High - Open)/Open * 100 in (select Positive from temp1 where data.Date == temp1.Date))")
    spark.sql("create temporary view temp4 as (select Date, stock_names as Negative_Stock_Names, (Low - Open)/Open * 100 as Negative_Percentage from data where "
              "(Low - Open)/Open * 100 in (select Negative from temp2 where data.Date == temp2.Date))")
    result = spark.sql("select temp3.Date, temp3.Positive_Stock_Names, temp3.Positive_Percentage, temp4.Negative_Stock_Names, "
              "temp4.Negative_Percentage from temp3 inner join temp4 on temp3.Date == temp4.Date order by temp3.Date")

    result.show()
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


@app.route("/query2", methods=["POST", "GET"])
def query2():
    return fun2()


def fun2():
    spark.sql("create temporary view temp2_1 as (select Date, max(Volume) as Max_Volume from data group by Date)")
    result = spark.sql("select temp2_1.Date, Max_Volume, Stock_names from temp2_1 inner join data on temp2_1.Date==data.Date")

    result.show()
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


@app.route("/query3", methods=["POST", "GET"])
def query3():
    return fun3()


def fun3():
    spark.sql("create temporary view temp3_1 as (select Date, Stock_names, lag(Close, 1, 0) over (order by Date) as "
              "Previous_Date_Close_Price, Open as Current_Date_Open_Price from data)")
    spark.sql(
        "create temporary view temp3_2 as (select Stock_names, max(Previous_Date_Close_Price - Current_Date_Open_Price) as Max_Gap, "
        "min(Previous_Date_Close_Price - Current_Date_Open_Price) as Min_Gap from temp3_1 group by Stock_names)")
    result = spark.sql(
        "select Stock_names, (Previous_Date_Close_Price - Current_Date_Open_Price) as Max_Min_Gap from temp3_1 where "
        "(Previous_Date_Close_Price - Current_Date_Open_Price)=(select min(Min_Gap) from temp3_2) or (Previous_Date_Close_Price - Current_Date_Open_Price)=(select max(Max_Gap) from temp3_2)")
    result.show()
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


@app.route("/query4", methods=["POST", "GET"])
def query4():
    return fun4()


def fun4():
    spark.sql("create temporary view temp4_1 as (select Stock_names, min(Date) as Min_Date from data group by Stock_names)")
    spark.sql("create temporary view temp4_2 as (select temp4_1.Stock_names, temp4_1.Min_Date, data.Open from temp4_1 inner join "
              "data on temp4_1.Min_Date = data.Date)")
    spark.sql("create temporary view temp4_3 as (select temp4_2.Stock_names, max(data.High - temp4_2.Open) as Max_Moved from data inner join "
              "temp4_2 on data.Stock_names=temp4_2.Stock_names group by temp4_2.Stock_names)")
    result = spark.sql("select Stock_names, Max_Moved from temp4_3 where Max_Moved=(select max(Max_Moved) from temp4_3)")
    result.show()
    return jsonify(json.loads(result.toPandas().to_json()))


@app.route("/query5", methods=["POST", "GET"])
def query5():
    return fun5()


def fun5():
    result = spark.sql("select Stock_names, std(Volume) as Standard_Deviation from data group by Stock_names")
    result.show()
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


@app.route("/query6", methods=["POST", "GET"])
def query6():
    return fun6()


def fun6():
    spark.sql("create temporary view temp5_1 as (select Stock_names, mean(Close) as Mean from data group by Stock_names)")
    spark.sql("create temporary view temp5_2 as (select Stock_names, percentile_approx(Close, 0.5) as Median from data group by Stock_names)")
    result = spark.sql("select temp5_1.Stock_names, Mean, Median from temp5_1 inner join temp5_2 on temp5_1.Stock_names == temp5_2.Stock_names")
    result.show()
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


@app.route("/query7", methods=["POST", "GET"])
def query7():
    return fun7()


def fun7():
    result = spark.sql("select Stock_names, avg(Volume) as Average_Volume from data group by Stock_names")
    result.show()
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


@app.route("/query8", methods=["POST", "GET"])
def query8():
    return fun8()


def fun8():
    spark.sql("create temporary view temp8_1 as (select Stock_names, avg(Volume) as Average_Volume from data group by Stock_names)")
    spark.sql("create temporary view temp8_2 as (select max(Average_Volume) as Average_Volume from temp8_1)")
    result = spark.sql("select Stock_names, temp8_1.Average_Volume from temp8_1 inner join temp8_2 on temp8_1.Average_Volume == temp8_2.Average_Volume")
    result.show()
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


@app.route("/query9", methods=["POST", "GET"])
def query9():
    return fun9()


def fun9():
    result = spark.sql("select Stock_names, max(High) as Highest_Price, min(Low) as Lowest_Price from data group by "
                       "Stock_names")
    result.show()
    return jsonify(json.loads(result.toPandas().to_json(orient="table", index=False)))


if __name__ == '__main__':
    app.run(debug=True, port=2000)
