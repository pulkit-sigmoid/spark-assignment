
from pyspark.sql import SparkSession
from flask import Flask, request
from pyspark.sql.functions import lit

spark = SparkSession.builder.master("local")

stock_names = ["AAN", "AAON", "AAT", "AAWW", "ABCB"]

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
    query1 = {}
    for stock_name in stock_names:
        df = spark.read.option("header", "true").csv(f"{stock_name}.csv", inferSchema=True)  # first way
        df = df.withColumn("Stock_names", lit(stock_name))
    for i in range(df.count()):
        Date = str(df.collect()[i][7])
        Close = df.collect()[i][4]
        Open = df.collect()[i][1]
        percentage = (Close - Open)/Open * 100
        if Date not in query1:
            query1[Date] = {"Stocks":
                                {"Positive_Percentage_Stock": stock_name,
                                 "Negative_Percentage_Stock": stock_name},
                            "Percentage_Value":
                                {"Positive_Percentage_Value": percentage,
                                 "Negative_Percentage_Value": percentage}
                            }
        else:
            if query1[Date]["Percentage_Value"]["Positive_Percentage_Value"] < percentage:
                print("Positive", query1[Date]["Percentage_Value"]["Positive_Percentage_Value"], percentage)
                query1[Date]["Stocks"]["Positive_Percentage_Stock"] = stock_name
            if query1[Date]["Percentage_Value"]["Negative_Percentage_Value"] > percentage:
                print("Positive", query1[Date]["Percentage_Value"]["Negative_Percentage_Value"], percentage)
                query1[Date]["Stocks"]["Negative_Percentage_Stock"] = stock_name
    return query1


if __name__ == '__main__':
    app.run(debug=True, port=20010)