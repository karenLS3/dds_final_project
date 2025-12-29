import json
from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
import os



####################################################################
# DELETE GCS PATH
####################################################################
def delete_gcs_path(spark, gcs_path):
    try:
        uri = spark._jvm.java.net.URI(gcs_path)
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(uri.getPath())

        if fs.exists(path):
            fs.delete(path, True)
            print(f" Deleted: {gcs_path}")
        else:
            print(f" Not found (OK): {gcs_path}")

    except Exception as e:
        print(f" Error deleting {gcs_path}: {e}")


####################################################################
# SAVE RESULTS AS JSON TO GCS
####################################################################
def save_json_to_gcs(spark, obj, gcs_path):
    delete_gcs_path(spark, gcs_path)

    json_str = json.dumps(json.loads(json.dumps(obj, default=str)), indent=2)

    spark.sparkContext.parallelize([json_str], 1).saveAsTextFile(gcs_path)

    print(f" JSON saved to: {gcs_path}")


####################################################################
# PROBLEM 1
####################################################################
def co_occurring_airline_pairs_by_origin(flights_data: RDD) -> RDD:
    parsed_data = flights_data.map(lambda line: line.split(','))
    date_origin_airline = parsed_data.map(lambda x: ((x[0], x[2]), x[1]))
    grouped_airlines = date_origin_airline.groupByKey().mapValues(list)

    def generate_pairs(airlines):
        u = sorted(set(airlines))
        return [((u[i], u[j]), 1) for i in range(len(u)) for j in range(i + 1, len(u))]

    return (
        grouped_airlines.flatMap(lambda x: generate_pairs(x[1]))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: (-x[1], x[0][0], x[0][1]))  # descending by count
    )


####################################################################
# PROBLEM 2
####################################################################
def air_flights_most_canceled_flights(flights: DataFrame) -> str:
    from pyspark.sql.functions import col, count, year, month

    df = (
        flights.filter(
            (year(col("FlightDate")) == 2021)
            & (month(col("FlightDate")) == 1)
            & (col("Cancelled") == True)
        )
        .groupBy("Airline")
        .agg(count("*").alias("cancelled_count"))
        .orderBy(col("cancelled_count").desc())
        .first()
    )
    return df["Airline"] if df else ""


def air_flights_diverted_flights(flights: DataFrame) -> int:
    from pyspark.sql.functions import col, year, month, dayofmonth

    return flights.filter(
        (year(col("FlightDate")) == 2021)
        & (month(col("FlightDate")) == 11)
        & (dayofmonth(col("FlightDate")) >= 1)
        & (dayofmonth(col("FlightDate")) <= 30)
        & (col("Diverted") == True)
    ).count()


def air_flights_avg_airtime(flights: DataFrame) -> float:
    from pyspark.sql.functions import col, avg

    df = (
        flights.filter(
            (col("OriginCityName") == "Los Angeles, CA")
            & (col("DestCityName") == "New York, NY")
            & col("AirTime").isNotNull()
        )
        .agg(avg("AirTime").alias("avg_airtime"))
        .first()
    )
    return float(df["avg_airtime"]) if df["avg_airtime"] else 0.0


def air_flights_missing_departure_time(flights: DataFrame) -> int:
    from pyspark.sql.functions import col, countDistinct

    df = (
        flights.filter(col("DepTime").isNull())
        .agg(countDistinct("FlightDate").alias("unique"))
        .first()
    )
    return int(df["unique"]) if df["unique"] else 0


def air_flights_most_canceled_flights_november(flights: DataFrame) -> str:
    from pyspark.sql.functions import col, count, year, month

    df = (
        flights.filter(
            (year(col("FlightDate")) == 2021)
            & (month(col("FlightDate")) == 11)
            & (col("Cancelled") == True)
        )
        .groupBy("Airline")
        .agg(count("*").alias("cancelled_count"))
        .orderBy(col("cancelled_count").desc())
        .first()
    )
    return df["Airline"] if df else ""


####################################################################
# MAIN
####################################################################
def main():
    spark = SparkSession.builder.appName("Airlines-Analysis").getOrCreate()
    sc = spark.sparkContext

    # Load environment variables
    GCS_BUCKET = os.getenv("BUCKET_NAME")
    GCS_BASE_PATH = os.getenv("GCS_BASE_PATH", "Project_dataproc")

    def gcs(*parts):
        prefix = "/".join([p.strip("/") for p in parts if p is not None])
        return f"gs://{GCS_BUCKET}/{prefix}"

    airline_textfile = gcs(GCS_BASE_PATH, "flights_data.txt")
    airline_csvfile = gcs(GCS_BASE_PATH, "flights_data.csv")

    print("=================== PROBLEM 1 ===================")
    flights_data = sc.textFile(airline_textfile)
    result_problem1 = co_occurring_airline_pairs_by_origin(flights_data).collect()

    # ========= TAKE ONLY TOP 10 =========
    top10 = result_problem1[:10]

    # ========= FORMAT JSON EXACTLY AS REQUIRED =========
    formatted_p1 = {
        str(i + 1): {
            "flight_1": top10[i][0][0],
            "flight_2": top10[i][0][1],
            "common_origin": top10[i][1],
        }
        for i in range(10)
    }

    save_json_to_gcs(spark, formatted_p1, gcs(GCS_BASE_PATH, "problem1"))

    print("=================== PROBLEM 2 ===================")
    flights = spark.read.csv(airline_csvfile, header=True, inferSchema=True)

    q1 = f"{air_flights_most_canceled_flights(flights)} had the most canceled flights in January 2021."
    q2 = f"{air_flights_diverted_flights(flights)} flights were diverted between the period of 1st-30th November 2021."
    q3 = f"{air_flights_avg_airtime(flights)} is the average airtime for flights that were flying from Los Angeles to New York."
    q4 = f"{air_flights_missing_departure_time(flights)} unique dates where departure time (DepTime) was not recorded."
    q5 = f"{air_flights_most_canceled_flights_november(flights)} had the most canceled flights in November 2021."

    result_problem2 = [
        {"q1": q1},
        {"q2": q2},
        {"q3": q3},
        {"q4": q4},
        {"q5": q5},
    ]

    save_json_to_gcs(spark, result_problem2, gcs(GCS_BASE_PATH, "problem2"))

    spark.stop()


if __name__ == "__main__":
    main()