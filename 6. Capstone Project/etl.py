import configparser
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

S3_BUCKET = config['S3']['S3_BUCKET']
S3_REGION = config['S3']['S3_REGION']


def create_spark_session():
    """
    Creates a Spark Session.
    """
    spark = SparkSession.builder \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport().getOrCreate()
    return spark


def process_airports(spark, input_path, output_path):
    """
    Load the airport code table then transform and save the table
    """
    logging.info("Starting processing airports data...")

    # Define the airport table schema
    schema = [
        StructField("airport_id", StringType()),
        StructField("type", StringType()),
        StructField("name", StringType()),
        StructField("elevation_ft", IntegerType()),
        StructField("continent", StringType()),
        StructField("iso_country", StringType()),
        StructField("iso_region", StringType()),
        StructField("municipality", StringType()),
        StructField("gps_code", StringType()),
        StructField("iata_code", StringType()),
        StructField("local_code", StringType()),
        StructField("coordinates", StringType())
    ]

    schema = StructType(schema)

    # Read airport data from file
    df = spark.read.csv(input_path, header=True, schema=schema).distinct()

    # Filter to select only airport in U.S.
    df = df.filter("iso_country == 'US'")

    # Split "coordinates" columns into atomic form
    df = df.withColumn("latitude", F.split(F.col("coordinates"), ",")[0].cast(DoubleType()))
    df = df.withColumn("longitude", F.split(F.col("coordinates"), ",")[1].cast(DoubleType()))
    df = df.drop("coordinates")

    # Deal with "US-" prefix in "iso_region" column
    df = df.withColumn("state_code", F.split(F.col("iso_region"), "-")[1].cast(DoubleType()))
    df = df.drop("iso_region")

    # Select interested columns from the table
    df = df.withColumnRenamed("ident", "airport_id")
    df = df.select("airport_id", "type", "name", "elevation_ft", "continent", "iso_country", "state_code",
                   "municipality", "gps_code", "latitude", "longitude")
    df = df.dropna(subset="airport_id")

    # Perform data quality check
    data_quality_check(df, primary_key="airport_id")

    # Write the table to parquet files
    df.coalesce(1).write.mode("overwrite").parquet(output_path)


def process_demographic(spark, input_path, output_path):
    """
    Load the U.S. city demographic data then transform and save the table
    """
    logging.info("Starting processing demographic data...")

    # Define the demographic table schema
    schema = [
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("median_age", DoubleType()),
        StructField("male_population", IntegerType()),
        StructField("female_population", IntegerType()),
        StructField("total_population", IntegerType()),
        StructField("number_of_veterans", IntegerType()),
        StructField("foreign_born", IntegerType()),
        StructField("avg_household_size", DoubleType()),
        StructField("state_code", StringType()),
        StructField("race", StringType()),
        StructField("count", IntegerType())
    ]

    # Read demographic data from file
    df = spark.read.csv(input_path, header=True, sep=";", schema=StructType(schema)).distinct()

    # Create primary id key column
    df = df.withColumn("demographic_id", F.monotonically_increasing_id())

    # Perform data quality check
    data_quality_check(df, primary_key="demographic_id")

    # Write demographic table to parquet files
    df.coalesce(1).write.mode("overwrite").partitionBy("state").parquet(output_path)


def process_label(spark, input_path, output_path):
    """
    Load the additional label data then transform and save the table
    """
    logging.info("Starting processing labels data...")

    with open(input_path, "r") as file:
        contents = file.readlines()

    country_codes = {}
    for line in contents[10:298]:
        pair = line.split("=")
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        country_codes[code] = country
    df_countries = spark.createDataFrame(country_codes.items(), ['country_code', 'country'])
    df_countries = df_countries.dropna(subset="country_code")
    data_quality_check(df_countries, primary_key="country_code")
    df_countries.coalesce(1).write.mode("overwrite").parquet(path=output_path + 'countries')

    city_codes = {}
    for line in contents[303:962]:
        pair = line.split("=")
        code, city = pair[0].strip("\t").strip().strip("'"), \
                     pair[1].strip('\t').strip().strip("''")
        city_codes[code] = city
    df_cities = spark.createDataFrame(city_codes.items(), ['city_code', 'city'])
    df_cities = df_cities.dropna(subset="city_code")
    data_quality_check(df_cities, primary_key="city_code")
    df_cities.coalesce(1).write.mode("overwrite").parquet(path=output_path + 'cities')

    state_codes = {}
    for line in contents[982:1036]:
        pair = line.split("=")
        code, state = pair[0].strip('\t').strip("'"), pair[1].strip().strip("'")
        state_codes[code] = state
    df_states = spark.createDataFrame(state_codes.items(), ['state_code', 'state'])
    df_states = df_states.dropna(subset="state_code")
    data_quality_check(df_states, primary_key="state_code")
    df_states.coalesce(1).write.mode("overwrite").parquet(path=output_path + 'states')


def process_immigration(spark, input_path, output_path):
    """
    Load the immigration data then transform and save the table
    """
    logging.info("Starting processing immigration data...")

    # Read immigration data from file
    df = spark.read.format('com.github.saurfang.sas.spark').load(input_path)

    # Create temp view of immigration data for performing SQL queries later
    df.createOrReplaceTempView("immigration")

    df = spark.sql("""
                        SELECT
                            i.cicid AS cic_id,
                            i.i94yr AS year,
                            i.i94mon AS month,
                            i.i94cit AS citizen_country,
                            i.i94res AS residence_country,
                            i.i94port AS city_code,
                            i.arrdate AS arrival_date,
                            i.i94mode AS mode,
                            i.i94addr AS state_code,
                            i.depdate AS departure_date,
                            i.i94bir AS age,
                            i.i94visa AS visa,
                            i.visapost AS visa_post,
                            i.occup AS occupation,
                            i.matflag AS match_flag,
                            i. biryear AS birth_year,
                            i.gender AS gender,
                            i.airline AS airline,
                            i.admnum AS admin_num,
                            i.fltno AS flight_num,
                            i.visatype AS visa_type

                        FROM immigration AS i
                    """)

    # Create primary id key column for this table
    df = df.withColumn("immigrant_id", F.monotonically_increasing_id())

    # Adjust data type for some columns
    df = df.withColumn("cic_id", F.col("cic_id").cast(IntegerType()))
    df = df.withColumn("citizen_country", F.col("citizen_country").cast(IntegerType()))
    df = df.withColumn("residence_country", F.col("residence_country").cast(IntegerType()))

    # Perform data quality check
    data_quality_check(df, primary_key="immigrant_id")

    # Write immigration table to parquet files
    df.coalesce(1).write.mode("overwrite").partitionBy("year", "month").parquet(output_path)


def data_quality_check(df, primary_key=None):
    """
    Perform data quality check on the PySpark DataFrame
    """
    # 1. Check if number of records are greater than zero
    record_num = df.count()
    if record_num <= 0:
        raise ValueError("This table is empty!")

    # 2. Check if primary key in the table are unique
    if df.count() > df.dropDuplicates([primary_key]).count():
        raise ValueError('Data has duplicates')

    return True


def main():
    """
    Main Function
    """
    s3_bucket = config.get('S3', 'S3_BUCKET')

    spark = create_spark_session()

    logging.info("Starting ETL processing...")
    process_label(spark, 'I94_SAS_Labels_Descriptions.SAS', s3_bucket)
    process_airports(spark, 'airport-codes_csv.csv', s3_bucket + "airports")
    process_demographic(spark, 'us-cities-demographics.csv', s3_bucket + "demographic")
    process_immigration(spark, '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat', s3_bucket + "immigration")
    logging.info("ETL processing completed")


if __name__ == "__main__":
    main()
