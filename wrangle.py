# imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

def calls_311_wrangle():
    """ Function is ment to prep 311 call data """
    # start spark session
    spark = SparkSession.builder.getOrCreate()
    # create a scheme
    schema = StructType([
                        StructField('source_id', StringType()),
                        StructField('source_username', StringType()),
                        ])
    # read in the csv file
    source = spark.read.csv('source.csv', header=True, schema=schema)
    dept = spark.read.csv('dept.csv', header=True)
    case = spark.read.csv('case.csv', header=True)
    # Rename the columns
    case = case.withColumnRenamed('SLA_due_date', 'case_due_date')
    # ser column to boolean
    case = case.withColumn('case_closed', expr('case_closed == "YES"')).withColumn('case_late', expr('case_late == "YES"'))
    # conver counsil_distric to string
    case = case.withColumn('council_district', col('council_district').cast('string'))
    # convert datetime
    fmt = 'M/d/yy H:mm'
    # convert case_closed_date and case_open_date to datetime
    case = case.withColumn('case_closed_date', to_timestamp('case_closed_date', fmt))\
                           .withColumn('case_opened_date', to_timestamp('case_opened_date', fmt))\
                           .withColumn('case_due_date', to_timestamp('case_due_date', fmt))
                           
    # lowercase
    case = case.withColumn('request_address', trim(lower(case.request_address)))
    # create new column
    case = case.withColumn('num_week_late', expr('num_days_late / 7'))
    # create new column for zipcode
    case = case.withColumn('zipcode', regexp_extract(case.request_address, r"(\d+$)", 1))
    # create new columns case_age, days_to_closed, and case_lifetime
    case = (
            case.withColumn(
                            "case_age", datediff(current_timestamp(), "case_opened_date")
                            )
                .withColumn(
                            "days_to_closed", datediff('case_closed_date', "case_opened_date")
                            )
                .withColumn(
                            "case_lifetime", 
                            when(expr("! case_closed"), col("case_age"))
                            .otherwise(col("days_to_closed")),
                            )
            )
    # join the data
    df = (case
        .join(dept, on='dept_division', how='left')
        .drop(dept.dept_division)
        .drop(dept.dept_name)
        .drop(case.dept_division)
        .withColumnRenamed('standardized_dept_name', 'department')
        .withColumn('dept_subject_to_SLA', col('dept_subject_to_SLA') == 'YES')
    )
    return df
