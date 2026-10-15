"""Rebuild fixed panels from existing Spark aggregate outputs."""
from rainfall import ART,ROOT,spark_session,export
from pyspark.sql import functions as F
spark=spark_session()
try:
    annual=spark.read.option('header',True).option('inferSchema',True).csv(str(ART/'annual_station'))
    coverage=spark.read.option('header',True).option('inferSchema',True).csv(str(ART/'station_year_coverage'))
    fixed=annual.groupBy('station_id').count().filter('count = 8').select('station_id')
    strict=coverage.filter('coverage >= 0.95').groupBy('station_id').count().filter('count = 8').select('station_id')
    export(annual.join(fixed,'station_id'),'fixed_station_annual')
    export(annual.join(strict,'station_id'),'strict_fixed_station_annual')
    df=spark.read.parquet(str(ROOT/'data'/'clean'))
    export(df.groupBy('year').agg(F.sum(F.col('extreme_flag').cast('int')).alias('flagged_intervals'),
        F.max('rain').alias('maximum_five_minute_rain')),'extreme_flags_year')
finally:spark.stop()
