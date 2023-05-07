from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def run_analysis():

    stats_schema = StructType([
        StructField("numRecords", IntegerType()),
        StructField("maxValues", StructType([
            StructField("c1", StringType()),
            StructField("c2", StringType()),
            StructField("c4", StringType())
        ])),
        StructField("minValues", StructType([
            StructField("c1", StringType()),
            StructField("c2", StringType()),
            StructField("c4", StringType())
        ])),
        StructField("nullCount", StringType())
    ])
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet('sales/file_metrics/*').withColumn('stats', from_json(col("stats"), stats_schema))\
              .select(col("path").alias("file"), col("partitionValues.p").alias("partition"),\
                      "size", "stats.numRecords", col("stats.minValues.c1").alias("min_c1"),\
                      col("stats.maxValues.c1").alias("max_c1"), col("stats.minValues.c2").alias("min_c2"),\
                      col("stats.maxValues.c2").alias("max_c2"), col("stats.minValues.c4").alias("min_c4"),\
                      col("stats.maxValues.c4").alias("max_c4"))
    print("\nRecords per partition:")
    df.groupBy("partition").sum("numRecords").show()
    print("Mean records per file per partition:")
    df.groupBy().mean("numRecords").show()
    print("Partition size:")
    df.groupby("partition").sum("size").show()
    print("Mean file size per partition")
    df.groupBy("partition").mean("size").show()
    print("Number of overlapping files per partition:")
    aux_df = df[['file', 'partition', 'min_c1', 'max_c1', 'min_c2', 'max_c2', 'min_c4', 'max_c4']]
    overlapping_cond = ((col("df1.max_c1") >= col("df2.min_c1")) & (col("df2.max_c1") >= col("df1.min_c1")))\
                       |(col("df1.max_c2") >= col("df2.min_c2")) & (col("df2.max_c2") >= col("df1.min_c2"))\
                       |(col("df1.max_c4") >= col("df2.min_c4")) & (col("df2.max_c4") >= col("df1.min_c4"))
    aux_df.alias("df1").join(aux_df.alias("df2"), [col("df1.partition") == col("df2.partition"),\
                                                   col("df1.file") != col("df2.file")])\
          .filter(overlapping_cond).groupby("df1.partition").agg(countDistinct("df2.file")).show()
    print("Number of overlapping partitions:")
    aux_df.alias("df1").join(aux_df.alias("df2"), col("df1.partition") != col("df2.partition"))\
          .filter(overlapping_cond).select(countDistinct("df1.partition")).show()

    return


if __name__ == '__main__':
    run_analysis()
