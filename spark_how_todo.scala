// Create spark session
val spark = SparkSession.builder().master("local[*]")
  .config("spark.executor.memory", "2G")
  .config("spark.driver.memory", "4G")
  .config("spark.warehouse.dir", "/data/spark/fl-data")
  .appName("Spark-Cheatsheet")
  .enableHiveSupport()
  .getOrCreate()

  // Read CSV file data
  val autoDf = spark.read.format("csv")
    .option("header", true)
    .load("/data/auto-mpg.csv")

// Save a DataFrame into a Hive catalog table
autoDf.write.mode("overwrite").saveAsTable("autompg")

// Load a Hive catalog table into a DataFrame
val hiveDf = spark.table("autompg")
hiveDf.show()

// Fixed automobile dataset
var auto_fixed_df = spark.emptyDataFrame
// add new columns and cast old columns to nee type
for (columnName <- "mpg cylinders displacement horsepower weight acceleration".split(" ").toList) {
  auto_fixed_df = autoDf
    .withColumn(columnName, col(columnName).cast(DoubleType))
    .withColumn("modelyear", col("modelyear").cast(IntegerType))
    .withColumn("origin", col("origin").cast(IntegerType))
}
auto_fixed_df.show()

// load tsv file
val tsvDf = spark.read.format("csv")
  .option("header", "true")
  .option("sep", "\t")
  .load("/data/auto-mpg.tsv")
tsvDf.show()

// Load a DataFrame from JSON Lines (jsonl) Formatted Data
// JSON Lines / jsonl format uses one JSON document per line.
val jsonNlDf = spark.read.json("/data/fl-data/weblog.jsonl")
jsonNlDf.show()

// Provide schema when load a dataframe from csv
val csvSchema = StructType(Array(
  StructField("mpg", DoubleType, nullable = true),
  StructField("cylinders", IntegerType, nullable = true),
  StructField("displacement", DoubleType, nullable = true),
  StructField("horsepower", DoubleType, nullable = true),
  StructField("weight", DoubleType, nullable = true),
  StructField("acceleration", DoubleType, nullable = true),
  StructField("modelyear", IntegerType, nullable = true),
  StructField("origin", IntegerType, nullable = true),
  StructField("carname", StringType, nullable = true)
))

val csvDf = spark.read.format("csv")
  .option("header", "true")
  .schema(csvSchema)
  .load("/home/pankaj/Downloads/fl-data/auto-mpg.csv")
csvDf.show()

// save a dataframe to csv, overwriting existing data
// note that this will create a directory named output.csv not a file
// actual data file name will be like part-0000part-00000-dcde62c9-ea55-40e6-81b0-097529d3d3be-c000.csv
csvDf.write.mode("overwrite").csv("output.csv")

// save dataframe in a single csv file, along with headers
csvDf.coalesce(numPartitions = 1)
  .write.option("header", "true").csv("output.csv")

// save DataFrame as a dynamic partitioned table
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
csvDf.write.mode("append")
    .partitionBy("modelyear")
    .saveAsTable("autompg_partitioned")

// load a CSV file with a money column into a DataFrame
val customerDf = spark.read.format("csv")
  .option("header", "true")
  .load("/home/pankaj/Downloads/fl-data/customer_spend.csv")

val moneyUdf = udf((x: String) => x
  .replace("$","")
  .replace(".",""))

spark.udf.register("moneyUdf", moneyUdf)

val dollarsDf = customerDf
  .withColumn("spend_dollars", moneyUdf(col("spend_dollars")))
dollarsDf.show()

// Add a new column
val df = autoDf
  .withColumn("upper", upper(col("carname")))
  .withColumn("lower", lower(col("carname")))

// Modifying a dataframe
val modDf = autoDf
  .withColumn("modelyear", concat(lit("19"), col("modelyear")))

// Add a column with multiple conditions
val multiModDf = autoDf.withColumn(
  "mpg_class",
  when(col("mpg") <= 20, "low")
    .when(col("mpg") <= 30, "mid")
    .when(col("mpg") <= 40, "high")
    .otherwise("very high")
)

// Add a constant column
val addColDf = autoDf.withColumn("one", lit(1))

// Concatenate columns
val concatDf = autoDf
  .withColumn("concatenated", concat(col("cylinders"), lit("_"), col("mpg")))

// Drop a column
val dropColDf = autoDf.drop("horsepower")

// Change a column name
val colRenameDf = autoDf.withColumnRenamed("horsepower", "bhp")

// Convert a DataFrame column to Scala list
import spark.implicits._
val carNames: List[String] = autoDf.select("carname").map(row => row.getString(0)).collect().toList
carNames.take(10).foreach(println)

// Create an empty dataframe with a specified schema
case class Person(id: Int, name: String)
val personDs: Dataset[Person] = spark.emptyDataset[Person]
personDs.printSchema()
// using implicit encoder
import spark.implicits._
val colSeq = Seq("emp_no", "emp_name", "designation")
val emptyDf = Seq.empty[(Integer, String, String)].toDF(colSeq: _*)
emptyDf.printSchema()

// Create constant data frame
import java.sql.Timestamp
import spark.implicits._
val dataSeq = Seq(
  (1, "abc", Timestamp.valueOf("2014-01-01 23:00:01")),
  (1, "def", Timestamp.valueOf("2014-11-30 12:40:32")),
  (2, "ghi", Timestamp.valueOf("2016-12-29 09:54:00"))
)
val constDF = spark.createDataFrame(dataSeq)
constDF.printSchema()

// Get data column names and data type
autoDf.dtypes

// Extract data from a string using a regular expression
autoDf
  .withColumn("identifier", regexp_extract(col("carname"), """(\S?\d+)""", 0))


// Unpack a DataFrame's JSON column to a new DataFrame
import spark.implicits._
val jsonDf = Seq(
    (1, """{ "a" : 10, "b" : 11 }"""),
    (2, """{ "a" : 20, "b" : 21 }""")
  ).toDF("id", "json_data")

val jsonSchema = spark.read.json(jsonDf.select("json_data").as[String]).schema
val df = jsonDf
  .withColumn("json_data", from_json(col("json_data"), jsonSchema))
df
  .select(col("json_data.a"))
  .show(5)

// Filter a column using a condition
autoDf
  .filter(col("mpg") > "30")
  .show(5)

// Filter based on a specific column value
autoDf
  .where(col("cylinders") === "8")
  .show(5)

// Filter based on an IN list
autoDf
  .where(col("cylinders").isin("4","6"))
  .show(5)

// Filter based on a NOT IN list
autoDf
  .where(!col("cylinders").isin("4","6"))
  .show(5)

// Filter values based on keys in another DataFrame
val excludeKeysDf = autoDf
  .select((col("modelyear") + 1).alias("adjusted_year"))
  .distinct()
// The anti join returns only keys with no matches
val joinCondition = autoDf.col("modelyear") === excludeKeysDf.col("adjusted_year")
autoDf
  .join(excludeKeysDf, joinCondition, "left_anti")
  .show(5)


// --------------------------------------------------
scala> df1.show
+----+----------+
|name|      dept|
+----+----------+
|   a|     sales|
|   b| marketing|
|   c|production|
|   d|     sales|
+----+----------+


scala> df2.show
+----+-----+
|name|grade|
+----+-----+
|   a|   10|
|   b|   20|
|   c|   10|
+----+-----+


// left_semi: show columns from left table for matching records only
scala> df1.join(df2, df1.col("name") === df2.col("name"), "left_semi").show
+----+----------+
|name|      dept|
+----+----------+
|   c|production|
|   b| marketing|
|   a|     sales|
+----+----------+


// left_anti: show columns from right table for non-matching records only
scala> df1.join(df2, df1.col("name") === df2.col("name"), "left_anti").show
+----+-----+
|name| dept|
+----+-----+
|   d|sales|
+----+-----+
// --------------------------------------------------

// Get Dataframe rows that match a substring
autoDf
  .where(col("carname").contains("custom"))
  .show(5,false)

// Filter a Dataframe based on a custom substring search
autoDf
   .where(col("carname").like("%custom%"))
   .show(5,false)

// Filter based on a column's length
autoDf
  .where(length(col("carname")) < 12)
  .show(5, false)

// Multiple filter conditions
autoDf
  .filter((col("mpg") > 30) || (col("acceleration") < 10))
  .show(5,false)

autoDf.orderBy("carname")
autoDf.orderBy(col("carname").desc)

// Distinct values
autoDf.select("cylinders").distinct()

// Remove duplicates
autoDf.dropDuplicates("carname")

///////////////////////////////////////////////
// Group DataFrame data by key to perform
// aggregates like counting, sums, averages, etc.
///////////////////////////////////////////////

// No sorting
val df = autoDf.groupBy("cylinders").count()

// With sorting
val df2 = autoDf.groupBy("cylinders").count().orderBy(desc("count"))

// Group and sort
val df3 =   autoDf
  .groupBy("cylinders")
  .agg(avg("horsepower").alias("avg_horsepower"))
  .orderBy(desc("avg_horsepower"))
df3.show()

// Filter groups based on an aggregate value, equivalent to SQL HAVING clause
val df4 =  autoDf
  .groupBy("cylinders")
  .count()
  .orderBy(desc("count"))
  .filter(col("count") > 100)
df4.show()

// Group by multiple columns
val df5 =   autoDf
  .groupBy("modelyear", "cylinders")
  .agg(avg("horsepower").alias("avg_horsepower"))
  .orderBy(desc("avg_horsepower"))
df5.show()

// Aggregate multiple columns
val aggMap = Map("horsepower" -> "avg", "weight" -> "max", "displacement" -> "max")
val df6 = autoDf.groupBy("modelyear").agg(aggMap)
df6.show()

// Aggregate multiple columns with custom orderings
val orderBySeq = Seq(
  desc_nulls_last("max(displacement)"),
  desc_nulls_last("avg(horsepower)"),
  asc("max(weight)")
)
val df7 = autoDf.groupBy("modelyear").agg(aggMap).orderBy(orderBySeq:_*)
df7.show(false)

// Get the maximum of a column
autoDf.select(max(col("horsepower")).alias("max_horsepower"))

// Apply sum function on a list of columns
val sumExpr = Map("weight" -> "sum", "cylinders" -> "sum", "mpg" -> "sum")
autoDf.agg(sumExpr).show()

// Sum a column
autoDf
  .groupBy("cylinders")
  .agg(sum("weight").alias("total_weight"))

// Count unique after grouping
autoDf.groupBy("cylinders").agg(countDistinct("mpg"))

// Group by then filter on the count
autoDf.groupBy("cylinders").count().where(col("count") > 100)

// Find the top N per row group (use N=1 for maximum)
val windowSpec = Window
  .partitionBy("cylinders")
  .orderBy(col("horsepower").desc)

val topN = 5
autoDf
  .withColumn("horsepower", col("horsepower").cast("double"))
  .withColumn("rn", row_number().over(windowSpec))
  .where(col("rn") <= topN)
  .select("*")

// Group key/values into a list
autoDf
  .groupBy("cylinders")
  .agg(
    collect_list(col("carname")).alias("models")
  ).show()

// Compute global percentiles
val windowSpec2 = Window.orderBy(col("mpg").desc)
val df9 = autoDf.withColumn("ntile4", ntile(4).over(windowSpec2))
df9.show()

// Compute percentiles within a partition
val windowSpec3 = Window.partitionBy("cylinders").orderBy(col("mpg").desc)
val df10 = autoDf.withColumn("ntile4", ntile(4).over(windowSpec3))
df10.show()

// Filter rows with values above a target percentile
val targetPercentile: Double = autoDf.agg(
  expr("percentile(mpg, 0.9)").alias("target_percentile")
).first().getDouble(0)
println(targetPercentile)

val df11 = autoDf.filter(col("mpg") > lit(targetPercentile))
df11.show()

// Aggregate and rollup
val subset = autoDf.filter(col("modelyear") > 79)
val df12 = subset.rollup("modelyear", "cylinders")
  .agg(
    avg("horsepower").alias("avg_horsepower"),
    count("modelyear").alias("count")
  )
  .orderBy(desc("modelyear"), desc("cylinders"))
df12.show()


///////////////////////////////////
// Joining and stacking DataFrames
///////////////////////////////////
val countriesDf = spark.read.format("csv")
  .option("header", "true")
  .load("/home/pankaj/Code/bigdata_notes/data/manufacturers.csv")

val firstWordUdf = udf((s: String) => {
  s.split("\\s+")(0)
})
spark.udf.register("firstWordUdf", firstWordUdf)

val autoDf = spark.read.format("csv")
  .option("header", true)
  .load("/home/pankaj/Code/bigdata_notes/data/auto-mpg.csv")

val autoManufacturerDf = autoDf
  .withColumn("manufacturer", firstWordUdf(col("carname")))

val joinCondition = countriesDf.col("manufacturer") === autoManufacturerDf.col("manufacturer")
val joinedDf = autoManufacturerDf.join(countriesDf, joinCondition, "inner")
joinedDf.show()

// Multiple join conditions
val multiJoinCondition = (countriesDf.col("manufacturer") === autoManufacturerDf.col("manufacturer")) ||
  autoManufacturerDf.col("mpg") === countriesDf.col("manufacturer")

val multiConditionJoinDf = autoManufacturerDf.join(countriesDf, multiJoinCondition, "inner")
multiConditionJoinDf.show()

// Different join types
// Inner join on one column.
 val innerJoinedDf = autoDf.join(autoDf, "carname")

// Left (outer) join
// Left anti (not in) join.
// Right (outer) join.
// Full join.
// Cross join.

// Concatenate two DataFrames
val df1 = spark.read.format("csv")
  .option("header", "true")
  .load("/home/pankaj/Code/bigdata_notes/data/part1.csv")

val df2 = spark.read.format("csv")
  .option("header", "true")
  .load("/home/pankaj/Code/bigdata_notes/data/part2.csv")
df1.union(df2)

// Load multiple files into a single DataFrame
val files = Seq("/home/pankaj/Code/bigdata_notes/data/part1.csv",
  "/home/pankaj/Code/bigdata_notes/data/part2.csv")
val df3 = spark.read.format("csv")
  .option("header", "true")
  .load(files:_*)

// Dealing with NULLs and NaNs in DataFrames
autoDf.where(col("horsepower) is null"))
autoDf.where(col("horsepower) is not null"))

// Drop rows with Null values
autoDf.na.drop(2, Seq("horsepower"))

// Count all Null or NaN values in a DataFrame
autoDf.where("horsepower is null").count

///////////////////////////////////////////
// Parsing and processing dates and times.
///////////////////////////////////////////

// Convert an ISO 8601 formatted date string to date type
import spark.implicits._
val df = spark.sparkContext.parallelize(
  Seq(
    "2021-01-01",
    "2022-01-01"
  )
).toDF("date_col")
df.printSchema()

val df1 = df
  .withColumn("date_col", col("date_col").cast(DateType))
df1.show()
df1.printSchema()

// Convert a custom formatted date string to date type
val df2 = spark.sparkContext.parallelize(
  Seq(
    "20210101",
    "20220101"
  )
).toDF("date_col")

val df3 = df2
  .withColumn("date_col", to_date(col("date_col"), "yyyyMMdd"))
df3.show()
df3.printSchema()

// Get the last day of the current month
val df4 = spark.sparkContext.parallelize(
  Seq(
    "2020-01-15",
    "1712-02-10"
  )
).toDF("date_col")

val df5 = df4
  .withColumn("date_col", col("date_col").cast(DateType))
  .withColumn("last_day", last_day(col("date_col")))

df5.show()
df5.printSchema()

// Convert UNIX (seconds since epoch) timestamp to date
val df6 = spark.sparkContext.parallelize(
  Seq(
    "1590183026",
    "2000000000"
  )
).toDF("ts_col")

val df7 = df6.withColumn("date_col", from_unixtime(col("ts_col")))
df7.show()
df7.printSchema() // date_col will be String type

/////////////////////////////////////////////////
/ Analyzing unstructured data like JSON, XML, etc
/////////////////////////////////////////////////
val baseDf = spark.read.json("/home/pankaj/Code/bigdata_notes/data/financial.jsonl")
baseDf.printSchema()

val targetJsonFields = Seq(
 col("symbol").alias("symbol"),
 col("quoteType.longName").alias("longName"),
 col("price.marketCap.raw").alias("marketCap"),
 col("summaryDetail.previousClose.raw").alias("previousClose"),
 col("summaryDetail.fiftyTwoWeekHigh.raw").alias("fiftyTwoWeekHigh"),
 col("summaryDetail.fiftyTwoWeekLow.raw").alias("fiftyTwoWeekLow"),
 col("summaryDetail.trailingPE.raw").alias("trailingPE")

baseDf
  .select(targetJsonFields:_*)
  .show()

// Flatten top level text fields from a JSON column
// ------------------------------------------------
val df8 = spark.read.format("csv")
  .option("header", "true")
  .option("quote", "\"")
  .option("escape", "\"")
  .load("/home/pankaj/Code/bigdata_notes/data/financial.csv")

// Infer schema of column containing JSON data
val jsonSchema = spark.read.json(df8.select("financial_data").as[String]).schema
val df9 = df8
  .withColumn("parsed", from_json(col("financial_data"), jsonSchema))

val jsonColumnNames = Seq(
  col("parsed.symbol").alias("symbol"),
  col("parsed.quoteType.longName").alias("longName"),
  col("parsed.price.marketCap.raw").alias("marketCap"),
  col("parsed.summaryDetail.previousClose.raw").alias("previousClose"),
  col("parsed.summaryDetail.fiftyTwoWeekHigh.raw").alias("fiftyTwoWeekHigh"),
  col("parsed.summaryDetail.fiftyTwoWeekLow.raw").alias("fiftyTwoWeekLow"),
  col("parsed.summaryDetail.trailingPE.raw").alias("trailingPE")
)
df9.select(jsonColumnNames:_*).show()

// Un-nest an array of complex structures
// --------------------------------------
val df10 = spark.read.json("/home/pankaj/Code/bigdata_notes/data/financial.jsonl")

// Analyze balance sheet data, which is held in an array of complex types.
val targetJsonColumns = Seq(
  col("symbol").alias("symbol"),
  col("balanceSheetHistoryQuarterly.balanceSheetStatements").alias(
    "balanceSheetStatements"
  )
)

val selectedDf = df10.select(targetJsonColumns:_*)

val selectedJsonColumns = Seq (
  col("symbol").alias("symbol"),
  col("col.endDate.fmt").alias("endDate"),
  col("col.cash.raw").alias("cash"),
  col("col.totalAssets.raw").alias("totalAssets"),
  col("col.totalLiab.raw").alias("totalLiab")
)

val df11 = selectedDf
  .select(col("symbol"), explode(col("balanceSheetStatements")))
  .select(selectedJsonColumns:_*)
df11.show()
