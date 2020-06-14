import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, when, element_at}
object NYTBookDataAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .master("local")
      .appName("NYTBookData")
      .enableHiveSupport()
      .getOrCreate()

    print("NYT Book Analysis")

    val df = spark.read.json("nyt2.json").toDF()

    // clean the dataset so that the json translates to a data frame format
    val cleaned = df.withColumn("_id",col("_id.$oid"))
      .withColumn("bestsellers_date",col("bestsellers_date.$date.$numberLong"))
      .withColumn("price", when(col("price.$numberDouble").isNull, col("price.$numberInt")).otherwise(col("price.$numberDouble")).cast("Double"))
      .withColumn("published_date", col("published_date.$date.$numberLong"))
      .withColumn("rank", col("rank.$numberInt"))
      .withColumn("rank_last_week", col("rank_last_week.$numberInt"))
      .withColumn("weeks_on_list", col("weeks_on_list.$numberInt"))

    // take a look at the schema
    println(cleaned.printSchema)

    // view a portion of the dataset
    cleaned.take(5).foreach(println)

    // which weeks had the top prices?
    println(cleaned.groupBy("weeks_on_list")
      .avg("price")
      .orderBy(col("avg(price)").desc)
      .show(false))

    // which books are ranked top ten?
    cleaned.select(col("title"), col("author"), col("rank"))
      .orderBy(col("rank").asc)
      .take(10).foreach(println)
    
    // count the number of free books that are within this data. what is the ratio between free
    // and non free books?


  }
}
