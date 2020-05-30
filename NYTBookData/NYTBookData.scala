import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, element_at}
object NYTBookDataAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .master("local")
      .appName("NYTBookData")
      .enableHiveSupport()
      .getOrCreate()

    print("NYT Book Analysis")

//    val schema = new StructType()
//      .add( new StructType("_id", StringType, true))
//      .add("amazon_product_url", StringType, true)
//      .add("author", StringType,true)
//      .add("bestsellers_date",LongType,true)
//      .add("description", StringType, true)
//      .add("published_date", DateType, true )
//      .add("publisher", StringType, true)
//      .add("rank", IntegerType, true)
//      .add("rank_last_week", StringType, true)
//      .add("title", StringType, true)
//      .add("weeks_on_list", IntegerType, true)

    val df = spark.read.json("nyt2.json").toDF()
    df.take(5).foreach(println)
    //df.withColumn("_id", element_at(col("_id"),0))
    println(df.select(df("_id")).show())


  }
}
