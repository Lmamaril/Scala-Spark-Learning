import org.apache.spark.sql.SparkSession

object Analysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .master("local")
      .appName("HousingAnalysis")
      .enableHiveSupport()
      .getOrCreate()

    print("Housing Analysis")
    val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",", "header"-> "true")).csv("home_data.csv")
    df.printSchema()

    println("How many houses were built before 1979?")

    val homesPre1979 = df.filter(df("yr_built") < 1979)
    println("Count of houses build before 1979: ", homesPre1979.count())

    println("What is the most expensive zipcode in the dataset?")

    val priciest_zipcode = df.groupBy("zipcode").mean("price").orderBy(org.apache.spark.sql.functions.col("avg(price)").desc).take(1)
    priciest_zipcode.foreach(println)
  }
}

