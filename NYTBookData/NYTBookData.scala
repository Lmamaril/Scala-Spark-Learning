import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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

    /**
     * root
     * |-- _id: string (nullable = true)
     * |-- amazon_product_url: string (nullable = true)
     * |-- author: string (nullable = true)
     * |-- bestsellers_date: string (nullable = true)
     * |-- description: string (nullable = true)
     * |-- price: double (nullable = true)
     * |-- published_date: string (nullable = true)
     * |-- publisher: string (nullable = true)
     * |-- rank: string (nullable = true)
     * |-- rank_last_week: string (nullable = true)
     * |-- title: string (nullable = true)
     * |-- weeks_on_list: string (nullable = true)
     */

    // view a portion of the dataset
    cleaned.take(5).foreach(println)

    /**
     * [5b4aa4ead3089013507db18b,http://www.amazon.com/Odd-Hours-Dean-Koontz/dp/0553807056?tag=NYTBS-20,Dean R Koontz,1211587200000,Odd Thomas, who can communicate with the dead, confronts evil forces in a California coastal town.,27.0,1212883200000,Bantam,1,0,ODD HOURS,1]
     * [5b4aa4ead3089013507db18c,http://www.amazon.com/The-Host-Novel-Stephenie-Meyer/dp/0316218502?tag=NYTBS-20,Stephenie Meyer,1211587200000,Aliens have taken control of the minds and bodies of most humans, but one woman won’t surrender.,25.99,1212883200000,Little, Brown,2,1,THE HOST,3]
     * [5b4aa4ead3089013507db18d,http://www.amazon.com/Love-Youre-With-Emily-Giffin/dp/0312348665?tag=NYTBS-20,Emily Giffin,1211587200000,A woman's happy marriage is shaken when she encounters an old boyfriend.,24.95,1212883200000,St. Martin's,3,2,LOVE THE ONE YOU'RE WITH,2]
     * [5b4aa4ead3089013507db18e,http://www.amazon.com/The-Front-Garano-Patricia-Cornwell-ebook/dp/B0017T0C9M?tag=NYTBS-20,Patricia Cornwell,1211587200000,A Massachusetts state investigator and his team from "At Risk" confront a rogue association of municipal police departments.,22.95,1212883200000,Putnam,4,0,THE FRONT,1]
     * [5b4aa4ead3089013507db18f,http://www.amazon.com/Snuff-Chuck-Palahniuk/dp/0385517882?tag=NYTBS-20,Chuck Palahniuk,1211587200000,An aging porn queens aims to cap her career by having sex on film with 600 men in one day.,24.95,1212883200000,Doubleday,5,0,SNUFF,1]
     */

    // which weeks had the top prices?
    println(cleaned.groupBy("weeks_on_list")
      .avg("price")
      .orderBy(col("avg(price)").desc)
      .show(false))

    /**
     * +-------------+------------------+
     * |weeks_on_list|avg(price)        |
     * +-------------+------------------+
     * |8            |14.493349514563088|
     * |3            |13.995338645418324|
     * |7            |13.898007246376784|
     * |44           |13.889            |
     * |43           |13.889            |
     * |46           |13.889            |
     * |45           |13.889            |
     * |4            |13.87154681139752 |
     * |2            |13.85736329588022 |
     * |6            |13.774209183673424|
     * |5            |13.65634831460669 |
     * |1            |13.181643700787589|
     * |28           |12.72578947368421 |
     * |29           |12.72578947368421 |
     * |38           |12.68             |
     * |39           |12.68             |
     * |59           |12.654444444444444|
     * |42           |12.626363636363635|
     * |67           |12.557142857142859|
     * |63           |12.557142857142859|
     * +-------------+------------------+
     */

    // name 10 of the books that were ranked top ten at any given time?
    cleaned.select(col("title"), col("author"), col("rank"))
      .orderBy(col("rank").asc)
      .take(10).foreach(println)

    /**
     *
     * [FEARLESS FOURTEEN,Janet Evanovich,1]
     * [DEVIL BONES,Kathy Reichs,1]
     * [THE LAST PATRIOT,Brad Thor,1]
     * [NOTHING TO LOSE,Lee Child,1]
     * [TRIBUTE,Nora Roberts,1]
     * [SAIL,James Patterson and Howard Roughan,1]
     * [TRIBUTE,Nora Roberts,1]
     * [THE SECRET SERVANT,Daniel Silva,1]
     * [THE FORCE UNLEASHED,Sean Williams,1]
     * [ACHERON,Sherrilyn Kenyon,1]
     *
     */

    // count the number of free books that are within this data. what is the ratio between free
    // and non free books?
    println(cleaned
      .withColumn("isFree", when(col("price") === 0,true)
      .otherwise(false))
      .groupBy("isFree")
      .agg(count("*"))
      .show(false))
    /**
     * +------+--------+
     * |isFree|count(1)|
     * +------+--------+
     * |true  |6184    |
     * |false |4011    |
     * +------+--------+
     *
     */
  }
}
