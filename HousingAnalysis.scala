import org.apache.spark.{SparkConf, SparkContext}

object Analysis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local").
      setAppName("HousingAnalysis")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val description = "Housing Analysis"
    print(description)

    val nums = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
    nums.foreach(println)

    val rdd = sc.textFile("home_data.csv")
    val words = rdd.flatMap(line => line.split(","))
    val wordCount = words.map(word => (word,1)).reduceByKey(_ + _)
    wordCount.foreach(println)
    sc.stop()
    
//    // read the full file
//    rdd.foreach(f=>{
//      println(f)
//    })

    print("How many houses were built before 1979?")
    print("What is the most expensive zipcode in the dataset?")

  }
}
