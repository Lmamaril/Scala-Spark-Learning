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

    val rdd = sc.textFile("home_data.csv")
    rdd.foreach(f=>{
      println(f)
    })

  }
}
