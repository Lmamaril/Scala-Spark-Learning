import org.apache.spark.sql.SparkSession

object Analysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .master("local")
      .appName("HousingAnalysis")
      .enableHiveSupport()
      .getOrCreate()

    case class homeSchema (id: String,
                         date: String,
                         price: Int, bedrooms: String,
                         bathrooms: String,
                         sqft_living: Int,
                         sqft_lot: Int,
                         floors: String,
                         waterfront: Boolean,
                         view: Boolean,
                         condition: Int,
                         grade: Int,
                         sqft_above: Int,
                         sqft_basement: Int,
                         yr_built: Int,
                         yr_renovated: Int,
                         zipcode: String,
                         latitude: Double,
                         longitude: Double,
                         sqft_living15: Int,
                         sqft_lot15: Int)
    

    print("Housing Analysis")
    val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",", "header"-> "true")).csv("home_data.csv")
    df.printSchema()

    print("How many houses were built before 1979?")
    print("What is the most expensive zipcode in the dataset?")

  }
}
