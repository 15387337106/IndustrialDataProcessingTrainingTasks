import org.apache.spark.sql.SparkSession

object FeatureEngineering {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("featureEngineering").enableHiveSupport().getOrCreate()
    spark.table("dwd.fact_machine_data").show()

  }
}
