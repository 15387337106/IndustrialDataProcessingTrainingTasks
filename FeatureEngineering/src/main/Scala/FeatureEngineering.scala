import org.apache.spark.sql.SparkSession
import org.dom4j.{DocumentHelper, Element}
import java.sql.Timestamp
import scala.jdk.CollectionConverters.asScalaBufferConverter


case class MachineData(machine_record_id: Int, machine_id: Double, machine_record_state: Double, machine_record_mainshaft_speed: Double,
                       machine_record_mainshaft_multiplerate: Double, machine_record_mainshaft_load: Double, machine_record_feed_speed: Double,
                       machine_record_feed_multiplerate: Double, machine_record_pmc_code: String, machine_record_circle_time: Double, machine_record_run_time: Double,
                       machine_record_effective_shaft: Double, machine_record_amount_process: Double, machine_record_use_memory: Double,
                       machine_record_free_memory: Double, machine_record_amount_use_code: Double, machine_record_amount_free_code: Double,
                       machine_record_date: Timestamp, dwd_insert_user: String, dwd_insert_time: Timestamp, dwd_modify_user: String, dwd_modify_time: Timestamp
                      )
object FeatureEngineering {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test dom4j").enableHiveSupport().getOrCreate()
    import spark.implicits._
    spark.table("dwd.fact_machine_data_pre").map(row => {
      val machine_record_id = row.getAs[Int]("machinerecordid")
      val machine_id = row.getAs[Int]("machineid").toDouble
      val machine_record_state = if (row.getAs[String]("machinerecordstate").eq("报警")) 1L else 0.0
      val machine_record_data = row.getAs[String]("machine_record_data")
      val document = DocumentHelper.parseText(machine_record_data)
      val array = document.getRootElement.elements().asScala.map { case element: Element => element.getText }
        .collect({ case x: String => x }).toArray
      //        val list = Array(machine_record_id, machine_id, machine_record_state) ++ array
      //        println(list.mkString("---"))
      MachineData(machine_record_id, machine_id, machine_record_state
        , array(0).toDouble, array(1).toDouble, array(2).toDouble, array(3).toDouble, array(4).toDouble, array(5), array(6).toDouble, array(7).toDouble, array(8).toDouble,
        array(9).toDouble, array(10).toDouble, array(11).toDouble, array(12).toDouble, array(13).toDouble,
        row.getAs[Timestamp]("machinerecorddate"), row.getAs[String]("dwd_insert_user"),
        Timestamp.valueOf(row.getAs[String]("dwd_insert_time")), row.getAs[String]("dwd_modify_user"), Timestamp.valueOf(row.getAs[String]("dwd_modify_time")))
    }).write.mode("overwrite").saveAsTable("dwd.fact_machine_learning_data")
  }
}
