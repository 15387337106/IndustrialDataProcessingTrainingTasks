import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.dom4j.io.SAXReader

import scala.util.Random
import scala.xml.XML

object generateMLData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("generate ml data").enableHiveSupport().getOrCreate()

    def randomMLDataGeneratorUDF(): String = {
      val machine_record_mainshaft_speed = Random.nextInt(3000) + 500
      val machine_record_mainshaft_multiplerate = Random.nextDouble() * 2.0
      val machine_record_mainshaft_load = Random.nextInt(100)
      val machine_record_feed_speed = Random.nextDouble() * 2.0
      val machine_record_feed_multiplerate = Random.nextInt(1000) + 100
      val machine_record_pmc_code = "P" + Random.nextInt(1000)
      val machine_record_circle_time = Random.nextInt(300) + 60
      val machine_record_run_time = Random.nextInt(36000) + 3600
      val machine_record_effective_shaft = Random.nextInt(10) + 1
      val machine_record_amount_process = Random.nextInt(200) + 1
      val machine_record_use_memory = Random.nextInt(2048) + 256
      val machine_record_free_memory = Random.nextInt(1024) + 128
      val machine_record_amount_use_code = Random.nextInt(20) + 5
      val machine_record_amount_free_code = Random.nextInt(10) + 1

      val xmlData =
        s"""<columns>
           |<col ColName="主轴转速">$machine_record_mainshaft_speed</col>
           |<col ColName="主轴倍率">$machine_record_mainshaft_multiplerate</col>
           |<col ColName="主轴负载">$machine_record_mainshaft_load</col>
           |<col ColName="进给倍率">$machine_record_feed_speed</col>
           |<col ColName="进给速度">$machine_record_feed_multiplerate</col>
           |<col ColName="PMC程序号">$machine_record_pmc_code</col>
           |<col ColName="循环时间">$machine_record_circle_time</col>
           |<col ColName="运行时间">$machine_record_run_time</col>
           |<col ColName="有效轴数">$machine_record_effective_shaft</col>
           |<col ColName="总加工个数">$machine_record_amount_process</col>
           |<col ColName="已使用内存">$machine_record_use_memory</col>
           |<col ColName="未使用内存">$machine_record_free_memory</col>
           |<col ColName="可用程序量">$machine_record_amount_use_code</col>
           |<col ColName="注册程序量">$machine_record_amount_free_code</col>
           |</columns>""".stripMargin
      xmlData
    }
    spark.udf.register("random_ml_data",()=>randomMLDataGeneratorUDF())
    spark.sql(
      """
        |select machinerecordid,machineid,machinerecordstate,random_ml_data() machine_record_data,
        |machinerecorddate,dwd_insert_user,
        |dwd_modify_user,dwd_insert_time,dwd_modify_time,etldate
        |from dwd.fact_machine_data
        |""".stripMargin).write.mode("overwrite").saveAsTable("dwd.fact_machine_data_pre")

  }
}
