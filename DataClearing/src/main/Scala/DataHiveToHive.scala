import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.util.LongAccumulator

import scala.collection.immutable.Seq
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.{Date, Properties}

object DataHiveToHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Batch Pull Data").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")

    val dbList = Array(("environmentdata","fact_environment_data"),("changerecord","fact_change_record")
      ,("basemachine","dim_machine"),("producerecord","fact_produce_record")
      ,("machinedata","fact_machine_data"))
    val counter = spark.sparkContext.longAccumulator

    dbList.foreach(x=>{
      counter.add(1)
      val df = spark.read.table(s"ods.${x._1}")
      counter.value.toLong match {
        case 2L => df.dropDuplicates(Array[String]("changeid", "changemachineid"))
        case 3L => df.dropDuplicates("basemachineid")
        case _ => println("不进行去重")
      }
      df.withColumnRenamed(df.columns.last, "etldate")
        .withColumn("dwd_insert_user", lit("user1"))
        .withColumn("dwd_modify_user", lit("user1"))
        .withColumn("dwd_insert_time", lit(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())))
        .withColumn("dwd_modify_time", lit(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())))
        .write.partitionBy("etldate").mode("overwrite")
        .saveAsTable(s"dwd.${x._2}")
//      df.show
    })
    spark.sql("select * from dwd.fact_environment_data order by EnvoId desc limit 5").show
    spark.sql("select * from dwd.fact_change_record order by ChangeMachineID desc limit 5").show
    spark.sql("select * from dwd.dim_machine order by BaseMachineID desc limit 5").show
    spark.sql("select * from dwd.fact_produce_record order by ProduceMachineID desc limit 5").show
    spark.sql("select * from dwd.fact_machine_data order by MachineID desc limit 5").show
  }

}
