
import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

import java.util.{Date, Properties}
import scala.collection.mutable.ListBuffer

case class MachineFT(machine_id: Int, machine_factory: Int, total_running_time: Int, row_num: Int)

object IndexCalculation2 {
  def main(args: Array[String]): Unit = {
    
        val properties = new Properties()
        properties.setProperty("user","default")
        properties.setProperty("password","")
        properties.setProperty("driver","com.clickhouse.jdbc.ClickHouseDriver")
        properties.setProperty("createTableColumnTypes","machine_id int,machine_factory int,total_running_time int")
        properties.setProperty("createTableOptions","engine=MergeTree() primary key machine_factory")
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

        val jdbcUrl="jdbc:clickhouse://master:8123/shtd_industry"
    val spark = SparkSession.builder().appName("Index Calculation 2").config("spark.sql.storeAssignmentPolicy", "LEGACY").enableHiveSupport().getOrCreate()
    //    spark.read.table("dwd.fact_change_record").show(100000)
    //    spark.read.table("dwd.dim_machine").show
    //
    ////    val con = DriverManager.getConnection(jdbcUrl, jpsproperties)
    ////    con.prepareStatement("drop table if exists machine_running_median").execute()
    ////    con.prepareStatement("create table machine_running_median(machine_id int,machine_factory int,total_running_time int)engine=MergeTree() primary key machine_factory;").execute()
    ////    con.close()
    ////    spark.sql(
    ////      """
    ////        |INSERT INTO TABLE dwd.dim_machine
    ////        |SELECT
    ////        |  BaseMachineID,
    ////        |  MachineFactory+10,
    ////        |  MachineNo,
    ////        |  MachineName,
    ////        |  MachineIP,
    ////        |  MachinePort,
    ////        |  MachineAddDate,
    ////        |  MachineRemarks,
    ////        |  MachineAddEmpID,
    ////        |  MachineResponsEmpID,
    ////        |  MachineLedgerXml,
    ////        |  ISWS,
    ////        |  dwd_insert_user,
    ////        |  dwd_modify_user,
    ////        |  dwd_insert_time,
    ////        |  dwd_modify_time,
    ////        |  etldate
    ////        |FROM dwd.dim_machine
    ////        |""".stripMargin).show
    //    //方法一
    //    spark.sql("use dwd")
    //    // inti: 生成数据
    //    //
    ////    spark.sql(
    ////      s"""
    ////         |INSERT INTO `fact_change_record` VALUES (20260668, 214, '0', '运行', '2021-10-12 15:58:55', '2021-10-12 18:06:41', '<col ColName="设备IP">192.168.1.204</col><col ColName="进给速度">0</col><col ColName="急停状态">否</col><col ColName="加工状态">MDI</col><col ColName="刀片相关信息">刀片组号:0;type:0;刀片组的全部数量:6;刀片号:0;刀片组号:0;寿命计时0</col><col ColName="切削时间">518100</col><col ColName="未使用内存">1006</col><col ColName="循环时间">573760</col><col ColName="报警信息">{"AlmNo":0,"AlmStartTime":"无","AlmMsg":"无报警"}</col><col ColName="主轴转速">0</col><col ColName="上电时间">1799120</col><col ColName="总加工个数">7850</col><col ColName="班次信息">null</col><col ColName="运行时间">793215</col><col ColName="正在运行刀具信息">没有刀具在运行</col><col ColName="有效轴数">8</col><col ColName="主轴负载">0</col><col ColName="PMC程序号">0012</col><col ColName="进给倍率">100</col><col ColName="主轴倍率">100</col><col ColName="已使用内存">67</col><col ColName="可用程序量">903</col><col ColName="刀补值">0</col><col ColName="工作模式">MEMory</col><col ColName="机器状态">待机</col><col ColName="连接状态">normal</col><col ColName="加工个数">3432</col><col ColName="机床位置">["-1.34","1.219","-0.08","0","252.627","271.214","485.396","0","-1.34","1.219","-0.08","0"]</col><col ColName="注册程序量">108</col>',0,"user1","user1","2023-10-18 20:01:17","2023-10-18 20:01:17","2023-10-17");
    ////         |""".stripMargin).show
    ////    spark.sql(
    ////      s"""
    ////        |INSERT INTO `dim_machine` VALUES (214, 789, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0,"user1","user1","2023-10-18 20:01:17","2023-10-18 20:01:17","2023-10-17");
    ////        |""".stripMargin).show

    val df = spark.sql(
      """
        |select machine_factory,machine_id,total_running_time
        |from    (select machine_factory,machine_id,dur_time total_running_time,
        |             row_number() over(partition by machine_factory order by dur_time)  duration,
        |             count(*) over(partition by machine_factory) cnt
        |         from (select machinefactory machine_factory,basemachineid machine_id,
        |         sum(unix_timestamp(changeendtime)-unix_timestamp(changestarttime)) dur_time
        |               from  dwd.fact_change_record
        |               join dwd.dim_machine  on basemachineid=changemachineid and changerecordstate="运行"
        |               group by machinefactory,basemachineid)t1)t2
        |where if(cnt%2=0,duration in(cnt/2,cnt/2+1),duration=(cnt+1)/2)
        |""".stripMargin)
      df.show(100)

    //方法二 （只能实现中位数有两个时返回均值）
    //        spark.sql(
    //          """
    //            |select first(c.ChangeMachineID) as machine_id,d.MachineFactory machine_factory,percentile(unix_timestamp(c.ChangeEndTime)-unix_timestamp(c.ChangeStartTime),0.5) total_running_time from dwd.fact_change_record c
    //            |join dwd.dim_machine d
    //            |on c.ChangeMachineID=d.BaseMachineID
    //            |group by d.MachineFactory
    //          """).show()


//    //方法三  todo  跑一边，还没写完
//    import spark.implicits._
//    val df2 = spark.table("dwd.dim_machine")
//    val df1 = spark.table("dwd.fact_change_record")
//    val fullTable = df1.join(df2, df1.col("ChangeMachineID") === df2.col("BaseMachineID")).filter("ChangeRecordState='运行'")
//    val result = fullTable
//      .withColumn("duration_time", unix_timestamp(col("ChangeEndTime")) - unix_timestamp(col("ChangeStartTime")))
//      .selectExpr("cast(MachineFactory as int)", "cast(ChangeMachineID as int)", "cast(duration_time as int)",
//        "row_number() over(partition by MachineFactory order by duration_time) as row_num")
//      .dropDuplicates()
//      .repartitionByRange(col("MachineFactory"))
//      .sortWithinPartitions("MachineFactory", "duration_time")
//      .mapPartitions((x: Iterator[Row]) => {
//        print(x)
//        //        val rows = x.map (y=>{
//        //          println(MachineFT(y.getInt(0), y.getInt(1), y.getInt(2), y.getInt(3)))
//        //          MachineFT(y.getInt(0), y.getInt(1), y.getInt(2), y.getInt(3))
//        //        })
//        val rows = x.filter(y => {
//          (x.length % 2 == 0 && (y.getInt(3) == x.length / 2 || y.getInt(3) == x.length / 2 + 1)) ||
//            (x.length % 2 == 1 && y.getInt(3) == x.length / 2 + 1)
//        })
//        //        println(rows, "-------------------mappartitions")
//        rows
//      })
//
//    result.show()
    //      .collect({case x:List[MachineFT]=>x.head})
    //      .toDF("machine_id","machine_factory","total_running_time").show()


    //    println(result,"-------------------result")
    //    println(result.collect(),"-------------------result.collect()")
    //    println(result.collect().length,"-------------------result.collect().length")
    //    println(result.collect().toList,"-------------------result.collect().toList,")
    //    spark.createDataFrame(spark.sparkContext.makeRDD(result), StructType(Seq(
    //      StructField("machine_id", IntegerType, true),
    //      StructField("machine_factory", IntegerType, true),
    //      StructField("total_running_time", IntegerType, true)
    //    ))).toDF().show()
        df.write.mode("append").jdbc(jdbcUrl,"machine_running_median",properties)

    spark.stop()
  }
}
