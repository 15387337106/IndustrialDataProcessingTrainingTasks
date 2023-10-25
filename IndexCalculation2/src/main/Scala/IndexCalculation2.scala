import jodd.typeconverter.Convert.toDoubleArray
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{unix_timestamp, _}
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy.LEGACY
import spire.compat.fractional

import java.time.LocalDate
import java.sql.DriverManager
import java.util.{Date, Properties}
import scala.math.Fractional.Implicits.infixFractionalOps

object IndexCalculation2 {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("user","default")
    properties.setProperty("password","")
    properties.setProperty("driver","com.clickhouse.jdbc.ClickHouseDriver")
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val jdbcUrl="jdbc:clickhouse://localhost:8123/shtd_industry"
    val spark = SparkSession.builder().appName("Index Calculation 2").config("spark.sql.storeAssignmentPolicy","LEGACY").enableHiveSupport().getOrCreate()
    spark.read.table("dwd.fact_change_record").show
    spark.read.table("dwd.dim_machine").show

    val con = DriverManager.getConnection(jdbcUrl, properties)
    con.prepareStatement("drop table if exists machine_running_median").execute()
    con.prepareStatement("create table machine_running_median(machine_id int,machine_factory int,total_running_time int)engine=MergeTree() primary key machine_factory;").execute()
    con.close()
//    spark.sql(
//      """
//        |INSERT INTO TABLE dwd.dim_machine
//        |SELECT
//        |  BaseMachineID,
//        |  MachineFactory+2,
//        |  MachineNo,
//        |  MachineName,
//        |  MachineIP,
//        |  MachinePort,
//        |  MachineAddDate,
//        |  MachineRemarks,
//        |  MachineAddEmpID,
//        |  MachineResponsEmpID,
//        |  MachineLedgerXml,
//        |  ISWS,
//        |  dwd_insert_user,
//        |  dwd_modify_user,
//        |  dwd_insert_time,
//        |  dwd_modify_time,
//        |  etldate
//        |FROM dwd.dim_machine
//        |""".stripMargin).show
    //方法一
    spark.sql("use dwd")
    // inti: 生成数据
    //
//    spark.sql(
//      s"""
//         |INSERT INTO `fact_change_record` VALUES (20260668, 113, '0', '运行', '2021-10-12 15:58:55', '2021-10-12 18:06:41', '<col ColName="设备IP">192.168.1.204</col><col ColName="进给速度">0</col><col ColName="急停状态">否</col><col ColName="加工状态">MDI</col><col ColName="刀片相关信息">刀片组号:0;type:0;刀片组的全部数量:6;刀片号:0;刀片组号:0;寿命计时0</col><col ColName="切削时间">518100</col><col ColName="未使用内存">1006</col><col ColName="循环时间">573760</col><col ColName="报警信息">{"AlmNo":0,"AlmStartTime":"无","AlmMsg":"无报警"}</col><col ColName="主轴转速">0</col><col ColName="上电时间">1799120</col><col ColName="总加工个数">7850</col><col ColName="班次信息">null</col><col ColName="运行时间">793215</col><col ColName="正在运行刀具信息">没有刀具在运行</col><col ColName="有效轴数">8</col><col ColName="主轴负载">0</col><col ColName="PMC程序号">0012</col><col ColName="进给倍率">100</col><col ColName="主轴倍率">100</col><col ColName="已使用内存">67</col><col ColName="可用程序量">903</col><col ColName="刀补值">0</col><col ColName="工作模式">MEMory</col><col ColName="机器状态">待机</col><col ColName="连接状态">normal</col><col ColName="加工个数">3432</col><col ColName="机床位置">["-1.34","1.219","-0.08","0","252.627","271.214","485.396","0","-1.34","1.219","-0.08","0"]</col><col ColName="注册程序量">108</col>',0,"user1","user1","2023-10-18 20:01:17","2023-10-18 20:01:17","2023-10-17");         |
//         |""".stripMargin).show
//    spark.sql(
//      s"""
//        |INSERT INTO `dim_machine` VALUES (113, 789, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0,"user1","user1","2023-10-18 20:01:17","2023-10-18 20:01:17","2023-10-17");
//        |""".stripMargin).show
    spark.sql(
      """
        |select MachineFactory,BaseMachineID,total_running_time
        |from    (select MachineFactory,BaseMachineID,dur_time total_running_time,
        |             row_number() over(partition by MachineFactory order by dur_time) as duration,
        |             count(*) over(partition by MachineFactory) cnt
        |         from (select MachineFactory,BaseMachineID,
        |         sum(unix_timestamp(ChangeEndTime)-unix_timestamp(ChangeStartTime)) dur_time
        |               from  dwd.fact_change_record
        |               join dwd.dim_machine  on BaseMachineID=ChangeMachineID and ChangeRecordState="运行"
        |               group by MachineFactory,BaseMachineID)t1)t2
        |where if(cnt%2=0,duration in(cnt/2,cnt/2+1),duration=(cnt+1)/2)
        |""".stripMargin).show(100)

    //方法二 （只能实现中位数有两个时返回均值）
    spark.sql(
      """
        |select first(c.ChangeMachineID) as machine_id,d.MachineFactory machine_factory,percentile(unix_timestamp(c.ChangeEndTime)-unix_timestamp(c.ChangeStartTime),0.5) total_running_time from dwd.fact_change_record c
        |join dwd.dim_machine d
        |on c.ChangeMachineID=d.BaseMachineID
        |group by d.MachineFactory
        |""".stripMargin).show(100)
//    .write.mode("append").jdbc(jdbcUrl,"machine_running_median",properties)

    spark.stop()
  }
}
