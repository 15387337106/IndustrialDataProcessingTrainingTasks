# 离线数据处理

## 任务一：数据抽取

### 任务要求

`编写Scala代码，使用Spark将MySQL的shtd_industry库中表EnvironmentData，ChangeRecord，BaseMachine，MachineData,ProduceRecord全量抽取到Hive的ods库（需自建）中对应表environmentdata，changerecord，basemachine， machinedata， producerecord中。`

**1、   抽取MySQL的shtd_industry库中`EnvironmentData`表的全量数据进入Hive的ods库中表`environmentdata`，字段排序、类型不变，同时添加静态分区，分区字段类型为String，且值为当前日期的前一天日期（分区字段格式为yyyyMMdd）。**

**2、   抽取MySQL的shtd_industry库中`ChangeRecord`表的全量数据进入Hive的ods库中表`changerecord`，字段排序、类型不变，同时添加静态分区，分区字段类型为String，且值为当前日期的前一天日期（分区字段格式为yyyyMMdd）。**

**3、   抽取MySQL的shtd_industry库中`BaseMachine`表的全量数据进入Hive的ods库中表`basemachine`，字段排序、类型不变，同时添加静态分区，分区字段类型为String，且值为当前日期的前一天日期（分区字段格式为yyyyMMdd）。**

**4、   抽取MySQL的shtd_industry库中`ProduceRecord`表的全量数据进入Hive的ods库中表`producerecord`，字段排序、类型不变，同时添加静态分区，分区字段类型为String，且值为当前日期的前一天日期（分区字段格式为yyyyMMdd）。**

**5、   抽取MySQL的shtd_industry库中`MachineData`表的全量数据进入Hive的ods库中表`machinedata`，字段排序、类型不变，同时添加静态分区，分区字段类型为String，且值为当前日期的前一天日期（分区字段格式为yyyyMMdd）。**

*分析：5个题目要求基本相同，唯一不同的是表名，可以定义一个列表包含所有表的输入输出表，但该题输出表的名字和输入表只有大小写不同的区别，我们知道MYSQL和HIVE默认是不区分大小写的，所以根据环境配置，我们可以选择**Array\[String]**()或者**Array\[Tulpe2]()***,*然后Foreach遍历进行操作*

### **代码实现**

```scala
import org.apache.spark.sql.{SparkSession, SaveMode}
import java.util.Properties
import org.apache.spark.sql.functions._

// 创建 Spark 会话
val spark = SparkSession.builder()
  .appName("pull Environment Data")
  .enableHiveSupport()
  .getOrCreate()

// 设置 Parquet 文件写入的 Legacy 模式
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")

// MySQL 数据库连接信息
val jdbcUrl = "jdbc:mysql://192.168.100.101/shtd_industry"
val properties = new Properties()
properties.setProperty("user", "root")
properties.setProperty("password", "123456")

// 显示数据库列表
spark.sql("show databases")

// 需要导入的表名列表
val tableNames = List("EnvironmentData", "ChangeRecord", "BaseMachine", "ProduceRecord", "MachineData")

// 遍历每个表名
tableNames.foreach(x => {
  // 从 MySQL 数据库读取数据
  val df = spark.read.jdbc(jdbcUrl, x, properties)
  
  // 添加名为 "partition_date" 的新列，值为昨天的日期
  df.withColumn("partition_date", lit(LocalDate.now().minusDays(1).toString))
  
  // 将数据保存为 Hive 表，并按 "partition_date" 分区
  .write
  .partitionBy("partition_date")
  .mode(SaveMode.Overwrite)
  .saveAsTable(s"ods.${x.toLowerCase}")
  
  // 显示 Hive 表的分区信息
  spark.sql(s"show partitions ods.${x.toLowerCase}").show
})

// 关闭 Spark 会话
spark.stop()
```

### 总结

1. 通过创建 Spark 会话，你准备了一个 Spark 应用程序的环境，允许你使用 Spark 引擎进行数据处理。
2. 通过设置 Parquet 文件写入的 Legacy 模式，确保了与以前版本的 Parquet 格式的兼容性。
3. 配置了 MySQL 数据库的连接信息，包括 JDBC URL、用户名和密码，以便从 MySQL 数据库中读取数据。
4. 通过执行 SQL 查询 "show databases"，你可以显示当前数据库中的所有数据库列表。
5. 你定义了一个包含需要导入的表名的列表，包括 "EnvironmentData"、"ChangeRecord"、"BaseMachine"、"ProduceRecord" 和 "MachineData"。
6. 使用 `foreach` 循环迭代每个表名，你执行了相同的操作，包括：
   - 从 MySQL 数据库中读取数据并将其存储在 DataFrame `df` 中。
   - 使用 `withColumn` 函数添加一个名为 "partition_date" 的新列，该列的值为昨天的日期。
   - 使用 `write` 方法将数据保存为 Hive 表，按 "partition_date" 列进行分区，使用 "overwrite" 模式覆盖现有数据。
   - 通过执行 SQL 查询 "show partitions"，你可以显示新创建的 Hive 表的分区信息，以确认分区成功。
7. 最后，通过关闭 Spark 会话，你释放了资源并结束了 Spark 应用程序的运行。

这段代码是一个通用的数据抽取和处理流程，可以轻松地在多个表之间重复使用，同时根据每个表的特定需求进行相应的更改。它展示了如何使用 Spark 和 Hive 来执行数据抽取、分区、转换和保存到数据仓库中的表。

------

[]()

## 任务二：数据清洗

### 任务要求

`编写Hive SQL代码，将ods库中相应表数据全量抽取到Hive的dwd库（需自建）中对应表中。表中有涉及到timestamp类型的，均要求按照yyyy-MM-dd HH:mm:ss，不记录毫秒数，若原数据中只有年月日，则在时分秒的位置添加00:00:00，添加之后使其符合yyyy-MM-dd HH:mm:ss。`

**1、   抽取ods库中environmentdata的全量数据进入Hive的dwd库中表fact_environment_data，分区字段为etldate且值与ods库的相对应表该值相等，并添加dwd_insert_user、dwd_insert_time、dwd_modify_user、dwd_modify_time四列,其中dwd_insert_user、dwd_modify_user均填写“user1”，dwd_insert_time、dwd_modify_time均填写当前操作时间，并进行数据类型转换。**

分析：首先分析总要求，得到以下信息

- 需要编写**HIVE SQL**即进入HIVE CLI 编写SQL实现功能
- 表中有涉及到timestamp类型的，均要求按照yyyy-MM-dd HH:mm:ss 即在timestamp类型的列可以加速**timestamp**关键词实现:   `若原数据中只有年月日，则在时分秒的位置添加00:00:00，添加之后使其符合yyyy-MM-dd HH:mm:ss`     *eg. **select timestamp 2022-10-10** 输出 **2022-10-10 00:00:00***

**观察数据及表结构：**

```sql
select *  from environmentdata limit 5;
desc environmentdata;
```

![image-20231107104957728](C:/Users/23041/AppData/Roaming/Typora/typora-user-images/image-20231107104957728.png)

![image-20231107104930134](C:/Users/23041/AppData/Roaming/Typora/typora-user-images/image-20231107104930134.png)

创建表dwd.fact_environment_data并创建动态分区

```sql
create table if not exists dwd.fact_environment_data
(
    envoid         STRING,
    baseid         STRING,
    co2            STRING,
    pm25           STRING,
    pm10           STRING,
    temperature    STRING,
    humidity       STRING,
    tvoc           STRING,
    ch2o           STRING,
    smoke          STRING,
    inputtime      STRING,
    dwd_insert_user STRING,
    dwd_insert_time TIMESTAMP,
    dwd_modify_user STRING,
    dwd_modify_time TIMESTAMP
)
partitioned by (etldate STRING);
```

插入数据

```sql
insert overwrite table dwd.fact_environment_data partition(etldate)
select 
    envoid,baseid,co2,pm25,pm10,temperature,humidity,tvoc,ch2o,smoke,inputtime,
    "user1" as dwd_insert_user,
    current_timestamp() as dwd_insert_time,
    "user1" as dwd_modify_user,
    current_timestamp() as dwd_modify_time,
    partition_date
from environmentdata;
```



**2、   抽取ods库中changerecord的全量数据进入Hive的dwd库中表fact_change_record，抽取数据之前需要对数据根据changeid和changemachineid进行联合去重处理，分区字段为etldate且值与ods库的相对应表该值相等，并添加dwd_insert_user、dwd_insert_time、dwd_modify_user、dwd_modify_time四列,其中dwd_insert_user、dwd_modify_user均填写“user1”，dwd_insert_time、dwd_modify_time均填写当前操作时间，并进行数据类型转换。**

分析：和题1实现逻辑不同之处为：**抽取数据之前需要对数据根据changeid和changemachineid进行联合去重处理**

代码实现:

```sql
create table if not exists dwd.fact_change_record
(
    ChangeID         int,
    ChangeMachineID         int,
    ChangeMachineRecordID            int,
    ChangeRecordState           STRING,
    ChangeStartTime           timestamp,
    ChangeEndTime    timestamp,
    ChangeRecordData       STRING,
    ChangeHandleState           int,
    dwd_insert_user STRING,
    dwd_insert_time TIMESTAMP,
    dwd_modify_user STRING,
    dwd_modify_time TIMESTAMP
)
partitioned by (etldate STRING);
```

### 代码实现

```sql
insert overwrite table dwd.fact_change_record
    partition (etldate)
select
    split(compositekey, ',')[0] as changeid,
    split(compositekey, ',')[1] as changemachineid,
    changemachinerecordid,
    changerecordstate,
    date_format(changestarttime,"yyyy-MM-dd HH:mm:ss") changestarttime,
    date_format(changeendtime,"yyyy-MM-dd HH:mm:ss") changestarttime,
    changerecorddata,
    changehandlestate,
    "user1" as dwd_insert_user,
    current_timestamp() as dwd_insert_time,
    "user1" as dwd_modify_user,
    current_timestamp() as dwd_modify_time,
    partition_date
from (select
        distinct(concat(changeid, ",", changemachineid)) as compositekey,changemachinerecordid,ChangeRecordState,
         changestarttime,
         changeendtime,
        changerecorddata,
        changehandlestate,partition_date
      from ods.changerecord
     ) t1
```

### 总结

1. `insert overwrite table dwd.fact_change_record partition (etldate)`：这是INSERT语句的开头，它指定了目标表的名称为 `dwd.fact_change_record`，并且表有一个名为 `etldate` 的分区。如果在目标表中已经存在与新数据分区值匹配的数据，那么它将被覆盖。
2. `select ...`：这是INSERT语句的主体部分，它从源表中选择数据，并对其进行转换和操作，然后将结果插入到目标表中。
3. `split(compositekey, ',')[0] as changeid` 和 `split(compositekey, ',')[1] as changemachineid`：这两行代码从 `compositekey` 列中使用逗号分隔符对数据进行拆分，并将拆分后的第一个部分作为 `changeid`，第二部分作为 `changemachineid`。这些列的数据将插入到目标表中。
4. `date_format(changestarttime,"yyyy-MM-dd HH:mm:ss") changestarttime` 和 `date_format(changeendtime,"yyyy-MM-dd HH:mm:ss") changestarttime`：这两行代码使用 `date_format` 函数将 `changestarttime` 和 `changeendtime` 列的日期时间值格式化为 "yyyy-MM-dd HH:mm:ss" 的字符串格式。这些格式化后的日期时间字符串将插入到目标表中。
5. `"user1" as dwd_insert_user` 和 `current_timestamp() as dwd_insert_time`：这两行代码分别为 `dwd_insert_user` 和 `dwd_insert_time` 列分配了静态值 "user1" 和当前时间戳。这些值将插入到目标表中。
6. `"user1" as dwd_modify_user` 和 `current_timestamp() as dwd_modify_time`：这两行代码分别为 `dwd_modify_user` 和 `dwd_modify_time` 列分配了静态值 "user1" 和当前时间戳。这些值将插入到目标表中。
7. `partition_date`：这一列来自子查询中的 `partition_date` 列，它将插入到目标表中。
8. `from (select ...)`：这是一个子查询，它从源表 `ods.changerecord` 中选择一些列，并对它们进行操作。子查询内部首先使用 `concat` 函数将 `changeid` 和 `changemachineid` 列的值连接成一个 `compositekey`，然后选择其他列。



## 任务三：指标计算

### 题目一

**编写Scala代码，使用Spark根据dwd层fact_change_record表统计每个月（change_start_time的月份）、每个设备、每种状态的时长，若某状态当前未结束（即change_end_time值为空）则该状态不参与计算。计算结果存入MySQL数据库shtd_industry的machine_state_time表（表结构如下）中**

| 字段名              | 类型    | 中文含义       | 备注               |
| ------------------- | ------- | -------------- | ------------------ |
| machine_id          | int     | 设备id         |                    |
| change_record_state | varchar | 状态           |                    |
| duration_time       | varchar | 持续时长（秒） | 当月该状态的时长和 |
| year                | int     | 年             | 状态产生的年       |
| month               | int     | 月             | 状态产生的月       |

### 思路分析

- 使用Scala+Spark  即可以使用sparkSql或SparkDataframe API 进行处理
- 表统计每个月（change_start_time的月份）、每个设备、每种状态的时长 即GroupBy 月（date_format(change_start_time,"yyyy-MM")）、设备（机器ID machine_id 机器唯一标识)、状态（change_record_state）的时长（change_end_time-change_start_time --`change_start_time 和change_end_time均为string类型或timestamp类型,使用unix_timestamp转为时间戳格式相减即为以秒为单位的机器运行持续时长
- 若某状态当前未结束（即change_end_time值为空）则该状态不参与计算。 过滤掉change_end_time为空的数据
- 年  year() 获取时间戳的年
- 月  month() 获取时间戳的月

### 代码实现

```scala
// 创建一个 Properties 对象来存储数据库连接属性
val properties = new Properties()
properties.setProperty("user", "root") // 设置用户名
properties.setProperty("password", "123456") // 设置密码
properties.setProperty("characterEncoding", "utf-8") // 设置字符编码
properties.setProperty("primaryKey", "machine_id") // 设置主键列名
properties.setProperty("createTableColumnTypes", "machine_id int, change_record_state varchar(64), duration_time varchar(64), year int, month int") // 设置创建表时的列类型
```

```scala
// 定义 MySQL 数据库连接 URL
val jdbcUrl = "jdbc:mysql://192.168.100.101:3306/shtd_industry"

// 创建 SparkSession
val spark = SparkSession.builder()
  .appName("Index Calculation")
  .enableHiveSupport()
  .getOrCreate()

// 从 Hive 表中读取数据，并过滤掉 "ChangeEndTime" 列不为 NULL 的行
var df = spark.read.table("dwd.fact_change_record").filter(col("ChangeEndTime").isNotNull)

// 显示读取的数据
df.show()

// 选择 "ChangeStartTime" 列的最大值和最小值并显示
df.select(max("ChangeStartTime"), min("ChangeStartTime")).show

// 对数据进行转换和聚合操作
df = df.groupBy(
    col("ChangeStartTime"),
    col("ChangeMachineID").as("machine_id"),
    date_format(col("ChangeRecordState"), "yyyy-MM").as("change_record_state")
)
  .agg(sum(unix_timestamp(col("ChangeEndTime")) - unix_timestamp(col("ChangeStartTime")).cast(LongType)).as("duration_time"))
  .withColumn("year", date_format(col("ChangeStartTime"), "yyyy"))
  .withColumn("month", date_format(col("ChangeStartTime"), "MM"))

// 选择需要的列
df.select("machine_id", "change_record_state", "duration_time", "year", "month")
  .write
  .mode("append")
  .jdbc(jdbcUrl, "machine_state_time", properties)

// 关闭 SparkSession
spark.stop()
```

### 总结

1. 创建一个 `Properties` 对象：首先创建一个 `Properties` 对象，该对象用于存储连接到MySQL数据库时需要的属性信息，包括用户名、密码、字符编码等。
2. 设置数据库连接属性：使用 `properties.setProperty` 方法设置数据库连接属性，如用户名、密码、字符编码、主键列名、以及创建表时的列类型等。
3. 定义JDBC URL：创建一个字符串 `jdbcUrl`，其中包含了MySQL数据库的连接信息，包括主机地址、端口号和数据库名。
4. 创建SparkSession：使用Apache Spark的 `SparkSession` 创建一个Spark会话，设置应用程序名称为 "Index Calculation"，并启用Hive支持，以便可以使用Hive表。
5. 读取Hive表数据：使用Spark的 `read.table` 方法读取名为 "dwd.fact_change_record" 的Hive表的数据，并过滤掉 "ChangeEndTime" 列不为NULL的行。
6. 显示数据：使用 `df.show()` 将读取的数据显示在控制台上。
7. 进行数据转换和聚合：对读取的数据进行转换和聚合操作。通过分组、计算持续时间（以秒为单位）、提取年份和月份等操作，创建一个新的DataFrame `df`。
8. 选择需要的列：从新的DataFrame `df` 中选择 "machine_id"、"change_record_state"、"duration_time"、"year" 和 "month" 列。
9. 将数据写入MySQL：使用 `write.jdbc` 方法将数据写入MySQL数据库的 "machine_state_time" 表中。这里使用了前面设置的连接属性 `properties`。

### 题目二

  **编写Scala代码，使用Spark根据dwd层fact_change_record表关联dim_machine表统计每个车间中所有设备运行时长（即设备状态为“运行”）的中位数在哪个设备（为偶数时，两条数据原样保留输出），若某个设备运行状态当前未结束（即change_end_time值为空）则该状态不参与计算。计算结果存入clickhouse数据库shtd_industry的表machine_running_median（表结构如下）中**

| 字段名             | 类型 | 中文含义   | 备注           |
| ------------------ | ---- | ---------- | -------------- |
| machine_id         | int  | 设备id     |                |
| machine_factory    | int  | 所属车间   |                |
| total_running_time | int  | 运行总时长 | 结果以秒为单位 |

### 思路分析

- 使用Scala+Spark  即可以使用sparkSql或SparkDataframe API 进行处理
- 根据dwd层fact_change_record表关联dim_machine表  使用join关键字 连接两个表查询
- 若某个设备运行状态当前未结束（即change_end_time值为空）则该状态不参与计算 过滤change_end_time=null
- 统计每个车间中所有设备运行时长  每个车间  即按车间分组 所有设备时长即sum()求和状态的最后运行时间-开始时间，运行时长，即过滤状态为运行的
- 中位数在哪个设备（为偶数时，两条数据原样保留输出）需要求分组之后的数据在那个未分组的设备，即不适用groupby，使用over() 开窗函数，groupby会去重，over保留原数据条数，使用over()配合row_number()按车间分区，按运行时长排序，得到每个车间的运行时长的排名，使用over()配后count(*) 按车间分区，得到每个车间的中位数有多少个，再添加判断条件，如何个数为偶数则取出两个，即排名=个数/2和个数/2+1的数据，等于奇数则取出排名=个数/2的数据

### 核心代码实现

```scala
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
    df.write.mode("append").jdbc(jdbcUrl, "machine_running_median", properties)
```

### 总结

1. `val df = spark.sql(...)`：使用`spark.sql`方法执行SQL查询，将查询结果存储在DataFrame `df`中。
2. SQL查询：SQL查询主要是对Hive表`dwd.fact_change_record`和`dwd.dim_machine`进行联接操作，然后计算中位数值。下面是查询的主要部分：
   - 首先，通过联接操作将`dwd.fact_change_record`和`dwd.dim_machine`表中的数据合并，连接条件为`basemachineid=changemachineid`和`changerecordstate="运行"`。这个操作将筛选出满足条件的记录。
   - 然后，对合并后的结果进行分组（`GROUP BY`）操作，以计算每个`machine_factory`和`machine_id`组合的运行总时长，并将其存储在名为`dur_time`的列中。
   - 接下来，使用窗口函数（`row_number()`和`count(*) over(...)`)为每个`machine_factory`分区计算`duration`和`cnt`。`duration`是每个机器运行总时长的排名，`cnt`是每个分区内的总记录数。
   - 最后，通过`if`条件筛选出中位数的记录，具体规则是如果`cnt`是偶数，则选择`duration`等于`cnt/2`或`cnt/2+1`的记录；如果`cnt`是奇数，则选择`duration`等于`(cnt+1)/2`的记录。
3. `df.show(100)`：显示DataFrame `df`的前100行数据，以便查看查询结果。
4. `df.write.mode("append").jdbc(jdbcUrl, "machine_running_median", properties)`：将DataFrame `df`的结果以追加模式写入ClickHouse数据库中的名为`machine_running_median`的表格。使用之前定义的连接属性`properties`和数据库连接URL`jdbcUrl`。

### 题目三  

**编写scala代码，使用Spark根据dwd层dwd.fact_produce_record表，基于全量历史增加设备生产一个产品的平均耗时字段（produce_per_avgtime），produce_code_end_time值为1900-01-01 00:00:00的数据为脏数据，需要剔除，并以produce_record_id和ProduceMachineID为联合主键进行去重（注：fact_produce_record表中，一条数据代表加工一个产品，produce_code_start_time字段为开始加工时间，produce_code_end_time字段为完成加工时间）。将得到的数据提取下表所需字段然后写入dws层表dws.machine_produce_per_avgtime表中**

| 字段名              | 类型 | 中文含义                 | 备注                       |
| ------------------- | ---- | ------------------------ | -------------------------- |
| produce_record_id   | int  | 生产记录id               | 每生产一件产品产生一条数据 |
| produce_machine_id  | int  | 设备id                   |                            |
| producetime         | int  | 该产品耗时               |                            |
| produce_per_avgtime | int  | 设备生产一个产品平均耗时 | 单位：秒                   |

### 思路分析

- 使用Scala+Spark  即可以使用sparkSql或SparkDataframe API 进行处理

- 增加设备生产一个产品的平均耗时字段produce_per_avgtime）即按设备id分组对生产所有产品的记录的时长进行求和，然后再对该数据求平均值

- produce_code_end_time值为1900-01-01 00:00:00的数据为脏数据，需要剔除 过滤produce_code_end_time=1900-01-01 00:00:00的数据

- 以produce_record_id和ProduceMachineID为联合主键进行去重 去掉produce_record_id和ProduceMachineID一样的重复数据

### 核心代码实现

```scala
    spark.sql(
      """
        |select
        |    ProduceRecordId produce_record_id,
        |    ProduceMachineId produce_machine_id,
        |    unix_timestamp(ProduceCodeEndTime)-unix_timestamp(ProduceCodeStartTime) producetime,
        |    avg(unix_timestamp(ProduceCodeEndTime)-unix_timestamp(ProduceCodeStartTime))
        |        over(partition by ProduceMachineID) produce_per_avgtime
        |from
        |    dwd.fact_produce_record
        |where ProduceCodeEndTime!='1900-01-01 00:00:00'
        |""".stripMargin).dropDuplicates(Array("produce_record_id","produce_machine_id"))
      .write.mode("overwrite").saveAsTable("dws.machine_produce_per_avgtime")
```

### 总结

1. `spark.sql(...)`：这是一个Spark SQL查询，用于从Hive表 `dwd.fact_produce_record` 中选择数据。
2. SQL查询：查询执行了以下操作：
   - 选择了四个列并进行了重命名，将列名重命名为 `ProduceRecordId`，`ProduceMachineId`，`producetime`，和 `produce_per_avgtime`。
   - `producetime` 列计算了从 `ProduceCodeStartTime` 到 `ProduceCodeEndTime` 的时间差，以秒为单位。
   - `produce_per_avgtime` 列使用窗口函数 `avg`，在每个 `ProduceMachineID` 分区中计算了平均的 `producetime` 值。
3. `.dropDuplicates(Array("produce_record_id", "produce_machine_id"))`：这一行代码用于删除重复的记录。它基于 `produce_record_id` 和 `produce_machine_id` 列的值来确定重复记录，并只保留一个副本。
4. `.write.mode("overwrite").saveAsTable("dws.machine_produce_per_avgtime")`：这一行代码将处理后的结果保存到名为 "dws.machine_produce_per_avgtime" 的Hive表中。使用 `overwrite` 模式会覆盖目标表，如果表已经存在的话。

### 题目四

**编写Scala代码，使用Spark根据dwd层dwd.fact_produce_record表，基于全量历史数据计算各设备生产一个产品的平均耗时，produce_code_end_time值为1900-01-01 00:00:00的数据为脏数据，需要剔除，并以produce_record_id和produce_machine_id为联合主键进行去重（注：fact_produce_record表中，一条数据代表加工一个产品，produce_code_start_time字段为开始加工时间，produce_code_end_time字段为完成加工时间）。将设备每个产品的耗时与该设备平均耗时作比较，保留耗时高于平均值的产品数据。将得到的数据写入clickhouse表machine_produce_per_avgtime表中**

| 字段名              | 类型 | 中文含义                 | 备注                       |
| ------------------- | ---- | ------------------------ | -------------------------- |
| produce_record_id   | int  | 生产记录id               | 每生产一件产品产生一条数据 |
| produce_machine_id  | int  | 设备id                   |                            |
| producetime         | int  | 该产品耗时               |                            |
| produce_per_avgtime | int  | 设备生产一个产品平均耗时 | 单位：秒                   |

### 思路分析

- 该题思路同**第三题思路**唯一不同的是需要过滤掉producetime<produce_per_avgtime的数据

### 核心代码实现

```scala
    spark.sql(
        """
          |
          |select
          |produce_record_id,produce_machine_id,producetime,produce_per_avgtime
          |from (select
          |          ProduceRecordId produce_record_id,
          |          ProduceMachineId produce_machine_id,
          |          unix_timestamp(ProduceCodeEndTime)-unix_timestamp(ProduceCodeStartTime) producetime,
          |          avg(unix_timestamp(ProduceCodeEndTime)-unix_timestamp(ProduceCodeStartTime))
          |              over(partition by ProduceMachineID) produce_per_avgtime
          |      from
          |          dwd.fact_produce_record
          |      where ProduceCodeEndTime!='1900-01-01 00:00:00')t1
          |where producetime>produce_per_avgtime
          |""".stripMargin).dropDuplicates(Array("produce_record_id","produce_machine_id"))
      .write.mode("append").jdbc("jdbc:clickhouse://master:8123","shtd_industry.machine_produce_per_avgtime",properties)
```

### 总结

1. `spark.sql(...)`：这是一个Spark SQL查询，用于从Hive表 `dwd.fact_produce_record` 中选择数据。
2. SQL查询：查询执行了以下操作：
   - 首先，选择了四个列并进行了重命名，将列名重命名为 `produce_record_id`，`produce_machine_id`，`producetime`，和 `produce_per_avgtime`。
   - `producetime` 列计算了从 `ProduceCodeStartTime` 到 `ProduceCodeEndTime` 的时间差，以秒为单位。
   - `produce_per_avgtime` 列使用窗口函数 `avg`，在每个 `ProduceMachineID` 分区中计算了平均的 `producetime` 值。
3. `.dropDuplicates(Array("produce_record_id", "produce_machine_id"))`：这一行代码用于删除重复的记录。它基于 `produce_record_id` 和 `produce_machine_id` 列的值来确定重复记录，并只保留一个副本。
4. `.write.mode("append").jdbc("jdbc:clickhouse://master:8123","shtd_industry.machine_produce_per_avgtime",properties)`：这一行代码将处理后的结果以追加模式写入ClickHouse数据库的 "shtd_industry.machine_produce_per_avgtime" 表中，使用之前定义的连接属性 `properties` 和数据库连接URL "jdbc:clickhouse://master:8123"。

### 题目五

**编写scala代码，使用Spark根据dws层的dws.machine_produce_per_avgtime表,获取各设备生产耗时最长的两个产品的用时。将计算结果存入clickhouse数据库shtd_industry的表machine_produce_timetop2中**

| 字段名      | 类型 | 中文含义 | 备注 |
| ----------- | ---- | -------- | ---- |
| machine_id  | int  | 设备id   |      |
| first_time  | int  | 耗时最长 |      |
| second_time | int  | 耗时次长 |      |

### 思路分析

- 使用Scala+Spark  即可以使用sparkSql或SparkDataframe API 进行处理

- 获取各设备生产耗时最长的两个产品的用时  按设备id进行分组，使用collect_list对producetime进行列转行操作，然后使用array_sort排序后取第一个和第二个

### 核心代码实现

```scala
    import spark.implicits._
    spark.sql(
      """
        |select
        |     produce_machine_id,
        |     array_sort(collect_list(producetime)) time_list
        |from dws.machine_produce_per_avgtime
        |group by produce_machine_id
        |""".stripMargin).map(x=>{
        (x.getAs[Int]("produce_machine_id"),x.getAs[Seq[Long]]("time_list").head.toInt,
          x.getAs[Seq[Long]]("time_list").tail.head.toInt)})
      .toDF("machine_id","first_time","second_time")
      .write.mode("append").jdbc("jdbc:clickhouse://master:8123","shtd_industry.machine_produce_timetop2",properties)
```

### 总结

1. `import spark.implicits._`：导入Spark的隐式转换，以便可以使用DataFrame的各种方法。
2. `spark.sql(...)`：这是一个Spark SQL查询，用于从名为 `dws.machine_produce_per_avgtime` 的Hive表中选择数据。
3. SQL查询：查询执行了以下操作：
   - 首先，对数据进行分组（`group by produce_machine_id`），以便按 `produce_machine_id` 进行分组。
   - 使用`collect_list`函数将每个 `produce_machine_id` 的 `producetime` 收集到一个数组中，并使用`array_sort`函数对数组进行排序，以便获取有序的时间列表。
4. `.map(x => ...)`：在数据上执行一个映射操作，将数据转换为另一种形式。
   - 使用`getAs`方法从DataFrame中获取 `produce_machine_id` 和 `time_list` 列的值。
   - 将 `time_list` 转换为Scala序列（`Seq`），并提取第一个和第二个元素，分别为 `first_time` 和 `second_time`。
5. `.toDF("machine_id", "first_time", "second_time")`：将映射后的数据转换为DataFrame，并为列指定新的列名。
6. `.write.mode("append").jdbc("jdbc:clickhouse://master:8123", "shtd_industry.machine_produce_timetop2", properties)`：这一行代码将处理后的结果以追加模式写入ClickHouse数据库的 "shtd_industry.machine_produce_timetop2" 表中，使用之前定义的连接属性 `properties` 和数据库连接URL "jdbc:clickhouse://master:8123"。

### 题目六

**编写Hive SQL代码，根据dwd层dwd.fact_environment_data表，统计检测设备（BaseID）每月的平均湿度（Humidity），然后将每个设备的每月平均湿度与厂内所有检测设备每月检测结果的平均湿度做比较（结果值为：高/低/相同）存入MySQL数据库shtd_industry的表machine_humidityAVG_compare（表结构如下）中**

| 字段名         | 类型    | 中文含义           | 备注       |
| -------------- | ------- | ------------------ | ---------- |
| base_id        | int     | 检测设备ID         |            |
| machine_avg    | varchar | 单设备检测平均值   |            |
| factory_avg    | varchar | 厂内所有设备平均值 |            |
| comparison     | varchar | 比较结果           | 高/低/相同 |
| env_date_year  | varchar | 检测年份           | 如：2021   |
| env_date_month | varchar | 检测月份           | 如：12     |

### 思路分析

- 编写Hive SQL代码，使用HIVE CLI编写
- 统计检测设备（BaseID）每月的平均湿度（Humidity）按照设备ID、月份分组对湿度求平均值
- 然后将每个设备的每月平均湿度与厂内所有检测设备每月检测结果的平均湿度做比较（结果值为：高/低/相同） 每个设备的每月平均湿度(按设备分组) 厂内所有检测设备每月检测结果的平均湿度(不按设备分组)
- 比较（结果值为：高/低/相同）使用case when 或者 if函数

```sql
select EnvoId,factory_avg,machine_avg,
       case
           when machine_avg>factory_avg then "高"
           when machine_avg<factory_avg then "低"
           else "相同"
       end comparison,env_date_year,env_date_month
from (select
          EnvoId,
          year(InPutTime) env_date_year,
          month(InPutTime) env_date_month,
          avg(Humidity) over(partition by year(InPutTime),month(InPutTime)) factory_avg,
          avg(Humidity) over(partition by EnvoId,year(InPutTime),month(InPutTime)) machine_avg
      from
          dwd.fact_environment_data)t1
```

### 总结

1. 主查询：这是一个SQL查询，通过以下操作生成最终结果：
   - `select` 子句选择了多个列，包括 `EnvoId`，`factory_avg`，`machine_avg`，`comparison`，`env_date_year`，和 `env_date_month`。
   - `case` 表达式生成了一个名为 `comparison` 的新列，根据条件判断 `machine_avg` 和 `factory_avg` 的关系。如果 `machine_avg` 大于 `factory_avg`，则结果是 "高"；如果 `machine_avg`小于 `factory_avg`，则结果是 "低"；否则结果是 "相同"。
   - `EnvoId`、`env_date_year` 和 `env_date_month` 列从子查询中选择。
2. 子查询：这个子查询执行了以下操作：
   - 从名为 `dwd.fact_environment_data` 的数据表中选择数据。
   - 使用 `year(InPutTime)` 和 `month(InPutTime)` 函数来提取输入时间 `InPutTime` 中的年份和月份，生成 `env_date_year` 和 `env_date_month` 列。
   - 使用窗口函数 `avg`，在每个分区中计算湿度 `Humidity` 的平均值。首先，使用 `avg(Humidity) over(partition by year(InPutTime), month(InPutTime))` 计算每个月的厂内所有设备的平均湿度，生成 `factory_avg` 列。然后，使用 `avg(Humidity) over(partition by EnvoId, year(InPutTime), month(InPutTime))` 计算每台设备在每个月的平均湿度，生成 `machine_avg` 列。

### 题目七

**编写Scala代码，使用Spark根据dwd层fact_change_record表和dim_machine表统计，计算每个车间设备的月平均运行时长与所有设备的月平均运行时长对比结果（即设备状态为“运行”，结果值为：高/低/相同），月份取值使用状态开始时间的月份，若某设备的运行状态当前未结束（即change_end_time值为空）则该状态不参与计算。计算结果存入clickhouse数据库shtd_store的表machine_running_compare（表结构如下）中**

| 字段名          | 类型   | 中文含义         | 备注        |
| --------------- | ------ | ---------------- | ----------- |
| start_month     | String | 月份             | 如：2021-12 |
| machine_factory | int    | 车间号           |             |
| comparison      | String | 比较结果         | 高/低/相同  |
| factory_avg     | String | 车间平均时长     |             |
| company_avg     | String | 所有设备平均时长 |             |

### 思路分析

同上

### 核心代码实现

```scala
    spark.sql(
      """
        |SELECT
        |    start_month,
        |    machine_factory,
        |    CASE
        |        WHEN factory_avg > company_avg THEN "高"
        |        WHEN factory_avg < company_avg THEN "低"
        |        ELSE "相同"
        |        END AS comparison,factory_avg,company_avg
        |from (SELECT
        |          start_month,
        |          MachineFactory machine_factory,
        |          factory_avg,
        |          AVG(factory_avg) OVER () AS company_avg
        |      FROM (
        |               SELECT
        |                   MachineFactory,
        |                   AVG(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) AS factory_avg,
        |                   concat(year(ChangeStartTime), "-", month(ChangeStartTime)) AS start_month
        |               FROM dwd.fact_change_record
        |                        JOIN dwd.dim_machine ON BaseMachineID = ChangeMachineID
        |               WHERE ChangeRecordState = "运行"  and ChangeEndTime is not null
        |               GROUP BY MachineFactory, start_month
        |           ) t1)t2
        |""".stripMargin)
```

### 总结

1. - `SELECT` 子句选择了五个列，包括 `start_month`，`machine_factory`，`comparison`，`factory_avg`，和 `company_avg`。
   - `CASE` 表达式用于根据 `factory_avg` 和 `company_avg` 列的比较结果生成 `comparison` 列。如果 `factory_avg` 大于 `company_avg`，则结果是 "高"；如果 `factory_avg` 小于 `company_avg`，则结果是 "低"；否则结果是 "相同"。
2. 子查询：在主查询中的子查询中执行以下操作：
   - 首先，从名为 `dwd.fact_change_record` 的数据表中选择数据，并将其与 `dwd.dim_machine` 表连接，使用 `BaseMachineID` 和 `ChangeMachineID` 进行连接。
   - 在连接的数据上，计算每个设备的运行时长，使用 `AVG(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) AS factory_avg` 计算每个设备的平均运行时长。
   - 使用 `concat(year(ChangeStartTime), "-", month(ChangeStartTime)) AS start_month` 生成每个设备的运行数据的月份。
   - 通过 `WHERE` 子句过滤只保留 "运行" 状态且 `ChangeEndTime` 不为null的记录。
   - 使用 `GROUP BY` 子句将数据按车间号（`MachineFactory`）和月份（`start_month`）分组。
3. 主查询中的外部子查询：在外部子查询中执行以下操作：
   - 从内部子查询中选择数据，并对数据进行进一步处理。
   - 在外部子查询中计算了整个公司所有设备的平均运行时长 `company_avg`，使用 `AVG(factory_avg) OVER ()` 计算整个公司的平均值。

### 题目八

**编写Scala代码，使用Spark根据dwd层fact_change_record表展示每一个设备最近第二次的状态（倒数第二次），时间字段选用change_start_time，如果设备仅有一种状态，返回该状态（一个设备不会同时拥有两种状态），存入clickhouse数据库shtd_store的表recent_state（表结构如下）中**

### 思路分析

- 使用Scala+Spark  即可以使用sparkSql或SparkDataframe API 进行处理

- 思路：

  - 思路1：展示每一个设备最近第二次的状态（倒数第二次）将列row_num,ChangeRecordState,ChangeStartTime,ChangeEndTime同过collect_list列转行，使用array_sort通过开窗之后大的行号排序，通过datafrmaeapi获取第二个
  - 思路2(未实现代码):对对数据进行分组排序后使用collect_set列转行，如果长度为1则该状态，否则通过split取第二个

  

```scala
    import spark.implicits._
    spark.sql(
      """
        |select
        |    ChangeMachineID,
        |    array_sort(collect_list(concat_ws("#",row_num,ChangeRecordState,ChangeStartTime,ChangeEndTime))) row_status
        |from
        |(select
        |     ChangeMachineID,
        |     ChangeStartTime,
        |     ChangeEndTime,
        |     ChangeRecordState,
        |     row_number() over(partition by ChangeMachineID order by ChangeStartTime) row_num
        | from
        |     dwd.fact_change_record)t1
        | group by ChangeMachineID
        |""".stripMargin).map(x=>{
      val tuples = x.getAs[Seq[String]]("row_status")
      val data = tuples(tuples.length - 2).split("#")
        (x.getAs[Int]("ChangeMachineID"),data(1),data(2),data(3))
    }).toDF("machine_id","record_state","change_start_time","change_end_time").show(2000)
```

### 总结

1. `import spark.implicits._`：导入Spark的隐式转换，以便可以使用DataFrame的各种方法。
2. `spark.sql(...)`：这是一个Spark SQL查询，用于从名为 `dwd.fact_change_record` 的Hive表中选择数据。
3. SQL查询：查询执行了以下操作：
   - 首先，选择了两列，包括 `ChangeMachineID` 和 `row_status`。`row_status` 列是一个数组，包含多个元素，每个元素都包含了一条记录的信息，如行号、状态、开始时间和结束时间，这些信息使用 `#` 字符串连接起来。
   - 使用窗口函数 `row_number()`，在每个 `ChangeMachineID` 分区中，按照 `ChangeStartTime` 的升序顺序为记录分配一个行号 `row_num`。
   - 最后，对结果进行分组，按照 `ChangeMachineID` 进行分组。
4. `.map(x => {...})`：在结果数据上执行一个映射操作，将数据从数组中提取出，并根据格式转换为具体的列。
   - 使用 `getAs[Seq[String]]("row_status")` 获取 `row_status` 列的值，将其转换为包含多个字符串的序列。
   - 从序列中获取倒数第二个元素，这是包含记录信息的字符串，然后使用 `split("#")` 方法将其拆分为不同的字段。
   - 将这些字段转换为具体的列，包括 `machine_id`，`record_state`，`change_start_time`，和 `change_end_time`。
5. `.toDF("machine_id", "record_state", "change_start_time", "change_end_time").show(2000)`：将映射后的数据转换为DataFrame，并指定列名，然后使用 `.show(2000)` 方法显示结果。这将以表格形式显示前2000行数据。

### **题目九**

**编写Scala代码，使用Spark根据dwd层dwd.fact_environment_data表，统计检测设备（baseid）每月的PM10的检测平均浓度，然后将每个设备的每月平均浓度与厂内所有检测设备每月检测结果的平均浓度做比较（结果值为：高/低/相同）。计算结果存入MySQL数据库shtd_industry的表machine_runningAVG_compare（表结构如下）中**

| 字段名         | 类型    | 中文含义           | 备注       |
| -------------- | ------- | ------------------ | ---------- |
| base_id        | int     | 检测设备ID         |            |
| machine_avg    | varchar | 单设备检测平均值   |            |
| factory_avg    | varchar | 厂内所有设备平均值 |            |
| comparison     | varchar | 比较结果           | 高/低/相同 |
| env_date_year  | varchar | 检测年份           | 如：2021   |
| env_date_month | varchar | 检测月份           | 如：12     |

### 思路分析

- 统计检测设备（baseid）每月的PM10的检测平均浓度 按设备id、月份分组求平均值
- 将每个设备的每月平均浓度与厂内所有检测设备每月检测结果的平均浓度做比较（结果值为：高/低/相同）同题目六

```scala
    spark.sql(
      """
        |select BaseID,machine_avg,factory_avg,
        |       case when
        |                machine_avg>factory_avg then "高"
        |            when machine_avg<factory_avg then "低"
        |            end comparison,env_date_year,env_date_month
        |from
        |    (select
        |         BaseID,env_date,machine_avg,
        |         avg(machine_avg) over() factory_avg,year(env_date) env_date_year,
        |         month(env_date) env_date_month
        |     from (select
        |         BaseID,substring(InPutTime,0,7) as env_date,
        |         avg(PM10) as machine_avg
        |         from
        |         dwd.fact_environment_data
        |         group by BaseID,env_date)t1)t2
        |""".stripMargin).write.mode("overwrite").jdbc(jdbcUrl,"machine_runningAVG_compare",properties)
```

### 总结

1. `spark.sql(...)`：这是一个Spark SQL查询，用于从名为 `dwd.fact_environment_data` 的Hive表中选择数据。
2. SQL查询：查询执行了以下操作：
   - 在主查询中，选择了六列，包括 `BaseID`，`machine_avg`，`factory_avg`，`comparison`，`env_date_year`，和 `env_date_month`。
   - 使用 `CASE` 表达式根据条件判断 `machine_avg` 与 `factory_avg` 的关系，生成 `comparison` 列。如果 `machine_avg` 大于 `factory_avg`，则结果是 "高"；如果 `machine_avg` 小于 `factory_avg`，则结果是 "低"。
   - 在子查询中，首先选择了三列，包括 `BaseID`，`env_date` 和 `machine_avg`。`env_date` 表示环境日期，是从 `InPutTime` 列中提取的年月份信息。
   - 使用 `avg(PM10)` 计算了每个设备的平均值 `machine_avg`。
   - 使用 `GROUP BY` 子句将数据按 `BaseID` 和 `env_date` 分组，以计算每个设备在每个月的平均值。
   - 在外部子查询中，计算了整个工厂所有设备的平均值 `factory_avg`，使用 `avg(machine_avg) over()` 计算整个工厂的平均值。
   - 提取了 `env_date` 的年份和月份，分别存储在 `env_date_year` 和 `env_date_month` 列中。
3. `.write.mode("overwrite").jdbc(jdbcUrl, "machine_runningAVG_compare", properties)`：将结果以覆盖模式写入数据库表 "machine_runningAVG_compare" 中，使用之前定义的数据库连接URL `jdbcUrl` 和连接属性 `properties`。

### 题目十

**编写scala代码，使用Spark根据dwd层dwd.fact_machine_data表统计出每日每台设备，状态为“运行”的时长（若运行无结束时间，则需根据时间判断这个设备的运行状态的下一个状态是哪条数据，将下一条数据的状态开始的时间置为这个设备运行状态的结束时间。如果某个设备数据的运行状态不存在下一个状态，则该设备数据的运行状态不参与计算，即该设备的这条数据的运行状态时长按0计算），将结果数据写入dws层表machine_data_total_time**

| 字段                | 类型   | 中文含义       | 备注           |
| ------------------- | ------ | -------------- | -------------- |
| machine_id          | int    | 设备id         |                |
| machine_record_date | string | 状态日期       | 如：2021-10-01 |
| total_time          | int    | 一天运行总时长 | 秒             |

### 思路分析

- 使用Scala+Spark  即可以使用sparkSql或SparkDataframe API 进行处理

- 统计出每日每台设备，状态为“运行”的时长 按日(date_format(start_time,"yyyy-MM-dd"))、设备ID

- 使用lead得到+over开窗按天和设备ID进行分区，如果lead()的作用是将某列上移，如果有下个状态，那么之后筛选运行状态是该分区不会有null，如果没有下个状态就会有null值，再开窗一列使用lead之后的时间减去lead之前的时间，的到该设备该记录的运行时间

- 如果某个设备数据的运行状态不存在下一个状态，则该设备数据的运行状态不参与计算，即该设备的这条数据的运行状态时长按0计算 ：再查询状态为运行并且lead之后有空值的数据unio上没有空值的数据

### 核心代码实现

```scala
    spark.sql(
      """
        |select
        |    substring(MachineRecordDate,0,10) as machine_record_date,
        |    MachineRecordID,MachineID as machine_id,MachineRecordState,MachineRecordDate,
        |    lead(MachineRecordDate) over(partition by substring(MachineRecordDate,0,10),MachineID order by MachineRecordDate) dt,
        |    unix_timestamp(lead(MachineRecordDate) over(partition by substring(MachineRecordDate,0,10),MachineID order by MachineRecordDate))-unix_timestamp(MachineRecordDate) period
        |from
        |    dwd.fact_machine_data
        |""".stripMargin)
      .createTempView("temp")
```

1. `spark.sql(...)`：这是一个Spark SQL查询，用于从名为 `dwd.fact_machine_data` 的Hive表中选择数据。
2. SQL查询：查询执行了以下操作：
   - `substring(MachineRecordDate,0,10) as machine_record_date`：从 `MachineRecordDate` 列中提取日期部分，并将其存储在名为 `machine_record_date` 的列中，只包括年、月、日。
   - `MachineRecordID`：选择了 `MachineRecordID` 列，表示机器记录的唯一标识符。
   - `MachineID as machine_id`：选择了 `MachineID` 列，并将其重命名为 `machine_id`，表示设备的唯一标识符。
   - `MachineRecordState`：选择了 `MachineRecordState` 列，表示机器记录的状态。
   - `MachineRecordDate`：选择了 `MachineRecordDate` 列，表示机器记录的日期和时间。
   - 使用窗口函数 `lead` 计算了以下两列：
     - `lead(MachineRecordDate) over(partition by substring(MachineRecordDate,0,10), MachineID order by MachineRecordDate) dt`：这一列表示每个设备在每个日期的下一次记录的日期和时间。
     - `unix_timestamp(lead(MachineRecordDate) over(partition by substring(MachineRecordDate,0,10), MachineID order by MachineRecordDate)) - unix_timestamp(MachineRecordDate) period`：这一列计算了相邻记录之间的时间差，以秒为单位。
3. `.createTempView("temp")`：将查询结果创建为一个临时视图，命名为 "temp"，以便在后续的Spark操作中可以引用这个临时视图。

```scala
        spark.sql(
      """
        |select
        |machine_id,
        |machine_record_date,
        |cast(sum(period) as int) total_time
        |from temp
        |where MachineRecordState="运行" and dt is not null
        |group by machine_record_date,machine_id
        |""".stripMargin).union(
      spark.sql(
        """
          |select
          |machine_id,
          |machine_record_date,
          |0 total_time
          |from temp
          |where MachineRecordState="运行" and dt is null
          |group by machine_record_date,machine_id
          |""".stripMargin)
      ).write
      .mode("overwrite")
      .saveAsTable("dws.machine_data_total_time")
```

### 总结

1. `spark.sql(...)`：这是两个Spark SQL查询，它们被联合在一起并执行。
2. 第一个SQL查询：这个查询执行以下操作：
   - 选择了三列：`machine_id`，`machine_record_date`，和 `cast(sum(period) as int) total_time`。`machine_id` 表示设备的唯一标识符，`machine_record_date` 表示机器记录的日期，`total_time` 表示机器记录状态为 "运行" 的记录的总时间。
   - `from temp` 表示数据来自之前创建的临时视图 "temp"。
   - 使用 `WHERE` 子句筛选出状态为 "运行"（`MachineRecordState="运行"）的记录，并且下一次记录的日期不为空（`dt is not null`）。
   - 使用 `GROUP BY` 子句按照 `machine_record_date` 和 `machine_id` 分组，以计算每个设备在每个日期的总运行时间。
3. 第二个SQL查询：这个查询执行了以下操作：
   - 选择了三列：`machine_id`，`machine_record_date`，和 `0 total_time`。`machine_id` 表示设备的唯一标识符，`machine_record_date` 表示机器记录的日期，`total_time` 表示状态为 "运行" 且下一次记录的日期为空的记录的总时间，这里设置为0。
   - `from temp` 表示数据来自之前创建的临时视图 "temp"。
   - 使用 `WHERE` 子句筛选出状态为 "运行"（`MachineRecordState="运行"）的记录，并且下一次记录的日期为空（`dt is null`）。
   - 使用 `GROUP BY` 子句按照 `machine_record_date` 和 `machine_id` 分组，以计算每个设备在每个日期的总运行时间，这里设置为0。
4. `.union(...)`：将两个SQL查询的结果合并为一个结果集。
5. `.write.mode("overwrite").saveAsTable("dws.machine_data_total_time")`：将最终结果写入名为 "dws.machine_data_total_time" 的表中，使用覆盖模式（"overwrite"），以确保数据表的内容被替换为新的结果。

### 题目十一

**编写scala代码，使用Spark根据dws层表machine_data_total_time，计算每日运行时长前三的设备（若存在运行时长相同的数据时应全部输出，例如有两条并列第二，则第三名次不变，总共输出四条数据）。将计算结果写入clickhouse的machine_data_total_time_top3表（表结构如下）中。**

| 字段名        | 类型   | 中文含义   | 备注               |
| ------------- | ------ | ---------- | ------------------ |
| day           | String | 日期       | 如：2021-10-01     |
| first_id      | int    | 第一       | 一天运行总时长第一 |
| second_id     | int    | 第二       | 一天运行总时长第二 |
| tertiary_id   | int    | 第三       | 一天运行总时长第三 |
| first_time    | int    | 第一的时长 | 秒                 |
| second_time   | int    | 第二的时长 | 秒                 |
| tertiary_time | int    | 第三的时长 | 秒                 |

### 思路分析

- 使用Scala+Spark  即可以使用sparkSql或SparkDataframe API 进行处理
- 计算每日运行时长前三的设备（若存在运行时长相同的数据时应全部输出，例如有两条并列第二，则第三名次不变，总共输出四条数据）使用dense_rank()+rank()开窗 对日期进行machine_record_date分区，对总运行时间total_time进行排序，获取的到排名，再使用数据透视表进行对排名rank为123的列进行转换

### 核心代码实现

```scala
    spark.sql(
      """
        |select
        |   rank,machine_id,
        |   total_time,
        |   day
        |from (select
        |       machine_id,
        |       total_time,
        |       machine_record_date day,
        |       dense_rank() over(partition by machine_record_date order by total_time desc) rank
        |from
        |dws.machine_data_total_time)t1
        |""".stripMargin).createTempView("t")
```

```scala
    spark.sql(
      """
        |SELECT *
        |  FROM t
        | PIVOT (
        |    SUM(machine_id) id,sum(total_time) as time
        |    FOR rank IN (1 as first, 2 as second, 3 as tertiary)
        |)  |""".stripMargin).select("day","first_id","second_id","tertiary_id","first_time","second_time","tertiary_time")
      .toDF().write   .mode("append").jdbc("jdbc:clickhouse://master:8123/shtd_store","machine_data_total_time_top3",properties)
```

### 总结

1. `spark.sql(...)`：这是一个Spark SQL查询，用于对数据执行透视操作。
2. SQL查询：查询执行了以下操作：
   - `SELECT * FROM t`：从名为 "t" 的表中选择所有列和数据。
   - `PIVOT` 操作：这是一个透视操作，用于将数据从长格式转换为宽格式。在透视操作中，数据按照 `machine_id` 列的值进行分组，然后计算每个分组的总和 `SUM(machine_id)` 和 `SUM(total_time)`。透视操作使用 `rank` 列进行分类，将不同的 `rank` 值作为新的列进行展示，包括 "first"、"second" 和 "tertiary"。
3. `.select("day", "first_id", "second_id", "tertiary_id", "first_time", "second_time", "tertiary_time")`：从透视后的数据中选择指定的列，包括 "day"，"first_id"，"second_id"，"tertiary_id"，"first_time"，"second_time" 和 "tertiary_time"。
4. `.toDF()`：将选定的列转化为DataFrame格式，以便后续的写入操作。
5. `.write`：开始数据写入操作，但代码中未提供完整的写入配置和目标。

### 题目十二

**根据dwd层的fact_change_record表，计算状态开始时间为2021年10月12日凌晨0点0分0秒且状态为“待机”的数据，计算当天每小时新增的“待机”状态时长与当天“待机”状态总时长，将结果存入MySQL数据库shtd_industry的accumulate_standby表中（表结构如下）**

| 字段             | 类型    | 中文含义                             | 备注                                         |
| ---------------- | ------- | ------------------------------------ | -------------------------------------------- |
| start_date       | varchar | 状态开始日期                         | yyyy-MM-dd                                   |
| start_hour       | varchar | 状态开始时间                         | 存当前时间的小时整数，如：1点存1、  13点存13 |
| hour_add_standby | int     | 该小时新增“待机”状态时长             |                                              |
| day_agg_standby  | int     | 截止到这个小时，当天“待机”状态总时长 |                                              |

### 思路分析

- 计算状态开始时间为2021年10月12日凌晨0点0分0秒且状态为“待机”的数据   过滤2021-10-12 00:00:00到2021-10-13 00:00:00并且changerecordstate=待机的数据
- 计算当天每小时新增的“待机”状态时长 新增的时长就是直接sum(该小时的数据unix_timestamp(最后时间)-unix_timestamp(开始时间))
- 截止到这个小时，当天“待机”状态总时时间 sum(unix_timestamp(最后时间)-unix_timestamp(开始时间)) 再over 按小时排序

### 核心代码实现

```scala
    spark.sql(
      """
        |select
        |    start_date,
        |    start_hour,
        |    run_time hour_add_standby,
        |    sum(run_time) over(order by start_hour) day_agg_standby
        |from (select
        |          first(to_date(ChangeStartTime,"yyyy-MM-dd")) start_date,hour(ChangeStartTime) start_hour,
        |          sum(unix_timestamp(ChangeEndTime)-unix_timestamp(ChangeStartTime)) run_time
        |      from
        |          dwd.fact_change_record
        |      where ChangeStartTime between "2021-10-12 00:00:00" and "2021-10-13 00:00:00" and ChangeRecordState="待机"
        |      group by start_hour)t1
        |""".stripMargin)
      .write.mode("overwrite")
```

1. `spark.sql(...)`：这是一个Spark SQL查询，用于选择和处理数据。
2. SQL查询：查询执行了以下操作：
   - `select start_date, start_hour, run_time hour_add_standby, sum(run_time) over (order by start_hour) day_agg_standby`：选择了四列，包括 `start_date`，`start_hour`，`hour_add_standby`，和 `day_agg_standby`。其中：
     - `start_date` 表示日期。
     - `start_hour` 表示时间的小时部分。
     - `hour_add_standby` 表示运行时间（以秒为单位），这里的运行状态是 "待机"。
     - `day_agg_standby` 表示在 `start_hour` 列的基础上，累积计算每个小时的 `hour_add_standby` 值，以便在每个小时之间创建一个累积值。
   - `from` 子句中的子查询：这个子查询执行了以下操作：
     - `first(to_date(ChangeStartTime, "yyyy-MM-dd")) start_date`：从 `ChangeStartTime` 中提取日期部分，并将其存储在 `start_date` 列中，表示日期。
     - `hour(ChangeStartTime) start_hour`：从 `ChangeStartTime` 中提取小时部分，并将其存储在 `start_hour` 列中，表示小时。
     - `sum(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) run_time`：计算每个记录的运行时间，以秒为单位。
     - `where` 子句用于筛选出满足条件的记录，包括日期范围和状态为 "待机" 的记录。
     - `group by start_hour`：按照 `start_hour` 列的值对数据进行分组，以便在每个小时内对运行时间进行汇总。
3. `.write.mode("overwrite")`：将查询结果写入某个目标，使用覆盖模式（"overwrite"），以确保数据表的内容被替换为新的结果。

### 题目十三

**根据dwd层的fact_change_record表，计算状态开始时间为2021年10月13日凌晨0点0分0秒且状态为“运行”的数据，以3小时为一个计算窗口，做滑动窗口计算，滑动步长为1小时，窗口不满3小时不触发计算（即从凌晨3点开始触发计算，第一条数据的state_start_time为2021-10-13_02，以有数据的3个小时为一个时间窗口），将计算结果存入MySQL数据库shtd_industry的slide_window_runnning表中**

| 字段             | 类型    | 中文含义           | 备注          |
| ---------------- | ------- | ------------------ | ------------- |
| state_start_time | varchar | 运行状态开始时间   | yyyy-MM-dd_HH |
| window_sum       | int     | 3小时运行总时长    | 单位：秒      |
| window_avg       | int     | 每小时平均运行时长 | 基于所在窗口  |

### 思路分析

- 计算状态开始时间为2021年10月13日凌晨0点0分0秒且状态为“运行”的数据     过滤2021-10-13 00:00:00 -2021-10-14 00:00:00并且状态为运行的数据

- 以3小时为一个计算窗口，做滑动窗口计算，滑动步长为1小时，窗口不满3小时不触发计算（即从凌晨3点开始触发计算，第一条数据的state_start_time为2021-10-13_02，以有数据的3个小时为一个时间窗口）  先分组求出每个小时的运行时长

- 使用rows BETWEEN 2 PRECEDING AND CURRENT ROW 开当前到前三个小时窗口，按小时进行排序

- 第一条数据的state_start_time为2021-10-13_02 使用row_number获取行号过滤掉前两行

### 核心代码实现

```scala
    spark.sql(
      """
        |select concat(start_date_ymd,"_",start_hour) state_start_time,
        |       window_sum,window_avg
        |from (select start_date_ymd,
        |             start_hour,
        |             run_time,
        |             sum(run_time) over(order by start_hour rows BETWEEN 2 PRECEDING AND CURRENT ROW) window_sum,
        |             avg(run_time) over(order by start_hour rows BETWEEN 2 PRECEDING AND CURRENT ROW) window_avg,
        |             row_number() over(order by start_hour) row_index
        |      from (select
        |                first (to_date(ChangeStartTime, "yyyy-MM-dd")) start_date_ymd,
        |                date_format (ChangeStartTime,"HH") start_hour,
        |                sum (unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) run_time
        |            from
        |                dwd.fact_change_record
        |            where ChangeStartTime between "2021-10-13 00:00:00" and "2021-10-14 00:00:00" and ChangeRecordState="运行"
        |            group by start_hour sort by start_hour) t1)t2
        |where row_index>2
        |""".stripMargin).write.mode("overwrite")
      .jdbc("jdbc:mysql://localhost:3306/shtd_industry","slide_window_runnning",properties)
```

### 总结

1. `spark.sql(...)`：这是一个Spark SQL查询，用于选择和处理数据。
2. SQL查询：查询执行了以下操作：
   - `select concat(start_date_ymd, "_", start_hour) state_start_time, window_sum, window_avg`：选择了三列，包括 `state_start_time`、`window_sum` 和 `window_avg`。其中：
     - `state_start_time` 是一个字符串，由 `start_date_ymd` 和 `start_hour` 拼接而成，表示日期和小时。
     - `window_sum` 表示在窗口中（窗口大小为3，包括当前行和前两行）的运行时间的总和。
     - `window_avg` 表示在窗口中的运行时间的平均值。
   - `from` 子句中的子查询：这个子查询执行了以下操作：
     - `start_date_ymd` 是从 `ChangeStartTime` 中提取日期部分的值。
     - `start_hour` 是从 `ChangeStartTime` 中提取小时部分的值。
     - `run_time` 是每条记录的运行时间（以秒为单位）。
     - `sum(run_time) over (order by start_hour rows BETWEEN 2 PRECEDING AND CURRENT ROW)` 计算了在窗口中的运行时间的总和，窗口的大小是3行，包括当前行和前两行。
     - `avg(run_time) over (order by start_hour rows BETWEEN 2 PRECEDING AND CURRENT ROW)` 计算了在窗口中的运行时间的平均值，窗口的大小是3行，包括当前行和前两行。
     - `row_number() over (order by start_hour)` 为每行分配一个行号。
3. `where row_index > 2`：筛选出行号大于2的行，因为窗口计算需要至少3行数据。
4. `.write.mode("overwrite")`：将查询结果写入到MySQL数据库，使用覆盖模式（"overwrite"）。
5. `.jdbc("jdbc:mysql://localhost:3306/shtd_industry", "slide_window_runnning", properties)`：将结果写入到名为 "slide_window_runnning" 的MySQL数据库表中。

# 数据挖掘

## 准备数据

### 定义UDF随机生成数据

```scala
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
```

## 特征工程

**1、   根据dwd库中fact_machine_data表（或MySQL的shtd_industry库中MachineData表），根据以下要求转换：获取最大分区（MySQL不用考虑）的数据后，首先解析列machine_record_data（MySQL中为MachineRecordData）的数据（数据格式为xml，采用dom4j解析，会给出解析demo），并获取每条数据的主轴转速，主轴倍率，主轴负载，进给倍率，进给速度，PMC程序号，循环时间，运行时间，有效轴数，总加工个数，已使用内存，未使用内存，可用程序量，注册程序量等相关的值（若该条数据没有相关值，则按下表设置默认值），同时转换machine_record_state字段的值，若值为报警，则填写1，否则填写0**

**dwd.fact_machine_learning_data表结构：**

| 字段                                  | 类型      | 中文含义   | 备注    |
| ------------------------------------- | --------- | ---------- | ------- |
| machine_record_id                     | int       | 自增长id   |         |
| machine_id                            | double    | 机器id     |         |
| machine_record_state                  | double    | 机器状态   | 默认0.0 |
| machine_record_mainshaft_speed        | double    | 主轴转速   | 默认0.0 |
| machine_record_mainshaft_multiplerate | double    | 主轴倍率   | 默认0.0 |
| machine_record_mainshaft_load         | double    | 主轴负载   | 默认0.0 |
| machine_record_feed_speed             | double    | 进给倍率   | 默认0.0 |
| machine_record_feed_multiplerate      | double    | 进给速度   | 默认0.0 |
| machine_record_pmc_code               | double    | PMC程序号  | 默认0.0 |
| machine_record_circle_time            | double    | 循环时间   | 默认0.0 |
| machine_record_run_time               | double    | 运行时间   | 默认0.0 |
| machine_record_effective_shaft        | double    | 有效轴数   | 默认0.0 |
| machine_record_amount_process         | double    | 总加工个数 | 默认0.0 |
| machine_record_use_memory             | double    | 已使用内存 | 默认0.0 |
| machine_record_free_memory            | double    | 未使用内存 | 默认0.0 |
| machine_record_amount_use_code        | double    | 可用程序量 | 默认0.0 |
| machine_record_amount_free_code       | double    | 注册程序量 | 默认0.0 |
| machine_record_date                   | timestamp | 记录日期   |         |
| dwd_insert_user                       | string    |            |         |
| dwd_insert_time                       | timestamp |            |         |
| dwd_modify_user                       | string    |            |         |
| dwd_modify_time                       | timestamp |            |         |

### 思路分析

- 定义表样例类，使用 document.getRootElement.elements()获取元素集合，然后map取出数据调用getText方法，封装到样例类中再toDF
- 同时转换machine_record_state字段的值，若值为报警，则填写1，否则填写0  写个if判断eq(报警)的返回1否则0

### 核心代码实现

```scala
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
```

### 总结

1. `import spark.implicits._`：导入了Spark的隐式转换，允许使用DataFrame和Dataset的操作。
2. `spark.table("dwd.fact_machine_data_pre")`：从名为 "dwd.fact_machine_data_pre" 的表中获取数据，这是一个预处理的机器数据表。
3. `.map(row => {...})`：对每行数据执行自定义的映射操作。
4. 映射操作：在映射操作中，对每一行数据执行以下操作：
   - 从行数据中提取各种字段，例如 `machinerecordid`、`machineid`、`machinerecordstate` 等。
   - 根据 `machinerecordstate` 的值（是否为 "报警"）设置 `machine_record_state` 的值为 1 或 0.0。
   - 通过解析 XML 格式的 `machine_record_data`，提取出其中的数据并转换为数组。
   - 创建一个名为 `MachineData` 的样例类的实例，将提取的字段和数组的元素分配给样例类的字段。
   - 最后，将样例类实例作为结果，包含了从行数据提取的各种字段和数组的元素。
5. `.write.mode("overwrite")`：将转换后的数据写入表中。
6. `.saveAsTable("dwd.fact_machine_learning_data")`：将结果保存为名为 "dwd.fact_machine_learning_data" 的表，用于进一步的数据分析和处理

## 报警预测

  **根据任务一的结果，建立随机森林（随机森林相关参数可自定义，不做限制），使用任务一的结果训练随机森林模型，然后再将hive中dwd.fact_machine_learning_data_test（该表字段含义与dwd.fact_machine_learning_data表相同，machine_record_state列值为空，表结构自行查看）转成向量，预测其是否报警将结果输出到MySQL库shtd_industry中的ml_result表中。**

ml_result表结构：

| 字段                 | 类型   | 中文含义 | 备注                   |
| -------------------- | ------ | -------- | ---------------------- |
| machine_record_id    | int    | 主键     |                        |
| machine_record_state | double | 设备状态 | 报警为1，其他状态则为0 |

### 思路分析

- 转成向量   使用StringIndex+oneHot将表转为向量，因为数据是随机生成，不标准，如果有string类型的使用StringIndex转，其他类型先转String
- 应为数据是随机生成，使用随机森林算法会遇到特征列的唯一值过大，可以进行数据分箱或者删掉,想日期字段可以进行手动降维，例如将yyyy-MM-dd HH:mm:ss 变为 yyyy-MM-dd,也可以
- 根据题意，数据应该是改的比较简单，建立随机森林训练就行，将预测的结果通过if转为报警插入test表中

也可以通过代码查看唯一值，并设置maxbins为最大唯一值

```scala
// 遍历每个分类特征列，计算唯一值数量
for (colName <- featureCols) {
  val uniqueValuesCount = oneHotEncodedData.select(colName).distinct().count()
  var t=(colName,uniqueValuesCount)
  featureANs=featureANs :+t
}
println(featureANs,"------------------------------------------------")
```

### 核心代码实现

```scala
// 创建或获取一个名为 "AlarmPrediction" 的 Spark 会话
val spark = SparkSession.builder().appName("AlarmPrediction").enableHiveSupport().getOrCreate()

// 从名为 "dwd.fact_machine_learning_data" 的表中获取数据
val df = spark.table("dwd.fact_machine_learning_data")

// 创建一个字符串索引器，将 "machine_record_pmc_code" 列的字符串值编码为数值，并存储在 "machine_record_pmc_code_int" 列中
val indexer = new StringIndexer().setInputCols(Array("machine_record_pmc_code")).setOutputCols(Array("machine_record_pmc_code_int"))
val indexedData = indexer.fit(df).transform(df)

// 创建一个独热编码器，将 "machine_record_pmc_code_int" 列中的数值编码转换为独热编码，并存储在 "machine_record_pmc_code_int_oneHot" 列中
val encoder = new OneHotEncoder().setInputCols(Array("machine_record_pmc_code_int")).setOutputCols(Array("machine_record_pmc_code_int_oneHot"))
val oneHotEncodedData = encoder.fit(indexedData).transform(indexedData)
oneHotEncodedData.show()

// 获取特征列，排除一些不需要的列
val featureCols = oneHotEncodedData.columns.filter(x => {
  x.!=("machine_record_pmc_code") && x.!=("machine_record_date_int") && x.!=("machine_record_date_int_oneHot") &&
    x.!=("machine_record_date_unix") && x.!=("machine_record_date") && x.!=("dwd_insert_user") && x.!=("dwd_insert_time") &&
    x.!=("dwd_modify_user") && x.!=("dwd_modify_time") && x.!=("machine_record_id") && x.!=("dwd_modify_time") &&
    x.!=("machine_record_run_time") && x.!=("machine_record_feed_speed") && x.!=("machine_record_mainshaft_multiplerate")
})

// 输出特征列列表
println(featureCols.mkString(","), "特征列表--------------------------------------------")

// 初始化一个空的特征列表
var featureANs = List[(String, Long)]()

// 遍历每个分类特征列，计算唯一值的数量
for (colName <- featureCols) {
  val uniqueValuesCount = oneHotEncodedData.select(colName).distinct().count()
  var t = (colName, uniqueValuesCount)
  featureANs = featureANs :+ t
}

// 打印特征列的唯一值数量
println(featureANs, "------------------------------------------------")

// 创建一个特征向量组装器，将多个特征列组合成一个单一的特征向量列 "features"
val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

// 使用特征向量组装器将特征数据转换为包含 "features" 和 "machine_record_state" 列的 DataFrame
val labeledData = assembler.transform(oneHotEncodedData).select("features", "machine_record_state")
labeledData.show()

// 随机拆分数据为训练集和测试集
val Array(train, test): Array[Dataset[Row]] = labeledData.randomSplit(Array(0.7, 0.3), 123)

// 创建一个随机森林分类器，并设置相关参数
val rf = new RandomForestClassifier().setMaxBins(3200).setLabelCol("machine_record_state").setFeaturesCol("features")

// 使用训练集训练随机森林分类模型
val model = rf.fit(train)

// 使用测试集对模型进行预测
val predictions = model.transform(test)

// 创建一个多类分类评估器，用于评估模型的性能
val accuracy = new MulticlassClassificationEvaluator().setLabelCol("machine_record_state").setPredictionCol("prediction").setMetricName("accuracy")

// 打印测试集上的模型准确率
println(s"Test set accuracy = $accuracy")

```

### 总结

1. `val df = spark.table("dwd.fact_machine_learning_data")`：从名为 "dwd.fact_machine_learning_data" 的表中获取数据，这是机器学习模型的输入数据。
2. 特征转换部分：
   - `StringIndexer`：这是一个用于将字符串特征列编码为数值的转换器。在这里，它将 "machine_record_pmc_code" 列中的字符串值编码为数值，并将结果存储在 "machine_record_pmc_code_int" 列中。
   - `OneHotEncoder`：这是一个独热编码转换器，用于将数值特征列编码为独热编码形式。在这里，它将 "machine_record_pmc_code_int" 列中的数值编码转换为独热编码，并将结果存储在 "machine_record_pmc_code_int_oneHot" 列中。
   - `featureCols`：这是一个特征列的数组，其中包含了需要用于模型训练的特征列。在这里，排除了一些不需要的列，如日期和时间戳列以及一些唯一值太多的列。
3. `assembler`：这是一个特征向量组装器，用于将多个特征列组合成一个单一的特征向量列 "features"，以便进行机器学习模型的训练。
4. `labeledData`：这是包含特征向量列 "features" 和目标列 "machine_record_state" 的DataFrame，用于模型训练。
5. 数据拆分：将数据拆分为训练集和测试集，以便进行模型的训练和评估。
6. 随机森林分类器（RandomForestClassifier）：创建一个随机森林分类模型，用于训练和预测数据。
7. 模型训练：使用训练集训练随机森林分类模型。
8. 模型预测：使用测试集对模型进行预测，生成预测结果。
9. 评估器（MulticlassClassificationEvaluator）：用于评估分类模型的性能，包括计算准确率（accuracy）等指标。
10. 打印准确率：打印测试集上的模型准确率。





### TODO代码

```scala
    val spark = SparkSession.builder().appName("AlarmPrediction").enableHiveSupport().getOrCreate()
    val df = spark.table("dwd.fact_machine_learning_data")
//      .withColumn("dwd_insert_time_unix",unix_timestamp(col("dwd_insert_time")))

    val indexer = new StringIndexer().setInputCols(
      Array("machine_record_pmc_code"))
      .setOutputCols(Array("machine_record_pmc_code_int"))

    val indexedData = indexer.fit(df).transform(df)
    val encoder = new OneHotEncoder()
      .setInputCols(Array("machine_record_pmc_code_int"))
      .setOutputCols(Array( "machine_record_pmc_code_int_oneHot"))
    val oneHotEncodedData = encoder.fit(indexedData).transform(indexedData)
    oneHotEncodedData.show()
    val featureCols  = oneHotEncodedData.columns.filter(x=>{
      x.!=("machine_record_pmc_code")&&x.!=("machine_record_date_int")&& x.!=("machine_record_date_int_oneHot")&&x.!=("machine_record_date_unix")&&x.!=("machine_record_date")&&x.!=("dwd_insert_user")&&x.!=("dwd_insert_time")&&x.!=("dwd_modify_user")&&x.!=("dwd_modify_time")&&x.!=("machine_record_id")&&x.!=("dwd_modify_time")&&x.!=("machine_record_run_time")&&x.!=("machine_record_feed_speed")&&x.!=("machine_record_mainshaft_multiplerate")
    })

    println(featureCols.mkString(","),"特征列表--------------------------------------------")
    var featureANs=List[(String,Long)]()
    // todo 需要进行分箱，特征唯一值太大
    //(List((machine_id,30), (machine_record_state,1),
    // (machine_record_mainshaft_speed,3000), (machine_record_mainshaft_load,100),
    // (machine_record_feed_multiplerate,1000), (machine_record_circle_time,300),
    // (machine_record_effective_shaft,10), (machine_record_amount_process,200),
    // (machine_record_use_memory,2048), (machine_record_free_memory,1024),
    // (machine_record_amount_use_code,20), (machine_record_amount_free_code,10),
    // (dwd_insert_time_unix,1), (dwd_modify_time_unix,1), (machine_record_date_unix,164128),
    // (machine_record_pmc_code_int,1000), (machine_record_date_int,164128), (machine_record_pmc_code_int_oneHot,1000),
    // (machine_record_date_int_oneHot,164128)),------------------------------------------------)
    // 遍历每个分类特征列，计算唯一值数量
//    for (colName <- featureCols) {
//      val uniqueValuesCount = oneHotEncodedData.select(colName).distinct().count()
//      var t=(colName,uniqueValuesCount)
//      featureANs=featureANs :+t
//    }
//    println(featureANs,"------------------------------------------------")

    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val labeledData = assembler.transform(oneHotEncodedData).select("features", "machine_record_state")
    labeledData.show()
    val Array(train,test): Array[Dataset[Row]] = labeledData.randomSplit(Array(0.7, 0.3), 123)
    val rf = new RandomForestClassifier().setMaxBins(3200).setLabelCol("machine_record_state").setFeaturesCol("features")
    val model = rf.fit(train)
    val predictions = model.transform(test)
    val accuracy = new MulticlassClassificationEvaluator().setLabelCol("machine_record_state").setPredictionCol("prediction").setMetricName("accuracy")
    println(s"Test set accuracy = $accuracy")
```
