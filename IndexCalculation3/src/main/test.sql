INSERT INTO TABLE dwd.dim_machine
SELECT BaseMachineID,
       MachineFactory,
       MachineNo,
       MachineName,
       MachineIP,
       MachinePort,
       MachineAddDate,
       MachineRemarks,
       MachineAddEmpID,
       MachineResponsEmpID,
       MachineLedgerXml,
       ISWS,
       dwd_insert_user,
       dwd_modify_user,
       dwd_insert_time,
       dwd_modify_time,
       etldate
FROM dwd.dim_machine
    INSERT
INTO `dim_machine`
VALUES (113, 787, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0, "user1", "user1", "2023-10-18 20:01:17", "2023-10-18 20:01:17", "2023-10-17");
INSERT INTO `fact_change_record`
VALUES (20260668, 113, '0', '运行', '2021-9-12 15:58:55', '2021-10-12 18:06:41',
        '<col ColName="设备IP">192.168.1.204</col><col ColName="进给速度">0</col><col ColName="急停状态">否</col><col ColName="加工状态">MDI</col><col ColName="刀片相关信息">刀片组号:0;type:0;刀片组的全部数量:6;刀片号:0;刀片组号:0;寿命计时0</col><col ColName="切削时间">518100</col><col ColName="未使用内存">1006</col><col ColName="循环时间">573760</col><col ColName="报警信息">{"AlmNo":0,"AlmStartTime":"无","AlmMsg":"无报警"}</col><col ColName="主轴转速">0</col><col ColName="上电时间">1799120</col><col ColName="总加工个数">7850</col><col ColName="班次信息">null</col><col ColName="运行时间">793215</col><col ColName="正在运行刀具信息">没有刀具在运行</col><col ColName="有效轴数">8</col><col ColName="主轴负载">0</col><col ColName="PMC程序号">0012</col><col ColName="进给倍率">100</col><col ColName="主轴倍率">100</col><col ColName="已使用内存">67</col><col ColName="可用程序量">903</col><col ColName="刀补值">0</col><col ColName="工作模式">MEMory</col><col ColName="机器状态">待机</col><col ColName="连接状态">normal</col><col ColName="加工个数">3432</col><col ColName="机床位置">["-1.34","1.219","-0.08","0","252.627","271.214","485.396","0","-1.34","1.219","-0.08","0"]</col><col ColName="注册程序量">108</col>',
        0, "user1", "user1", "2023-10-18 20:01:17", "2023-10-18 20:01:17", "2023-10-17");


select *
from (select *,
      from dwd.fact_change_record as c
               join dwd.dim_machine as d on c.ChangeMachineID = d.BaseMachineID
      where c.ChangeRecordState = "运行"
        and ChangeEndTime is not null)
group by MachineFactory

select
    first (c.ChangeMachineID) as machine_id, d.MachineFactory machine_factory, percentile(unix_timestamp(c.ChangeEndTime)-unix_timestamp(c.ChangeStartTime), 0.5) total_running_time
from
    dwd.fact_change_record c
    join dwd.dim_machine d
on c.ChangeMachineID=d.BaseMachineID
group by d.MachineFactory


select produce_record_id,
       produce_machine_id,
       unix_timestamp(ProduceCodeEndTime) - unix_timestamp(ProduceCodeStartTime) producetime avg(producetime)
        over(partition by ProduceMachineID) produce_per_avgtime
from
    dwd.fact_produce_record
where produce_code_end_time!='1900-01-01 00:00:00'


select producetime > produce_per_avgtime
from (select ProduceRecordId                                                           produce_record_id,
             ProduceMachineId                                                          produce_machine_id,
             unix_timestamp(ProduceCodeEndTime) - unix_timestamp(ProduceCodeStartTime) producetime,
             avg(unix_timestamp(ProduceCodeEndTime) - unix_timestamp(ProduceCodeStartTime))
                                                                                       over(partition by ProduceMachineID) produce_per_avgtime
      from dwd.fact_produce_record
      where ProduceCodeEndTime!='1900-01-01 00:00:00') t1;

create table machine_produce_per_avgtime
(
    produce_record_id   int,
    produce_machine_id  int,
    producetime         int,
    produce_per_avgtime int,
    primary key (produce_record_id, produce_machine_id)
)engine=MergeTree()


select produce_machine_id, first_time, second_time
from (select produce_machine_id,
             plit(array_sort(collect_list(producetime)), ",")[1] first_time,
             plit(array_sort(collect_list(producetime)), ",")[2] second_time
      from dws.machine_produce_per_avgtime
      group by produce_machine_id) t1


select EnvoId, year (InPutTime) env_date_year, month (InPutTime) env_date_month, avg (Humidity) over(partition by year (InPutTime), month (InPutTime)) factory_avg, avg (Humidity) over(partition by EnvoId, year (InPutTime), month (InPutTime)) machine_avg,
from
    dwd.fact_environment_data



select EnvoId,
       factory_avg,
       machine_avg,
       case
           when machine_avg > factory_avg then "高"
           when machine_avg > factory_avg then "低"
           else "相同"
           end comparison,
       env_date_year,
       env_date_month
from (select EnvoId, year (InPutTime) env_date_year, month (InPutTime) env_date_month, avg (Humidity) over(partition by year (InPutTime), month (InPutTime)) factory_avg, avg (Humidity) over(partition by EnvoId, year (InPutTime), month (InPutTime)) machine_avg
      from
          dwd.fact_environment_data) t1


select start_month,
       machine_factory,
       case
           when machine_avg > factory_avg then "高"
           when machine_avg > factory_avg then "低"
           else "相同"
           end comparison,
       factory_avg,
       company_avg
from (select concat(year(ChangeStartTime)-month(ChangeStartTime)) start_month,
             MachineFactory                                       machine_factory,
             avg(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime))
                                                                  over(partition by month(ChangeStartTime))
     factory_avg, avg(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) company_avg
      from dwd.fact_change_record
               join dim_machine
                    on BaseMachineID = ChangeMachineID)

select MachineFactory,
       avg(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) runtime,
       concat(year(ChangeStartTime)-month(ChangeStartTime))                 start_month
from dwd.fact_change_record
         join dim_machine
              on BaseMachineID = ChangeMachineID
group by MachineFactory

select MachineFactory,
       avg(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) runtime,
       concat(year(ChangeStartTime),"-", month(ChangeStartTime))            start_month
from dwd.fact_change_record
         join dwd.dim_machine
              on BaseMachineID = ChangeMachineID
group by MachineFactory, start_month

select start_month,
       MachineFactory,
       factory_avg,
       case
           when factory_avg > company_avg then "高"
           when factory_avg < company_avg then "低"
           else "相同"
           end comparison,
from (select MachineFactory,
             avg(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime))      factory_avg,
             concat(year(ChangeStartTime),"-", month(ChangeStartTime))                 start_month,
             sum(avg(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime))) company_avg
      from dwd.fact_change_record
               join dwd.dim_machine
                    on BaseMachineID = ChangeMachineID and ChangeRecordState!="运行"
      group by MachineFactory, start_month) t1



select start_month,
       MachineFactory,
       case
           when factory_avg > company_avg then "高"
           when factory_avg < company_avg then "低"
           else "相同"
           end comparison,
       factory_avg,
       company_avg
from (select start_month,
             MachineFactory,
             factory_avg,
             sum(factory_avg) as company_avg
      from (select MachineFactory,
                   avg(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) factory_avg, first (concat(year (ChangeStartTime), "-", month (ChangeStartTime))) start_month
            from dwd.fact_change_record
                join dwd.dim_machine
            on BaseMachineID = ChangeMachineID and ChangeRecordState!="运行"
            group by MachineFactory, concat(year (ChangeStartTime), "-", month (ChangeStartTime))) t1) t2

select start_month,
       MachineFactory,
       case
           when factory_avg > company_avg then "高"
           when factory_avg < company_avg then "低"
           else "相同"
           end comparison,
       factory_avg,
       company_avg
from (select start_month,
             MachineFactory,
             factory_avg,
             sum(factory_avg) as company_avg
      from (select MachineFactory,
                   avg(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) factory_avg,
                   start_month
            from (select MachineFactory,
                         ChangeEndTime,
                         ChangeStartTime,
                         concat(year(ChangeStartTime),"-", month(ChangeStartTime)) start_month
                  from dwd.fact_change_record
                           join dwd.dim_machine
                                on BaseMachineID = ChangeMachineID and ChangeRecordState!="运行") t1
            group by MachineFactory, start_month) t2) t3



SELECT start_month,
       MachineFactory,
       CASE
           WHEN factory_avg > company_avg THEN "高"
           WHEN factory_avg < company_avg THEN "低"
           ELSE "相同"
           END AS comparison,
       factory_avg,
       company_avg
from (SELECT start_month,
             MachineFactory,
             factory_avg,
             AVG(factory_avg) OVER () AS company_avg
      FROM (SELECT MachineFactory,
                   AVG(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) AS factory_avg,
                   concat(year(ChangeStartTime), "-", month(ChangeStartTime))           AS start_month
            FROM dwd.fact_change_record
                     JOIN dwd.dim_machine ON BaseMachineID = ChangeMachineID
            WHERE ChangeRecordState != "运行"
            GROUP BY MachineFactory, start_month) t1) t2

select ChangeMachineID,
       ChangeStartTime,
       ChangeRecordState,
       row_number(ChangeRecordState) over(partition by ChangeMachineID order by ChangeStartTime), count(*) over(partition by ChangeMachineID,ChangeRecordState) cnt
from dwd.fact_change_record

select ChangeMachineID,
       ChangeStartTime,
       array_sort(collect_list(concat(row_num, "-", ChangeRecordState)))
from (select ChangeMachineID,
             ChangeStartTime,
             ChangeRecordState,
             row_number() over(partition by ChangeMachineID order by ChangeStartTime) row_num
      from dwd.fact_change_record) t1



select BaseID,
       machine_avg,
       factory_avg,
       case
           when
               machine_avg > factory_avg then "高"
           when machine_avg < factory_avg then "低"
           end comparison,
       env_date_year,
       env_date_month
from (select BaseID,
             env_date,
             machine_avg,
             avg(machine_avg) over() factory_avg,year(env_date) env_date_year,
         month(env_date) env_date_month
      from (select
          BaseID, substring (InPutTime, 0, 7) as env_date, avg (PM10) as machine_avg
          from
          dwd.fact_environment_data
          group by BaseID, env_date) t1) t2


select MachineID,
       to_date(MachineRecordDate, "yyyy-MM") m_date,
       MachineRecordState,
       row_number()                          over(partition by MachineID,MachineRecordState order by to_date(MachineRecordDate,"yyyy-MM")),
from dwd.fact_machine_data
group by MachineID, m_date, MachineRecordState

--        sum(*) over(partition by )

select MachineID,
       to_date(MachineRecordDate, "yyyy-MM") d_date,
       MachineRecordState,
       row_number()
                                             over(partition by MachineID,MachineID,to_date(MachineRecordDate,"yyyy-MM")
       ,MachineRecordState order by to_date(MachineRecordDate,"yyyy-MM")) row
from dwd.fact_machine_data
group by MachineID, d_date, MachineRecordState

select MachineID,
       d_date,
       MachineRecordState,
       row,
       array_sort(collect_list(concat_ws("-", row, d_date, MachineRecordState)))
from (select MachineID,
             to_date(MachineRecordDate, "yyyy-MM") d_date,
             MachineRecordState,
             row_number()
                                                   over(partition by MachineID,to_date(MachineRecordDate,"yyyy-MM")
       ,MachineRecordState order by to_date(MachineRecordDate,"yyyy-MM")) row
      from dwd.fact_machine_data
      group by MachineID, d_date, MachineRecordState) t1
group by MachineID, d_date, MachineRecordState


select to_date(MachineRecordDate, "yyyy-MM") d_date,
       MachineID,
       MachineRecordState,
       dense_rank()                          over(partition by to_date(MachineRecordDate,"yyyy-MM"),MachineID
        order by MachineRecordDate) rank_d
from dwd.fact_machine_data



select substring(MachineRecordDate, 0, 10) as machine_record_date,
       MachineRecordID,
       MachineID                           as machine_id,
       MachineRecordState,
       MachineRecordDate,
       lead(MachineRecordDate)                over(partition by substring(MachineRecordDate,0,10),MachineID order by MachineRecordDate) dt,
            unix_timestamp(lead(MachineRecordDate) over(partition by substring(MachineRecordDate,0,10),MachineID order by MachineRecordDate)) -
            unix_timestamp(MachineRecordState) period
from dwd.fact_machine_data



select start_date,
       start_hour,
       runtime - lag(run_time) over(partition by start_hour order by start_hour) hour_add_standby, sum(runtime) over(partition by start_hour order by start_hour) day_agg_standby
from (select
          first (to_date(ChangeStartTime, "yyyy-MM-dd")) start_date, hour (ChangeStartTime) start_hour, sum (unix_timestamp(ChangeEndTime)-unix_timestamp(ChangeStartTime)) run_time,
      from
          dwd.fact_change_record
      where ChangeStartTime between "2021-10-12 00:00:00" and "2021-10-13 00:00:00" and ChangeRecordState="待机"
      group by start_hour) t1


select start_date,
       start_hour,
       sum(run_time) over(order by run_time range between 2 preceding and current row) day_agg_standby
from (select
          first (to_date(ChangeStartTime, "yyyy-MM-dd HH")) start_date, hour (ChangeStartTime) start_hour, sum (unix_timestamp(ChangeEndTime)-unix_timestamp(ChangeStartTime)) run_time
      from
          dwd.fact_change_record
      where ChangeStartTime between "2021-10-13 00:00:00" and "2021-10-14 00:00:00" and ChangeRecordState="运行"
      group by start_hour) t1


select start_date_ymd,
       start_hour,
       run_time,
       sum(run_time) over(order by start_hour range between 2 preceding and current row) day_agg_standby
from (select
          first (to_date(ChangeStartTime, "yyyy-MM-dd")) start_date_ymd, substring (ChangeStartTime, 11, 13)) start_hour, sum (unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) run_time
    from
    dwd.fact_change_record
where ChangeStartTime between "2021-10-13 00:00:00" and "2021-10-14 00:00:00" and ChangeRecordState="运行"
group by start_hour sort by start_hour) t1


select
    first (to_date(ChangeStartTime, "yyyy-MM-dd")) start_date_ymd, substring (ChangeStartTime, 11, 13)) start_hour, sum (unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) run_time
from
    dwd.fact_change_record
where ChangeStartTime between "2021-10-13 00:00:00" and "2021-10-14 00:00:00" and ChangeRecordState="运行"
group by start_hour sort by start_hour





select concat(start_date_ymd,"_",start_hour) state_start_time,
       window_sum,window_avg
from (select start_date_ymd,
             start_hour,
             run_time,
             sum(run_time) over(order by start_hour rows BETWEEN 2 PRECEDING AND CURRENT ROW) window_sum,
             avg(run_time) over(order by start_hour rows BETWEEN 2 PRECEDING AND CURRENT ROW) window_avg,
             row_number() over(order by start_hour) row_index
      from (select
                first (to_date(ChangeStartTime, "yyyy-MM-dd")) start_date_ymd,
                date_format (ChangeStartTime,"HH") start_hour,
                sum (unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) run_time
            from
                dwd.fact_change_record
            where ChangeStartTime between "2021-10-13 00:00:00" and "2021-10-14 00:00:00" and ChangeRecordState="运行"
            group by start_hour sort by start_hour) t1)t1
where row_index>2

create table machine_state_time(
                                   machine_id int,change_record_state varchar(64),duration_time varchar(64),year int,month int)