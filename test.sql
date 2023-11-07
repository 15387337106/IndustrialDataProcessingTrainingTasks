select
    machine_id,
    machine_record_date,
    if(first(dt) is null,0,cast(sum(period) as int)) total_time,
    count(*) over() cnt
from temp
where MachineRecordState="运行"
group by machine_record_date,machine_id


select * from (
                  select
                      machine_id,
                      machine_record_date,
                      if(first(dt) is null,0,cast(sum(period) as int)) total_time,
                      count(*) over() cnt
                  from temp
                  where MachineRecordState="运行"
                  group by machine_record_date,machine_id
              )t1
where total_time=0;


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