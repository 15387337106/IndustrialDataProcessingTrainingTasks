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