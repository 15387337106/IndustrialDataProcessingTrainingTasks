

select
unix_timestamp(ChangeEndTime)-unix_timestamp(ChangeStartTime)

select
    MachineFactory,
    BaseMachineID,
    total_running_time
from t1

select
    row_number() over(
    partition by MachineFactory,BaseMachineID
     order by unix_timestamp(ChangeEndTime)-unix_timestamp(ChangeStartTime)) duration,
        count(*) over() cnt
from dwd.fact_change_record c join dwd.dim_machine m on BaseMa787chineID=ChangeMachineID




select MachineFactory,BaseMachineID,duration1
from (select
          MachineFactory,
          BaseMachineID,
          unix_timestamp(ChangeEndTime)-unix_timestamp(ChangeStartTime) duration,
          row_number() over(
    partition by MachineFactory
     order by unix_timestamp(ChangeEndTime)-unix_timestamp(ChangeStartTime)) duration1,
              count(*) over(partition by MachineFactory) cnt
      from dwd.fact_change_record c
               join dwd.dim_machine m on BaseMachineID=ChangeMachineID and ChangeRecordState="运行")t1
where if(cnt%2=0,duration in(cnt/2,cnt/2+1),duration=(cnt+1)/2)

select MachineFactory,BaseMachineID,unix_timestamp(ChangeEndTime)-unix_timestamp(ChangeStartTime) dur_time
from  dwd.fact_change_record
join dwd.dim_machine
on BaseMachineID=ChangeMachineID and ChangeRecordState="运行"

(select MachineFactory,BaseMachineID,sum(unix_timestamp(ChangeEndTime)-unix_timestamp(ChangeStartTime)) dur_time
from  dwd.fact_change_record
join dwd.dim_machine  on BaseMachineID=ChangeMachineID and ChangeRecordState="运行"
group by MachineFactory,BaseMachineID)t1

select MachineFactory,BaseMachineID,dur_time,
       row_number() over(partition by MachineFactory order by dur_time) as duration,
       count(*) over(partition by MachineFactory) cnt
from (select MachineFactory,BaseMachineID,sum(unix_timestamp(ChangeEndTime)-unix_timestamp(ChangeStartTime)) dur_time
      from  dwd.fact_change_record
                join dwd.dim_machine  on BaseMachineID=ChangeMachineID and ChangeRecordState="运行"
      group by MachineFactory,BaseMachineID)t1
where if(cnt%2=0,duration in(cnt/2,cnt/2+1),duration=(cnt+1)/2)



INSERT INTO `BaseMachine` VALUES (111, 787, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (110, 787, 'OP30', 'OP30', '192.168.1.202', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (111, 787, 'OP50', 'OP50', '192.168.1.203', 8193, '2020-03-28', '', 97, 97, '', 0);
INSERT INTO `BaseMachine` VALUES (112, 787, 'OP160', 'OP160', '192.168.1.205', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (113, 787, 'OP130', 'OP130', '192.168.1.207', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (114, 787, 'OP120', 'OP120', '192.168.1.208', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (115, 787, 'OP170B', 'OP170B', '192.168.1.217', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (116, 787, 'OP60', 'OP60', '192.168.1.204', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (117, 787, 'OP170', 'OP170', '192.168.1.218', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (109, 787, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (110, 787, 'OP30', 'OP30', '192.168.1.202', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (111, 787, 'OP50', 'OP50', '192.168.1.203', 8193, '2020-03-28', '', 97, 97, '', 0);
INSERT INTO `BaseMachine` VALUES (112, 787, 'OP160', 'OP160', '192.168.1.205', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (113, 787, 'OP130', 'OP130', '192.168.1.207', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (114, 787, 'OP120', 'OP120', '192.168.1.208', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (115, 787, 'OP170B', 'OP170B', '192.168.1.217', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (116, 787, 'OP60', 'OP60', '192.168.1.204', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (117, 787, 'OP170', 'OP170', '192.168.1.218', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (109, 787, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (110, 787, 'OP30', 'OP30', '192.168.1.202', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (111, 787, 'OP50', 'OP50', '192.168.1.203', 8193, '2020-03-28', '', 97, 97, '', 0);
INSERT INTO `BaseMachine` VALUES (112, 787, 'OP160', 'OP160', '192.168.1.205', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (113, 787, 'OP130', 'OP130', '192.168.1.207', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (114, 787, 'OP120', 'OP120', '192.168.1.208', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (115, 787, 'OP170B', 'OP170B', '192.168.1.217', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (116, 787, 'OP60', 'OP60', '192.168.1.204', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (117, 787, 'OP170', 'OP170', '192.168.1.218', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (109, 787, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (110, 787, 'OP30', 'OP30', '192.168.1.202', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (111, 787, 'OP50', 'OP50', '192.168.1.203', 8193, '2020-03-28', '', 97, 97, '', 0);
INSERT INTO `BaseMachine` VALUES (112, 787, 'OP160', 'OP160', '192.168.1.205', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (113, 787, 'OP130', 'OP130', '192.168.1.207', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (114, 787, 'OP120', 'OP120', '192.168.1.208', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (115, 787, 'OP170B', 'OP170B', '192.168.1.217', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (116, 788, 'OP60', 'OP60', '192.168.1.204', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (117, 788, 'OP170', 'OP170', '192.168.1.218', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (109, 788, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (110, 788, 'OP30', 'OP30', '192.168.1.202', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (111, 788, 'OP50', 'OP50', '192.168.1.203', 8193, '2020-03-28', '', 97, 97, '', 0);
INSERT INTO `BaseMachine` VALUES (112, 788, 'OP160', 'OP160', '192.168.1.205', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (113, 788, 'OP130', 'OP130', '192.168.1.207', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (114, 788, 'OP120', 'OP120', '192.168.1.208', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (115, 788, 'OP170B', 'OP170B', '192.168.1.217', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (116, 788, 'OP60', 'OP60', '192.168.1.204', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (117, 788, 'OP170', 'OP170', '192.168.1.218', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (109, 788, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (110, 788, 'OP30', 'OP30', '192.168.1.202', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (111, 788, 'OP50', 'OP50', '192.168.1.203', 8193, '2020-03-28', '', 97, 97, '', 0);
INSERT INTO `BaseMachine` VALUES (112, 788, 'OP160', 'OP160', '192.168.1.205', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (113, 788, 'OP130', 'OP130', '192.168.1.207', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (114, 788, 'OP120', 'OP120', '192.168.1.208', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (115, 788, 'OP170B', 'OP170B', '192.168.1.217', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (116, 788, 'OP60', 'OP60', '192.168.1.204', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (117, 788, 'OP170', 'OP170', '192.168.1.218', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (109, 788, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (110, 788, 'OP30', 'OP30', '192.168.1.202', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (111, 788, 'OP50', 'OP50', '192.168.1.203', 8193, '2020-03-28', '', 97, 97, '', 0);
INSERT INTO `BaseMachine` VALUES (112, 788, 'OP160', 'OP160', '192.168.1.205', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (113, 788, 'OP130', 'OP130', '192.168.1.207', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (114, 788, 'OP120', 'OP120', '192.168.1.208', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (115, 788, 'OP170B', 'OP170B', '192.168.1.217', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (116, 788, 'OP60', 'OP60', '192.168.1.204', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (117, 788, 'OP170', 'OP170', '192.168.1.218', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (109, 788, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (110, 788, 'OP30', 'OP30', '192.168.1.202', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (111, 788, 'OP50', 'OP50', '192.168.1.203', 8193, '2020-03-28', '', 97, 97, '', 0);
INSERT INTO `BaseMachine` VALUES (112, 788, 'OP160', 'OP160', '192.168.1.205', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (113, 788, 'OP130', 'OP130', '192.168.1.207', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (114, 788, 'OP120', 'OP120', '192.168.1.208', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (115, 788, 'OP170B', 'OP170B', '192.168.1.217', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (116, 788, 'OP60', 'OP60', '192.168.1.204', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (117, 788, 'OP170', 'OP170', '192.168.1.218', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (109, 788, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (110, 788, 'OP30', 'OP30', '192.168.1.202', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (111, 788, 'OP50', 'OP50', '192.168.1.203', 8193, '2020-03-28', '', 97, 97, '', 0);
INSERT INTO `BaseMachine` VALUES (112, 788, 'OP160', 'OP160', '192.168.1.205', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (113, 788, 'OP130', 'OP130', '192.168.1.207', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (114, 788, 'OP120', 'OP120', '192.168.1.208', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (115, 788, 'OP170B', 'OP170B', '192.168.1.217', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (116, 788, 'OP60', 'OP60', '192.168.1.204', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (117, 788, 'OP170', 'OP170', '192.168.1.218', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (109, 788, 'OP10', 'OP10', '192.168.1.201', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (110, 788, 'OP30', 'OP30', '192.168.1.202', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (111, 788, 'OP50', 'OP50', '192.168.1.203', 8193, '2020-03-28', '', 97, 97, '', 0);
INSERT INTO `BaseMachine` VALUES (112, 788, 'OP160', 'OP160', '192.168.1.205', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (113, 788, 'OP130', 'OP130', '192.168.1.207', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (114, 788, 'OP120', 'OP120', '192.168.1.208', 8193, '2019-07-18', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (115, 788, 'OP170B', 'OP170B', '192.168.1.217', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (116, 788, 'OP60', 'OP60', '192.168.1.204', 8193, '2020-03-30', '', 1, 1, '', 0);
INSERT INTO `BaseMachine` VALUES (117, 788, 'OP170', 'OP170', '192.168.1.218', 8193, '2020-03-30', '', 1, 1, '', 0);