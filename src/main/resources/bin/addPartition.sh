#!/bin/bash


source /etc/profile
cd `dirname $0`
pwd=`pwd`
echo $pwd

ARGS=`getopt -o d:t:s:e:sh:eh:on --long database:,table:,startDate:,endDate:,startHour:,endHour:,isOnline: -- "$@"`

#将规范化后的命令行参数分配至位置参数（$1,$2,...)
eval set -- "${ARGS}"

while true
do
    case "$1" in
        -d|--database)
            database=$2;
            shift 2;;
		-t|--table)
            table=$2;
            shift 2;;
        -s|--startDate)
            startDate=$2;
            shift 2;;
        -e|--endDate)
            endDate=$2;
            shift 2;;
        -sh|--startHour)
            startHour=$2;
            shift 2;;
        -eh|--endHour)
            endHour=$2;
            shift 2;;
        -on|--isOnline)
            isOnline=$2;
            shift 2;;
        --)
            shift;
            break;;

        *)
            exit 1
            ;;
    esac
done

  if [ $isOnline != 'true' ];then
   echo "do not need to add partition"
   exit 0
  fi

startTimeParam=`date -d "1 hours $startDate $startHour" +"%Y%m%d %H"`
endTimeParam=`date -d "1 hours $endDate $endHour" +"%Y%m%d %H"`

startDate=`date -d "$startTimeParam" +%Y%m%d`
startHour=`date -d "$startTimeParam" +%H`
endDate=`date -d "$endTimeParam" +%Y%m%d`
endHour=`date -d "$endTimeParam" +%H`

echo "table is $table"
echo "startTime is $startTimeParam"
echo "endTime is $endTimeParam"

startTime=$startDate$startHour
endTime=$endDate$endHour

while [[ $startTime -le $endTime ]]
 do
  echo "$startTime    ..................."
  hive  -hivevar table=$table -hivevar day_p=$startDate -hivevar hour_p=$startHour  -e '
  use dw_facts;
  alter table ${hivevar:table} add if not exists partition (day_p=${hivevar:day_p},hour_p="${hivevar:hour_p}") location "/data_warehouse/dw_facts/${hivevar:table}/${hivevar:day_p}/${hivevar:hour_p}";
  '
  if [ $? -ne 0 ];then
   echo "hive addPartitions  ${startTime} is fail ..."
   exit 1
  fi
  startTimeParam=`date -d "1 hours $startDate $startHour" +"%Y%m%d %H"`
  startDate=`date -d "$startTimeParam" +%Y%m%d`
  startHour=`date -d "$startTimeParam" +%H`
  startTime=$startDate$startHour
done

