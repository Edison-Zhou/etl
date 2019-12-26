#!/bin/bash


source /etc/profile
cd `dirname $0`
pwd=`pwd`
echo $pwd

ARGS=`getopt -o d:t:s:e:on --long database:,table:,startDate:,endDate:,isOnline: -- "$@"`

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

hour_p=00

day_p=`date -d "-1 days "$startDate +%Y%m%d`
endDate=`date -d "-1 days "$endDate +%Y%m%d`

if [ -z "$database" ];then
    database="dw_facts"
fi

echo "table is $table"
echo "day_p is $day_p"
echo "endDate is $endDate"
echo "hour_p is $hour_p"

while [[ $day_p -le $endDate ]]
 do
  echo "$day_p    ..................."
  hive  -hivevar database=$database -hivevar table=$table -hivevar day_p=$day_p -hivevar hour_p=$hour_p  -e '
  use ${hivevar:database};
  alter table ${hivevar:table} add if not exists partition (day_p=${hivevar:day_p},hour_p="${hivevar:hour_p}") location "/data_warehouse/${hivevar:database}/${hivevar:table}/${hivevar:day_p}/${hivevar:hour_p}";
  '
  if [ $? -ne 0 ];then
   echo "hive addPartitions  ${day_p} is fail ..."
   exit 1
  fi
  day_p=`date -d "+1 days "$day_p +%Y%m%d`
done

