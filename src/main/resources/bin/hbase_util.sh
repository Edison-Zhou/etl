#!/usr/bin/env bash

# deprecated

action=$1
day=$2
hour=$3

HBASE_HOME="/opt/hbase"
SCALA_HOME="/usr/local/bin/scala"

datehour_1=`date -d "-1 hour $day $hour" +%Y%m%d%H`
datehour_2=`date -d "-2 hour $day $hour" +%Y%m%d%H`

case $action in

    "snapshot")
        snapshot_cmd="snapshot 'dw:path_parser_info_$datehour_1','snapshot_path_parser_info_$datehour_1' "
        echo $snapshot_cmd | $HBASE_HOME/bin/hbase shell -n
        ;;

    "clone_snapshot")
        clone_snapshot_cmd="clone_snapshot 'snapshot_path_parser_info_$datehour_2','dw:path_parser_info_$datehour_1'"
        echo $clone_snapshot_cmd | $HBASE_HOME/bin/hbase shell -n
        ;;

    "clear_expire_rowkey")

        resFiles="$resFiles,/opt/hadoop/etc/hadoop/core-site.xml,/opt/hadoop/etc/hadoop/hdfs-site.xml,/opt/spark2/conf/hive-site.xml"

        mainJarName="DataWarehouseEtlSpark-1.0.0.jar"
        for file in /data/apps/azkaban/etl2/lib/*.jar
        do
            if [[ "$file" == *${mainJarName} ]]; then
                echo "skip $file"
            else
                if [ -n "$jarFiles" ]; then
                    jarFiles="$jarFiles:$file"
                else
                    jarFiles="$file"
                fi
            fi
        done
        jarFiles="$jarFiles:../lib/$mainJarName:../conf"
        $SCALA_HOME/bin/scala -cp $jarFiles cn.whaley.datawarehouse.util.HbaseClearUtil $day $hour 3
        ;;

esac