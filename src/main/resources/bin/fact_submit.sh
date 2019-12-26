#!/bin/bash

jobName=$3
startDateParam=$5
endDateParam=$7
startHourParam=$9
endHourParam=${11}
startDate=`date -d "-1 hours $startDateParam $startHourParam" +%Y%m%d`
endDate=`date -d "-1 hours $endDateParam $endHourParam" +%Y%m%d`
startHour=`date -d "-1 hours $startDateParam $startHourParam" +%H`
endHour=`date -d "-1 hours $endDateParam $endHourParam" +%H`
echo "start of data is $startDate $startHour"
echo "end of data is $endDate $endHour"

Params=($@)
MainClass=${Params[0]}
Length=${#Params[@]}
Args=${Params[@]:11:Length-11}

cd `dirname $0`
pwd=`pwd`

source ./envFn.sh

load_properties ../conf/spark_fact.properties

#params: $1 className, $2 propName
getSparkProp(){
    className=$1
    propName=$2

    defaultPropKey=${propName}
    defaultPropKey=${defaultPropKey//./_}
    defaultPropKey=${defaultPropKey//-/_}
    #echo "defaultPropValue=\$${defaultPropKey}"
    eval "defaultPropValue=\$${defaultPropKey}"

    propKey="${className}_${propName}"
    propKey=${propKey//./_}
    propKey=${propKey//-/_}
    eval "propValue=\$${propKey}"

    if [ -z "$propValue" ]; then
        echo "$defaultPropValue"
    else
        echo "$propValue"
    fi
}


spark_home=${spark_home:-$SPARK_HOME}
spark_master=${spark_master}
spark_mainJar="../lib/${spark_mainJarName}"
spark_driver_memory=$(getSparkProp $MainClass "spark.driver-memory")
spark_executor_memory=$(getSparkProp $MainClass "spark.executor-memory")
spark_cores_max=$(getSparkProp $MainClass "spark.cores.max")
spark_shuffle_service_enabled=$(getSparkProp $MainClass "spark.shuffle.service.enabled")
spark_dynamicAllocation_enabled=$(getSparkProp $MainClass "spark.dynamicAllocation.enabled")
spark_dynamicAllocation_minExecutors=$(getSparkProp $MainClass "spark.dynamicAllocation.minExecutors")
spark_dynamicAllocation_maxExecutors=$(getSparkProp $MainClass "spark.dynamicAllocation.maxExecutors")
spark_dynamicAllocation_initialExecutors=$(getSparkProp $MainClass "spark.dynamicAllocation.initialExecutors")
spark_default_parallelism=$(getSparkProp $MainClass "spark.default.parallelism")
spark_yarn_queue=$(getSparkProp $MainClass "spark.yarn.queue")



for file in ../conf/*
do
	if [ -n "$resFiles" ]; then
		resFiles="$resFiles,$file"
	else
		resFiles="$file"
    fi
done

resFiles="$resFiles,/opt/hadoop/etc/hadoop/core-site.xml,/opt/hadoop/etc/hadoop/hdfs-site.xml,/opt/spark2/conf/hive-site.xml"

for file in /data/apps/azkaban/etl2/lib/*.jar
do
    if [[ "$file" == *${spark_mainJarName} ]]; then
        echo "skip $file"
    else
        if [ -n "$jarFiles" ]; then
            jarFiles="$jarFiles,$file"
        else
            jarFiles="$file"
        fi
    fi
done

startTime=$startDate$startHour
endTime=$endDate$endHour

while [[ $startTime -le $endTime ]]
do
    echo $startTime
    ts=`date +%Y%m%d_%H%M%S`
    set -x
    $spark_home/bin/spark-submit -v \
    --name li.tuo_${app_name:-$MainClass}_${jobName}_$ts \
    --master ${spark_master} \
    --executor-memory $spark_executor_memory \
    --driver-memory $spark_driver_memory \
    --files $resFiles \
    --jars $jarFiles \
    --conf spark.cores.max=${spark_cores_max}  \
    --conf spark.shuffle.service.enabled=${spark_shuffle_service_enabled} \
    --conf spark.dynamicAllocation.enabled=${spark_dynamicAllocation_enabled}  \
    --conf spark.dynamicAllocation.minExecutors=${spark_dynamicAllocation_minExecutors} \
    --conf spark.dynamicAllocation.maxExecutors=${spark_dynamicAllocation_maxExecutors} \
    --conf spark.dynamicAllocation.initialExecutors=${spark_dynamicAllocation_initialExecutors} \
    --conf spark.default.parallelism=${spark_default_parallelism} \
    --conf spark.yarn.queue=${spark_yarn_queue} \
    --conf spark.sql.parquet.compression.codec=gzip \
    --conf spark.memory.storageFraction=0.4 \
    --conf spark.memory.fraction=0.75 \
    --class "$MainClass" $spark_mainJar --date $startDate --hour $startHour $Args
    if [ $? -ne 0 ];then
        echo "Execution failed, startDate of data is ${startTime}  ..."
        exit 1
    fi

    startTimeParam=`date -d "1 hours $startDate $startHour" +"%Y%m%d %H"`
    startDate=`date -d "$startTimeParam" +%Y%m%d`
    startHour=`date -d "$startTimeParam" +%H`
    startTime=$startDate$startHour

done