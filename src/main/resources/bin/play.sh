#!/usr/bin/env bash

one_day=$1
echo "Play start"
nohup sh fact_submit.sh cn.whaley.datawarehouse.fact.moretv.PlayFinal --startDate ${one_day} --endDate ${one_day} --isOnline false > ${one_day}.log 2>&1 &

