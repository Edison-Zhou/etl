#!/bin/sh

#检查日期格式(例:20090227) 
#返回状态($?) 0 合法 1 非法 
check_date() 
{ 
	#检查是否传入一个参数
	[ $# -ne 1 ] && echo 1 && exit 1

	#检查字符串长度 
	_lenStr=`expr length "$1"` 
	[ "$_lenStr" -ne 8 ] && echo 1 && exit 1

	#检查是否输入的是非0开头的数字 
	_tmpStr=`echo "$1" | grep "^[^0][0-9]*$"` 
	[ -z "$_tmpStr" ] && echo 1 && exit 1

	Y=`expr substr $1 1 4` 
	M=`expr substr $1 5 2` 
	D=`expr substr $1 7 2` 
	#检查月份 
	[ "$M" -lt 1 -o "$M" -gt 12 ] && echo 1&&exit 1
	#取当月天数 
	days=`get_mon_days "$Y$M"` 
	#检查日 
	[ "$D" -lt 1 -o "$D" -gt "$days" ] && echo 1&&exit 1

	echo 0 
} 

#获取一个月中的天数
get_mon_days()
{
        Y=`expr substr $1 1 4`
        M=`expr substr $1 5 2`

        r1=`expr $Y \% 4`
        r2=`expr $Y \% 100`
        r3=`expr $Y \% 400`

        case $M in
	        01|03|05|07|08|10|12) days=31;;
        	04|06|09|11) days=30;;
        esac
        if [ $M -eq 02 ]
        then
                if [ $r1 -eq 0 -a $r2 -ne 0 -o $r3 -eq 0 ]
                then
                        days=29
                else
                        days=28
                fi
        fi
        echo $days
}

#返回昨日日期
get_before_date() 
{ 
	Y=`expr substr $1 1 4` 
	M=`expr substr $1 5 2` 
	D=`expr substr $1 7 2` 

	#某月01日的情况 
	if [ "$D" -eq 01 ] 
	then 
		if [ "$M" -eq 01 ] 
		then 
			#某年01月01日的情况 
			#取上年年底日期(12月31日) 
			YY=`expr $Y - 1` 
			be_date="${YY}1231" 
		else 
			#取上个月月末日期 
			MM=`expr $M - 1` 
			MM=`printf "%02d" $MM` 
			dad=`get_mon_days "$Y$MM"` 
			be_date="$Y$MM$dad" 
		fi 
	else 
		#通常情况 
		DD=`expr $D - 1` 
		DD=`printf "%02d" $DD` 
		be_date="$Y$M$DD" 
	fi 
	echo $be_date 
}
#返回前N天日期	 get_beforeN_date 20140301 7
get_beforeN_date()
{
	v=$1
	for((k=1;k<=$2;k++)); do 
		v=`get_before_date $v`			
	done
	echo $v
}

#返回明天日期 
get_next_date() 
{ 
	Y=`expr substr $1 1 4` 
	M=`expr substr $1 5 2` 
	D=`expr substr $1 7 2` 

	dad=`get_mon_days "$Y$M"` #当月天数 

	if [ "$D" = "$dad" ];then 
		#特殊情况:月底 
		if [ "$M$D" = "1231" ];then 
			#年底的情况 
			YY=`expr $Y + 1` 
			next_date="${YY}0101" 
		else 
			MM=`expr $M + 1` 
			MM=`printf "%02d" $MM` 
			next_date="$Y${MM}01" 
		fi 
	else 
		#通常情况 
		DD=`expr $D + 1` 
		DD=`printf "%02d" $DD` 
		next_date="$Y$M$DD" 
	fi 

	echo $next_date 
} 

#----------------------------------------------------------------- 
#返回当月月末日期YYYYMMDD
get_cur_date()
{
	Y=`expr substr $1 1 4`
	M=`expr substr $1 5 2`
	r1=`expr $Y % 4`
	r2=`expr $Y % 100`
	r3=`expr $Y % 400`
	case $M in 01|03|05|07|08|10|12) days=31;; 04|06|09|11) days=30;;
	esac
	if [ $M -eq 02 ]
	then if [ $r1 -eq 0 -a $r2 -ne 0 -o $r3 -eq 0 ]
		then days=29
		else days=28
		fi
	fi
	echo $Y$M$days
}
#返回当月月份YYYYMM
get_cur_month()
{
	Y=`expr substr $1 1 4`
	M=`expr substr $1 5 2`
	echo $Y$M
}
#返回上月月末日期YYYYMMDD
get_last_date()
{
	Y=`expr substr $1 1 4`
	M=`expr substr $1 5 2`
	M=`expr $M "-" 1`
	if [ $M -eq 0 ]
	then
		let Y=Y-1
		M=12
	else M=`printf "%02d" $M`
	fi
	r1=`expr $Y % 4`
	r2=`expr $Y % 100`
	r3=`expr $Y % 400`
	case $M in 01|03|05|07|08|10|12) days=31;; 04|06|09|11) days=30;;
	esac
	if [ $M -eq 02 ]
	then if [ $r1 -eq 0 -a $r2 -ne 0 -o $r3 -eq 0 ]
		then days=29
		else days=28
		fi
	fi
	echo $Y$M$days
}
#返回上月月份YYYYMM
get_last_month()
{
	Y=`expr substr $1 1 4`
	M=`expr substr $1 5 2`
	M=`expr $M "-" 1`
	if [ $M -eq 0 ]
	then
		let Y=Y-1
		M=12
	else M=`printf "%02d" $M`
	fi
	echo $Y$M
}

 
