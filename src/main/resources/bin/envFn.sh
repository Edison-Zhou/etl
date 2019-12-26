#!/bin/bash

load_args()
{
    for x in $*
    do
        if [[ $x == --*=*  ]]
        then
            x=`echo $x|tr -d '\n'|tr -d '\r'|tr -d '-'`
            arr=(${x/=/ })
            propName=${arr[0]}
            propValue=${arr[1]}
            propName=${propName//./_}
            propName=${propName//-/_}
            eval "$propName=$propValue"
            echo arg: $propName=$propValue
        fi
    done
}

#load properties to bash env
#params: $1 inFile
load_properties()
{
    for line in `cat $1|grep -v ^[#]`
    do

            if [[ $line == *=*  ]]
            then
                line=`echo $line|tr -d ' '|tr -d '\n'|tr -d '\r'`
                arr=(${line/=/ })
                propName=${arr[0]}
                propValue=${arr[1]}
                propName=${propName//./_}
                propName=${propName//-/_}
                eval "$propName=$propValue"
                echo propline: $propName=$propValue
            fi
    done
}

#convert properties
#params: $1 inFile, $2 outFile
resolve_properties()
{
    > $2
    for line in `cat $1|grep -v ^[#]`
    do
	    if [[ $line == *=*  ]]
            then
                line=`echo $line|tr -d ' '|tr -d '\n'|tr -d '\r'`
		        arr=(${line/=/ })
		        eval echo "${arr[0]}=${arr[1]}" >> $2
            fi
    done
}

get_fullpath()
{

    #判断是否有参数,比如当前路径为/home/user1/workspace   参数为 ./../dir/hello.c
    if [ -z $1 ]
    then
        return 1
    fi
    relative_path=$1

    #取前面一部分目录,比如  ./../../ ,  ../../ 等, 在这里调用cd命令来获取这部分路径的绝对路径,因为按这样写的,在当前路径的上级目录肯定是存在的.
    #tmp_path1为 ./..
    #tmp_fullpath1 /home/user1

    tmp_path1=$(echo $relative_path | sed -e "s=/[^\.]*$==")
    tmp_fullpath1=$(cd $tmp_path1 ;  pwd)

    #获取后面一部分路径
    #tmp_path2为dir/hello.c
    tmp_path2=$(echo $relative_path | sed -e "s=\.[\./]*[/|$]==")

    #拼凑路径返回
    echo ${tmp_fullpath1}/${tmp_path2}
    return 0
}

