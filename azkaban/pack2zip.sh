#! /bin/bash

BASE_PATH=$(cd `dirname $0`; pwd)
JOB_PATH=$(cd ${BASE_PATH}/job; pwd)
ZIP_PATH=$(cd ${BASE_PATH}/zip; pwd)

OPT=`getopt -o n:d: --long zip-name:,dir-name:`

zip_name=""
dir_name=""

case "$1" in
    -n|--zip-name)
        zip_name=$2
        shift; shift;;
esac

case "$1" in
    -d|--dir-name)
        if [ $zip_name=="" ]; then
            zip_name=$2
        fi;
        dir_name=$2;
        shift; shift;;
esac

cd ${JOB_PATH}
zip -rv ${ZIP_PATH}/${zip_name}.zip ${dir_name}
