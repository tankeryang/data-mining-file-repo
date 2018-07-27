#! /bin/bash

BASE_PATH=$(cd `dirname $0`; cd ..; pwd)

for line in `cat ${BASE_PATH}/config/filepath.config`
    do
        SUB_PATH_NAME=${line%%'='*}
        SUB_PATH=${line##*'='}
        case ${SUB_PATH_NAME} in
            PROD_PATH) PROD_PATH=${SUB_PATH};;
            SCRAP_PATH) SCRAP_PATH=${SUB_PATH};;
            LOG_PATH) LOG_PATH=${SUB_PATH};;
        esac
    done

OPT=`getopt -o f: --long py,project:,file:`

case "$1" in
    --py)
        project="";
        shift;

        case $1 in --project) project=$2; shift; shift;; esac
        case $1 in -f|--file)
            echo ${BASE_PATH}/${SCRAP_PATH}/${project}/$2 $3
            /opt/anaconda3/bin/python ${BASE_PATH}/${SCRAP_PATH}/${project}/$2 $3 >> ${BASE_PATH}/${LOG_PATH}/${project}/$2.log 2>&1; shift; shift;;
        esac;;
esac
