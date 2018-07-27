#! /bin/bash

BASE_PATH=$(cd `dirname $0`; cd ..; pwd)

for line in `cat ${BASE_PATH}/config/filepath.config`
    do
        SUB_PATH_NAME=${line%%'='*}
        SUB_PATH=${line##*'='}
        case ${SUB_PATH_NAME} in
            INPUT_PATH) INPUT_PATH=${SUB_PATH};;
        esac
    done

OPT=`getopt --long presto-config:,schema:,table-name:,file-name:`

case "$1" in --presto-config)
    for line in `cat ${BASE_PATH}/config/$2.config`
        do
            PARAM_NAME=${line%%'='*}
            PARAM_VALUE=${line##*'='}
            case ${PARAM_NAME} in
                presto.host) HOST=${PARAM_VALUE};;
                presto.port) PORT=${PARAM_VALUE};;
                presto.user) USER=${PARAM_VALUE};;
                presto.catalog) CATALOG=${PARAM_VALUE};;
            esac
        done
    shift;
    shift;;

sh /program/presto-cli --server ${HOST}:${PORT} --user ${USER} --catalog ${CATALOG} --output-format CSV_HEADER --execute "select * from $2.$4" > ${BASE_PATH}/${INPUT_PATH}/$6.csv