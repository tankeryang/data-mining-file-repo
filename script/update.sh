#! /bin/bash

BASE_PATH=$(cd `dirname $0`; cd ..; pwd)

for line in `cat ${BASE_PATH}/config/filepath.config`
    do
        SUB_PATH_NAME=${line%%'='*}
        SUB_PATH=${line##*'='}
        case ${SUB_PATH_NAME} in
            PROD_PATH) PROD_PATH=${SUB_PATH};;
            SQL_SCRIPT_PATH) SQL_SCRIPT_PATH=${SUB_PATH};;
            LOG_PATH) LOG_PATH=${SUB_PATH};;
        esac
    done

for line in `cat ${BASE_PATH}/config/presto_prod.config`
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


OPT=`getopt -o f: --long py,sql,file:,table:,schema:`

case "$1" in
    --py)
        project="";
        shift;

        case $1 in --project) project=$2; shift; shift;; esac
        case $1 in -f|--file)
            /opt/anaconda3/bin/python ${BASE_PATH}/${SQL_SCRIPT_PATH}/${project}/$2 $3 >> ${BASE_PATH}/${LOG_PATH}/$2.log 2>&1; shift; shift;;
        esac;;

    --sql)
        project="";
        schema="";
        table="";

        shift;
        
        case $1 in --project) project=$2; shift; shift;; esac

        case $1 in --schema) schema=$2; shift; shift;; esac

        case $1 in --table) table=$2 shift; shift;; esac

        case $schema in
            ods_mms) sh /program/presto-cli --server ${HOST}:${PORT} --user ${USER} --catalog ${CATALOG} -f ${BASE_PATH}/${PROD_PATH}/${project}/${schema}/${table}/fully.sql;;
            cdm_mms) sh /program/presto-cli --server ${HOST}:${PORT} --user ${USER} --catalog ${CATALOG} -f ${BASE_PATH}/${PROD_PATH}/${project}/${schema}/${table}/fully.sql;;
            ads_mms) sh /program/presto-cli --server ${HOST}:${PORT} --user ${USER} --catalog ${CATALOG} -f ${BASE_PATH}/${PROD_PATH}/${project}/${schema}/${table}/fully.sql;;
            ods_predict) sh /program/presto-cli --server ${HOST}:${PORT} --user ${USER} --catalog ${CATALOG} -f ${BASE_PATH}/${PROD_PATH}/${project}/${schema}/${table}/fully.sql;;
        esac;;

esac
