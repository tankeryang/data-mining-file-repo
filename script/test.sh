#!/bin/bash
echo '$0: '$0
echo "pwd: "`pwd`
echo "scriptPath1: "$(cd `dirname $0`; cd ..; pwd)


BASE_PATH=$(cd `dirname $0`; cd ..; pwd)

for line in `cat ${BASE_PATH}/config/filepath.config`
    do
        SUB_PATH_NAME=${line%%'='*}
        SUB_PATH=${line##*'='}
        case ${SUB_PATH_NAME} in
            PROD_PATH)
                PROD_PATH=${SUB_PATH};;
            ODS_TEST_PATH)
                ODS_TEST_PATH=${SUB_PATH};;
            CDM_TEST_PATH)
                CDM_TEST_PATH=${SUB_PATH};;
            ADS_TEST_PATH)
                ADS_TEST_PATH=${SUB_PATH};;
            LOG_PATH)
                LOG_PATH=${SUB_PATH};;
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

echo ${PROD_PATH}
echo ${ODS_TEST_PATH}
echo ${CDM_TEST_PATH}
echo ${ADS_TEST_PATH}
echo ${LOG_PATH}
echo ${HOST}
