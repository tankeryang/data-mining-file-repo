DROP TABLE IF EXISTS ods_mms.store_sub_cate_num;


CREATE TABLE IF NOT EXISTS ods_mms.store_sub_cate_num(
    store_code    VARCHAR,
    year_code     VARCHAR,
    week_code     VARCHAR,
    sub_cate      VARCHAR,
    sub_cate_num  BIGINT
);


INSERT INTO ods_mms.store_sub_cate_num
    SELECT
        store_code,
        year_code,
        week_code,
        sub_cate,
        count(sub_cate) sub_cate_num
    FROM (
        SELECT
            mso.store_code                                     store_code,
            mso.sku_code                                       sku_code,
            mso.year_code                                      year_code,
            mso.week_code                                      week_code,
            pd_sku.sub_cate                                    sub_cate
        FROM (
            SELECT DISTINCT
                t1.store_code                                  store_code,
                replace(t1i.sku_code, ' ', '')                 sku_code,
                cast(year(t1.audit_time) AS VARCHAR)           year_code,
                IF(
                    week(t1.audit_time) < 10,
                    concat('0', cast(week(t1.audit_time) AS VARCHAR)),
                    cast(week(t1.audit_time) AS VARCHAR)
                )                                               week_code
            FROM 
                cdm_ipos.merged_sales_order t1
            LEFT JOIN
                cdm_ipos.merged_sales_order_item t1i
            ON
                t1.sales_order_no = t1i.sales_order_no
            WHERE
                date_format(t1.audit_time, '%Y-%m-%d') >= '2016-01-01'
                AND t1.order_type = 1
            GROUP BY
                t1.store_code,
                t1i.product_code,
                replace(t1i.sku_code, ' ', ''),
                cast(year(t1.audit_time) AS VARCHAR),
                IF(
                    week(t1.audit_time) < 10,
                    concat('0', cast(week(t1.audit_time) AS VARCHAR)),
                    cast(week(t1.audit_time) AS VARCHAR)
                )
            ) mso
        JOIN
            cdm_cic.product_sku pd_sku
        ON
            mso.sku_code = pd_sku.sku_code
            AND pd_sku.main_cate = '21'
    )
    GROUP BY 
        store_code,
        year_code,
        week_code,
        sub_cate
    ORDER BY
        store_code,
        year_code,
        week_code,
        sub_cate_num DESC;
