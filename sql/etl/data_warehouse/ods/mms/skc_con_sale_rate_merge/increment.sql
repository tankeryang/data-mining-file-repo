INSERT INTO ods_mms.skc_con_sale_rate_merge
    SELECT
        mso.sales_order_no                                                         sales_order_no,
        substr(msoi.sku_code, 1, 13)                                               skc_code,
        base_str_info.city_name                                                    city_name,
        mso.store_code                                                             terminal_store_code,
        date_format(mso.audit_time, '%Y-%m-%d')                                    audit_date,
        cast(year(mso.audit_time) AS VARCHAR)                                      year_code,
        IF(
            month(mso.audit_time) < 10,
            concat('0', cast(month(mso.audit_time) AS VARCHAR)),
            cast(month(mso.audit_time) AS VARCHAR)
        )                                                                          month_code,
        IF(
            week(mso.audit_time) < 10,
            concat('0', cast(week(mso.audit_time) AS VARCHAR)),
            cast(week(mso.audit_time) AS VARCHAR)
        )                                                                          week_code,
        ot.order_ntype                                                             order_ntype
    FROM
        cdm_ipos.merged_sales_order mso
    LEFT JOIN
        cdm_ipos.merged_sales_order_item msoi
    ON
        mso.sales_order_no = msoi.sales_order_no
    LEFT JOIN
        cdm_base.store_info base_str_info
    ON
        mso.store_code = base_str_info.store_code
    RIGHT JOIN (
        SELECT
            mso.sales_order_no,
            mso.store_code,
            date_format(mso.audit_time, '%Y-%m-%d')            audit_date,
            count(msoi.sku_code)                               order_sku_num,
            cast(IF(count(msoi.sku_code)=1, 0, 1) AS TINYINT)  order_ntype
        FROM 
            cdm_ipos.merged_sales_order mso
        LEFT JOIN
            cdm_ipos.merged_sales_order_item msoi
        ON
            mso.sales_order_no = msoi.sales_order_no
        WHERE
            mso.order_type = 1
        GROUP BY 
            mso.sales_order_no,
            mso.store_code,
            date_format(mso.audit_time, '%Y-%m-%d')
    ) ot
    ON
        mso.sales_order_no = ot.sales_order_no
        AND mso.store_code = ot.store_code
        AND date_format(mso.audit_time, '%Y-%m-%d') = ot.audit_date
    WHERE
        date_format(mso.audit_time, '%Y-%m-%d') > (SELECT max(audit_date) FROM ods_mms.skc_con_sale_rate_merge);
