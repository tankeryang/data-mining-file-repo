INSERT INTO ods_mms.skc_con_sale_rate_merge
    SELECT
        mso.store_code  store_code,
        substr(msoi.sku_code, 1, 13)  skc_code,
        IF(roa.repeat_order_amount IS NOT NULL, roa.repeat_order_amount, 0)  repeat_order_amount,
        IF(aoa.all_order_amount IS NOT NULL, aoa.all_order_amount, 0)  all_order_amount,
        cast(year(mso.audit_time) AS VARCHAR)  year_code,
        IF(
            week(mso.audit_time) < 10,
            concat('0', cast(week(mso.audit_time) AS VARCHAR)),
            cast(week(mso.audit_time) AS VARCHAR)
        )  week_code,
        date_format(mso.audit_time, '%Y-%m-%d')  audit_date
    FROM
        cdm_ipos.merged_sales_order mso
    LEFT JOIN
        cdm_ipos.merged_sales_order_item msoi
    ON
        mso.sales_order_no = msoi.sales_order_no
    
    -- aoa 全单数量
    LEFT JOIN (
        SELECT
            mso.store_code  store_code,
            substr(msoi.sku_code, 1, 13)  skc_code,
            cast(count(mso.sales_order_no) AS INTEGER)  all_order_amount,
            date_format(mso.audit_time, '%Y-%m-%d')  audit_date
        FROM
            cdm_ipos.merged_sales_order mso
        LEFT JOIN
            cdm_ipos.merged_sales_order_item msoi
        ON
            mso.sales_order_no = msoi.sales_order_no
        GROUP BY
            mso.store_code,
            substr(msoi.sku_code, 1, 13),
            date_format(mso.audit_time, '%Y-%m-%d')
    ) aoa
    ON
        mso.store_code = aoa.store_code
        AND substr(msoi.sku_code, 1, 13) = aoa.skc_code
        AND date_format(mso.audit_time, '%Y-%m-%d') = aoa.audit_date

    -- roa 多件商品的单数量
    LEFT JOIN (
        SELECT
            mso.store_code  store_code,
            substr(msoi.sku_code, 1, 13)  skc_code,
            cast(count(mso.sales_order_no) AS INTEGER)  repeat_order_amount,
            date_format(mso.audit_time, '%Y-%m-%d')  audit_date
        FROM
            cdm_ipos.merged_sales_order mso
        LEFT JOIN
            cdm_ipos.merged_sales_order_item msoi
        ON
            mso.sales_order_no = msoi.sales_order_no
        WHERE
            mso.order_item_quantity > 1
        GROUP BY
            mso.store_code,
            substr(msoi.sku_code, 1, 13),
            date_format(mso.audit_time, '%Y-%m-%d')
    ) roa
    ON
        mso.store_code = roa.store_code
        AND substr(msoi.sku_code, 1, 13) = roa.skc_code
        AND date_format(mso.audit_time, '%Y-%m-%d') = roa.audit_date
    WHERE
        date_format(mso.audit_time, '%Y-%m-%d') IS NOT NULL;
        AND date_format(mso.audit_time, '%Y-%m-%d') > (SELECT max(audit_date) FROM ods_mms.skc_con_sale_rate_merge);
