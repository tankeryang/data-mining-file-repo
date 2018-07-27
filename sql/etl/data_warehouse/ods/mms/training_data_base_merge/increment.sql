INSERT INTO ods_mms.training_data_base_merge
    SELECT
        mso.store_code                                                             store_code,
        concat(pd_sku.sub_cate, pd_sku.color_code)                                 scc_code,
        concat(pd_sku.leaf_cate, pd_sku.color_code)                                lcc_code,
        mso.product_code                                                           product_code,
        substr(mso.sku_code, 1, 13)                                                skc_code,
        mso.sku_code                                                               sku_code,
        mso.dates                                                                  dates,
        mso.year_code                                                              year_code,
        mso.month_code                                                             month_code,
        mso.week_code                                                              week_code,
        concat(mso.year_code, mso.week_code)                                       year_week_code,
        mso.day_of_month                                                           day_of_month,
        mso.day_of_week                                                            day_of_week,
        date_format(date_parse(pd_sku.list_date, '%Y%m%d'), '%Y-%m-%d')            list_dates,
        substr(pd_sku.list_date, 1, 4)                                             list_dates_year_code,
        substr(pd_sku.list_date, 5, 2)                                             list_dates_month_code,
        IF(
            week(date_parse(pd_sku.list_date, '%Y%m%d')) < 10,
            concat('0', cast(week(date_parse(pd_sku.list_date, '%Y%m%d')) AS VARCHAR)),
            cast(week(date_parse(pd_sku.list_date, '%Y%m%d')) AS VARCHAR)
        )                                                                          list_dates_week_code,
        base_str_info.city_name                                                    city_name,
        ods_cic_str_info.area_code                                                 area_code,
        base_str_info.sales_channel                                                sales_channel,
        mms_str_info.store_type                                                    store_type,
        base_str_info.store_level                                                  store_level,
        pd_sku.main_cate                                                           main_cate,
        pd_sku.sub_cate                                                            sub_cate,
        pd_sku.leaf_cate                                                           leaf_cate,
        pd_sku.color_code                                                          color_code,
        mso.size_code                                                              size_code,
        IF(pd_sku.lining != '', pd_sku.lining, 'None')                             lining,
        mso.customer_level                                                         customer_level,
        sp.special_day_type                                                        special_day_type,
        cwh.weather_day_id                                                         weather_day_id,
        cwh.weather_day                                                            weather_day,
        cwh.weather_night_id                                                       weanther_night_id,
        cwh.weather_night                                                          weather_night,
        cast(cwh.tempreture_day AS TINYINT)                                        tempreture_day,
        cast(cwh.tempreture_night AS TINYINT)                                      tempreture_night,
        cwh.wind_day                                                               wind_day,
        cast(cwh.wind_comp_day AS TINYINT)                                         wind_comp_day,
        cwh.wind_night                                                             wind_night,
        cast(cwh.wind_comp_night AS TINYINT)                                       wind_comp_night,
        avg(mso.retail_price)                                                      retail_amount,
        avg(mso.sales_price)                                                       sales_amount,
        avg(mso.discount_rate)                                                     discount_rate,
        sum(mso.quantity)                                                          sales_qty,
        mso.pos_system                                                             pos_system
    FROM (
        SELECT DISTINCT
            t1.store_code                                   store_code,
            t1i.product_code                                product_code,
            replace(t1i.sku_code, ' ', '')                  sku_code,
            date_format(t1.audit_time, '%Y-%m-%d')          dates,
            cast(year(t1.audit_time) AS VARCHAR)            year_code,
            IF(
                month(t1.audit_time) < 10,
                concat('0', cast(month(t1.audit_time) AS VARCHAR)),
                cast(month(t1.audit_time) AS VARCHAR)
            )                                               month_code,
            IF(
                week(t1.audit_time) < 10,
                concat('0', cast(week(t1.audit_time) AS VARCHAR)),
                cast(week(t1.audit_time) AS VARCHAR)
            )                                               week_code,
            IF(
                day(t1.audit_time) < 10,
                concat('0', cast(day(t1.audit_time) AS VARCHAR)),
                cast(day(t1.audit_time) AS VARCHAR)
            )                                               day_of_month,
            IF(
                dow(t1.audit_time) < 10,
                concat('0', cast(dow(t1.audit_time) AS VARCHAR)),
                cast(dow(t1.audit_time) AS VARCHAR)
            )                                               day_of_week,
            substr(replace(t1i.sku_code, ' ', ''), 14, 1)   size_code,
            t1.customer_level                               customer_level,
            avg(t1i.retail_price)                           retail_price,
            avg(t1i.sales_price)                            sales_price,
            avg(t1i.discount_rate)                          discount_rate,
            sum(t1i.quantity)                               quantity,
            t1.order_type                                   order_type,
            t1.pos_system                                   pos_system
        FROM 
            cdm_ipos.merged_sales_order t1
        LEFT JOIN
            cdm_ipos.merged_sales_order_item t1i
        ON
            t1.sales_order_no = t1i.sales_order_no
        WHERE
            date_format(t1.audit_time, '%Y-%m-%d') > (SELECT max(dates) FROM ods_mms.training_data_base_merge)
            AND t1.order_type = 1
            AND t1i.discount_rate > 0
            AND t1i.sales_price > 0
        GROUP BY
            t1.store_code,
            t1i.product_code,
            replace(t1i.sku_code, ' ', ''),
            date_format(t1.audit_time, '%Y-%m-%d'),
            cast(year(t1.audit_time) AS VARCHAR),
            IF(
                month(t1.audit_time) < 10,
                concat('0', cast(month(t1.audit_time) AS VARCHAR)),
                cast(month(t1.audit_time) AS VARCHAR)
            ),
            IF(
                week(t1.audit_time) < 10,
                concat('0', cast(week(t1.audit_time) AS VARCHAR)),
                cast(week(t1.audit_time) AS VARCHAR)
            ),
            IF(
                day(t1.audit_time) < 10,
                concat('0', cast(day(t1.audit_time) AS VARCHAR)),
                cast(day(t1.audit_time) AS VARCHAR)
            ),
            IF(
                dow(t1.audit_time) < 10,
                concat('0', cast(dow(t1.audit_time) AS VARCHAR)),
                cast(dow(t1.audit_time) AS VARCHAR)
            ),
            substr(replace(t1i.sku_code, ' ', ''), 14, 1),
            t1.customer_level,
            t1.order_type,
            t1.pos_system
    ) mso
    JOIN
        cdm_base.store_info base_str_info
    ON
        mso.store_code = base_str_info.store_code
    JOIN
        ods_cic.store_info ods_cic_str_info
    ON
        mso.store_code = ods_cic_str_info.store_code
    JOIN
        ods_mms.store_info mms_str_info
    ON
        mso.store_code = mms_str_info.store_code
        AND base_str_info.store_code = mms_str_info.store_code
    JOIN
        ods_mms.city_weather_history cwh
    ON
        base_str_info.city = cwh.city_name
        AND mso.dates = cwh.dates
    LEFT JOIN
        ods_mms.special_day_info sp
    ON
        mso.dates = sp.dates
    JOIN
        cdm_cic.product_sku pd_sku
    ON
        mso.sku_code = pd_sku.sku_code
        AND pd_sku.main_cate = '21'
    GROUP BY
        mso.store_code,
        mso.product_code,
        substr(mso.sku_code, 1, 13),
        mso.sku_code,
        mso.dates,
        mso.year_code,
        mso.month_code,
        mso.week_code,
        mso.day_of_month,
        mso.day_of_week,
        date_format(date_parse(pd_sku.list_date, '%Y%m%d'), '%Y-%m-%d'),
        substr(pd_sku.list_date, 1, 4),
        substr(pd_sku.list_date, 5, 2),
        IF(
            week(date_parse(pd_sku.list_date, '%Y%m%d')) < 10,
            concat('0', cast(week(date_parse(pd_sku.list_date, '%Y%m%d')) AS VARCHAR)),
            cast(week(date_parse(pd_sku.list_date, '%Y%m%d')) AS VARCHAR)
        ),
        base_str_info.city_name,
        ods_cic_str_info.area_code,
        base_str_info.sales_channel,
        mms_str_info.store_type,
        base_str_info.store_level,
        pd_sku.main_cate,
        pd_sku.sub_cate,
        pd_sku.leaf_cate,
        pd_sku.color_code,
        mso.size_code,
        IF(pd_sku.lining != '', pd_sku.lining, 'None'),
        mso.customer_level,
        sp.special_day_type,
        cwh.weather_day_id,
        cwh.weather_day,
        cwh.weather_night_id,
        cwh.weather_night,
        cwh.tempreture_day,
        cwh.tempreture_night,
        cwh.wind_day,
        cwh.wind_comp_day,
        cwh.wind_night,
        cwh.wind_comp_night,
        mso.pos_system;
