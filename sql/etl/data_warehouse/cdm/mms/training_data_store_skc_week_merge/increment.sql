INSERT INTO cdm_mms.training_data_store_skc_week_merge
    SELECT DISTINCT
        tr_base.store_code,
        tr_base.skc_code,
        tr_base.year_code,
        tr_base.week_code,
        tr_base.year_week_code,
        IF(itv.prev_year_week_code IS NOT NULL, itv.prev_year_week_code, tr_base.year_week_code),

        -- interval_weeks_to_prev
        cast(IF(
            IF (
                tr_base.year_code = substr(itv.prev_year_week_code, 1, 4), 
                cast(tr_base.year_week_code AS INTEGER) - cast(itv.prev_year_week_code AS INTEGER),
                cast(tr_base.year_week_code AS INTEGER) - cast(itv.prev_year_week_code AS INTEGER) - ((cast(tr_base.year_code AS INTEGER) - cast(substr(itv.prev_year_week_code, 1, 4) AS INTEGER)) * 48)
            ) IS NOT NULL,
            IF (
                tr_base.year_code = substr(itv.prev_year_week_code, 1, 4), 
                cast(tr_base.year_week_code AS INTEGER) - cast(itv.prev_year_week_code AS INTEGER),
                cast(tr_base.year_week_code AS INTEGER) - cast(itv.prev_year_week_code AS INTEGER) - ((cast(tr_base.year_code AS INTEGER) - cast(substr(itv.prev_year_week_code, 1, 4) AS INTEGER)) * 48)
            ),
            0
        ) AS INTEGER),

        tr_base.list_dates_year_code,
        tr_base.list_dates_week_code,
        concat(tr_base.list_dates_year_code, tr_base.list_dates_week_code),

        -- interval_weeks_to_list
        cast(IF (
            tr_base.year_code = tr_base.list_dates_year_code, 
            cast(tr_base.year_week_code AS INTEGER)
                - cast(concat(tr_base.list_dates_year_code, tr_base.list_dates_week_code) AS INTEGER),
            cast(tr_base.year_week_code AS INTEGER)
                - cast(concat(tr_base.list_dates_year_code, tr_base.list_dates_week_code) AS INTEGER)
                - ((cast(tr_base.year_code AS INTEGER) - cast(tr_base.list_dates_year_code AS INTEGER)) * 48)
        ) AS INTEGER),

        -- str_attr
        tr_base.city_name,
        tr_base.area_code,
        tr_base.sales_channel,
        tr_base.store_type,
        tr_base.store_level,
        IF(str_attr.store_retail_amount_highest_8week IS NOT NULL, str_attr.store_retail_amount_highest_8week, 0),
        IF(str_attr.store_retail_amount_lowest_8week IS NOT NULL, str_attr.store_retail_amount_lowest_8week, 0),
        IF(str_attr.store_retail_amount_mean_8week IS NOT NULL, str_attr.store_retail_amount_mean_8week, 0),
        IF(str_attr.store_retail_amount_gap_8week IS NOT NULL, str_attr.store_retail_amount_gap_8week, 0),
        IF(str_attr.store_sales_amount_highest_8week IS NOT NULL, str_attr.store_sales_amount_highest_8week, 0),
        IF(str_attr.store_sales_amount_lowest_8week IS NOT NULL, str_attr.store_sales_amount_lowest_8week, 0),
        IF(str_attr.store_sales_amount_mean_8week IS NOT NULL, str_attr.store_sales_amount_mean_8week, 0),
        IF(str_attr.store_sales_amount_gap_8week IS NOT NULL, str_attr.store_sales_amount_gap_8week, 0),
        IF(str_attr.store_discount_rate_mean_8week IS NOT NULL, str_attr.store_discount_rate_mean_8week, 1),

        -- leaf_cate_attr
        tr_base.main_cate,
        tr_base.sub_cate,
        tr_base.leaf_cate,
        IF(lfc_attr.leaf_cate_retail_amount_highest_8week IS NOT NULL, lfc_attr.leaf_cate_retail_amount_highest_8week, 0),
        IF(lfc_attr.leaf_cate_retail_amount_lowest_8week IS NOT NULL, lfc_attr.leaf_cate_retail_amount_lowest_8week, 0),
        IF(lfc_attr.leaf_cate_retail_amount_mean_8week IS NOT NULL, lfc_attr.leaf_cate_retail_amount_mean_8week, 0),
        IF(lfc_attr.leaf_cate_retail_amount_gap_8week IS NOT NULL, lfc_attr.leaf_cate_retail_amount_gap_8week, 0),
        IF(lfc_attr.leaf_cate_sales_amount_highest_8week IS NOT NULL, lfc_attr.leaf_cate_sales_amount_highest_8week, 0),
        IF(lfc_attr.leaf_cate_sales_amount_lowest_8week IS NOT NULL, lfc_attr.leaf_cate_sales_amount_lowest_8week, 0),
        IF(lfc_attr.leaf_cate_sales_amount_mean_8week IS NOT NULL, lfc_attr.leaf_cate_sales_amount_mean_8week, 0),
        IF(lfc_attr.leaf_cate_sales_amount_gap_8week IS NOT NULL, lfc_attr.leaf_cate_sales_amount_gap_8week, 0),
        IF(lfc_attr.leaf_cate_discount_rate_mean_8week IS NOT NULL, lfc_attr.leaf_cate_discount_rate_mean_8week, 1),
        tr_base.color_code,
        tr_base.lining,

        -- customer
        cast(IF(cstm_l1_1.customer_level_1 IS NOT NULL, cstm_l1_1.customer_level_1, 0) AS INTEGER),
        cast(IF(cstm_l1_2.customer_level_2 IS NOT NULL, cstm_l1_2.customer_level_2, 0) AS INTEGER),
        cast(IF(cstm_l1_3.customer_level_3 IS NOT NULL, cstm_l1_3.customer_level_3, 0) AS INTEGER),
        cast(IF(
            (
                IF(cstm_l1_1.customer_level_1 IS NOT NULL, cstm_l1_1.customer_level_1, 0)
                + IF(cstm_l1_2.customer_level_2 IS NOT NULL, cstm_l1_2.customer_level_2, 0)
                + IF(cstm_l1_3.customer_level_3 IS NOT NULL, cstm_l1_3.customer_level_3, 0)
            ) != 0,
            IF(cstm_l1_1.customer_level_1 IS NOT NULL, cstm_l1_1.customer_level_1, 0) * 1.0 / (
            IF(cstm_l1_1.customer_level_1 IS NOT NULL, cstm_l1_1.customer_level_1, 0)
            + IF(cstm_l1_2.customer_level_2 IS NOT NULL, cstm_l1_2.customer_level_2, 0)
            + IF(cstm_l1_3.customer_level_3 IS NOT NULL, cstm_l1_3.customer_level_3, 0)),
            0
        ) AS DECIMAL(18, 2)),
        cast(IF(
            (
                IF(cstm_l1_1.customer_level_1 IS NOT NULL, cstm_l1_1.customer_level_1, 0)
                + IF(cstm_l1_2.customer_level_2 IS NOT NULL, cstm_l1_2.customer_level_2, 0)
                + IF(cstm_l1_3.customer_level_3 IS NOT NULL, cstm_l1_3.customer_level_3, 0)
            ) != 0,
            IF(cstm_l1_2.customer_level_2 IS NOT NULL, cstm_l1_2.customer_level_2, 0) * 1.0 / (
            IF(cstm_l1_1.customer_level_1 IS NOT NULL, cstm_l1_1.customer_level_1, 0)
            + IF(cstm_l1_2.customer_level_2 IS NOT NULL, cstm_l1_2.customer_level_2, 0)
            + IF(cstm_l1_3.customer_level_3 IS NOT NULL, cstm_l1_3.customer_level_3, 0)),
            0
        ) AS DECIMAL(18, 2)),
        cast(IF(
            (
                IF(cstm_l1_1.customer_level_1 IS NOT NULL, cstm_l1_1.customer_level_1, 0)
                + IF(cstm_l1_2.customer_level_2 IS NOT NULL, cstm_l1_2.customer_level_2, 0)
                + IF(cstm_l1_3.customer_level_3 IS NOT NULL, cstm_l1_3.customer_level_3, 0)
            ) != 0,
            IF(cstm_l1_3.customer_level_3 IS NOT NULL, cstm_l1_3.customer_level_3, 0) * 1.0 / (
            IF(cstm_l1_1.customer_level_1 IS NOT NULL, cstm_l1_1.customer_level_1, 0)
            + IF(cstm_l1_2.customer_level_2 IS NOT NULL, cstm_l1_2.customer_level_2, 0)
            + IF(cstm_l1_3.customer_level_3 IS NOT NULL, cstm_l1_3.customer_level_3, 0)),
            0
        ) AS DECIMAL(18, 2)),

        cast(IF(cstm_l2_1.customer_level_1 IS NOT NULL, cstm_l2_1.customer_level_1, 0) AS INTEGER),
        cast(IF(cstm_l2_2.customer_level_2 IS NOT NULL, cstm_l2_2.customer_level_2, 0) AS INTEGER),
        cast(IF(cstm_l2_3.customer_level_3 IS NOT NULL, cstm_l2_3.customer_level_3, 0) AS INTEGER),
        cast(IF(
            (
                IF(cstm_l2_1.customer_level_1 IS NOT NULL, cstm_l2_1.customer_level_1, 0)
                + IF(cstm_l2_2.customer_level_2 IS NOT NULL, cstm_l2_2.customer_level_2, 0)
                + IF(cstm_l2_3.customer_level_3 IS NOT NULL, cstm_l2_3.customer_level_3, 0)
            ) != 0,
            IF(cstm_l2_1.customer_level_1 IS NOT NULL, cstm_l2_1.customer_level_1, 0) * 1.0 / (
            IF(cstm_l2_1.customer_level_1 IS NOT NULL, cstm_l2_1.customer_level_1, 0)
            + IF(cstm_l2_2.customer_level_2 IS NOT NULL, cstm_l2_2.customer_level_2, 0)
            + IF(cstm_l2_3.customer_level_3 IS NOT NULL, cstm_l2_3.customer_level_3, 0)),
            0
        ) AS DECIMAL(18, 2)),
        cast(IF(
            (
                IF(cstm_l2_1.customer_level_1 IS NOT NULL, cstm_l2_1.customer_level_1, 0)
                + IF(cstm_l2_2.customer_level_2 IS NOT NULL, cstm_l2_2.customer_level_2, 0)
                + IF(cstm_l2_3.customer_level_3 IS NOT NULL, cstm_l2_3.customer_level_3, 0)
            ) != 0,
            IF(cstm_l2_2.customer_level_2 IS NOT NULL, cstm_l2_2.customer_level_2, 0) * 1.0 / (
            IF(cstm_l2_1.customer_level_1 IS NOT NULL, cstm_l2_1.customer_level_1, 0)
            + IF(cstm_l2_2.customer_level_2 IS NOT NULL, cstm_l2_2.customer_level_2, 0)
            + IF(cstm_l2_3.customer_level_3 IS NOT NULL, cstm_l2_3.customer_level_3, 0)),
            0
        ) AS DECIMAL(18, 2)),
        cast(IF(
            (
                IF(cstm_l2_1.customer_level_1 IS NOT NULL, cstm_l2_1.customer_level_1, 0)
                + IF(cstm_l2_2.customer_level_2 IS NOT NULL, cstm_l2_2.customer_level_2, 0)
                + IF(cstm_l2_3.customer_level_3 IS NOT NULL, cstm_l2_3.customer_level_3, 0)
            ) != 0,
            IF(cstm_l2_3.customer_level_3 IS NOT NULL, cstm_l2_3.customer_level_3, 0) * 1.0 / (
            IF(cstm_l2_1.customer_level_1 IS NOT NULL, cstm_l2_1.customer_level_1, 0)
            + IF(cstm_l2_2.customer_level_2 IS NOT NULL, cstm_l2_2.customer_level_2, 0)
            + IF(cstm_l2_3.customer_level_3 IS NOT NULL, cstm_l2_3.customer_level_3, 0)),
            0
        ) AS DECIMAL(18, 2)),

        cast(IF(cstm_l1_1.customer_level_1 IS NOT NULL, cstm_l1_1.customer_level_1, 0) - IF(cstm_l2_1.customer_level_1 IS NOT NULL, cstm_l2_1.customer_level_1, 0) AS INTEGER),
        cast(IF(cstm_l1_2.customer_level_2 IS NOT NULL, cstm_l1_2.customer_level_2, 0) - IF(cstm_l2_2.customer_level_2 IS NOT NULL, cstm_l2_2.customer_level_2, 0) AS INTEGER),
        cast(IF(cstm_l1_3.customer_level_3 IS NOT NULL, cstm_l1_3.customer_level_3, 0) - IF(cstm_l2_3.customer_level_3 IS NOT NULL, cstm_l2_3.customer_level_3, 0) AS INTEGER),

        tr_base.special_day_type,

        -- 天气
        wdm.weather_day_most,
        wnm.weather_night_most,
        tmptr.tempreture_day_highest,
        tmptr_lw.tempreture_day_highest,
        tmptr.tempreture_day_highest - tmptr_lw.tempreture_day_highest,
        tmptr.tempreture_day_lowest,
        tmptr_lw.tempreture_day_lowest,
        tmptr.tempreture_day_lowest - tmptr_lw.tempreture_day_lowest,
        tmptr.tempreture_day_avg,
        tmptr_lw.tempreture_day_avg,
        cast(tmptr.tempreture_day_avg - tmptr_lw.tempreture_day_avg AS DECIMAL(18, 2)),
        tmptr.tempreture_day_gap,
        tmptr_lw.tempreture_day_gap,
        tmptr.tempreture_night_highest,
        tmptr_lw.tempreture_night_highest,
        tmptr.tempreture_night_highest - tmptr_lw.tempreture_night_highest,
        tmptr.tempreture_night_lowest,
        tmptr_lw.tempreture_night_lowest,
        tmptr.tempreture_night_lowest - tmptr_lw.tempreture_night_lowest,
        tmptr.tempreture_night_avg,
        tmptr_lw.tempreture_night_avg,
        cast(tmptr.tempreture_night_avg - tmptr_lw.tempreture_night_avg AS DECIMAL(18, 2)),
        tmptr.tempreture_night_gap,
        tmptr_lw.tempreture_night_gap,
        tmptr.tempreture_avg_gap,
        tmptr_lw.tempreture_avg_gap,

        -- 销售相关
        sls.retail_amount_mean,
        cast(sls.retail_amount_mean - IF(str_attr.store_retail_amount_mean_8week IS NOT NULL, str_attr.store_retail_amount_mean_8week, 0) AS DECIMAL(18, 2)),
        IF(sls_lw.discount_rate_mean_last_week IS NOT NULL, sls_lw.discount_rate_mean_last_week, 1),
        IF(sls_l2w.discount_rate_mean_last_2week IS NOT NULL, sls_l2w.discount_rate_mean_last_2week, 1),
        cast(IF(sls_lw.discount_rate_mean_last_week IS NOT NULL, sls_lw.discount_rate_mean_last_week, 1)
        - IF(sls_l2w.discount_rate_mean_last_2week IS NOT NULL, sls_l2w.discount_rate_mean_last_2week, 1)
        AS DECIMAL(18, 2)),
        IF(skc_csr_lw.skc_con_sale_rate_last_week IS NOT NULL, skc_csr_lw.skc_con_sale_rate_last_week, 0),
        IF(skc_csr_l2w.skc_con_sale_rate_last_2week IS NOT NULL, skc_csr_l2w.skc_con_sale_rate_last_2week, 0),
        cast(IF(skc_csr_lw.skc_con_sale_rate_last_week IS NOT NULL, skc_csr_lw.skc_con_sale_rate_last_week, 0)
        - IF(skc_csr_l2w.skc_con_sale_rate_last_2week IS NOT NULL, skc_csr_l2w.skc_con_sale_rate_last_2week, 0)
        AS DECIMAL(18, 2)),
        IF(sls_lw.sales_qty_last_week IS NOT NULL, sls_lw.sales_qty_last_week, 0),
        IF(sls_l2w.sales_qty_last_2week IS NOT NULL, sls_l2w.sales_qty_last_2week, 0),
        cast(IF(sls_lw.sales_qty_last_week IS NOT NULL, sls_lw.sales_qty_last_week, 0)
        - IF(sls_l2w.sales_qty_last_2week IS NOT NULL, sls_l2w.sales_qty_last_2week, 0)
        AS INTEGER),
        sls.sales_qty
    FROM
        ods_mms.training_data_base_merge tr_base
    
    -- 上次购买的年-周
    LEFT JOIN (
        SELECT
            t1.store_code,
            t1.skc_code,
            t1.year_code,
            t1.week_code,
            t1.year_week_code,
            max(t2.year_week_code) prev_year_week_code
        FROM
            ods_mms.training_data_base_merge t1
        INNER JOIN
            ods_mms.training_data_base_merge t2
        ON
            t1.store_code = t2.store_code
            AND t1.skc_code = t2.skc_code
            AND t1.year_week_code > t2.year_week_code
        GROUP BY
            t1.store_code,
            t1.skc_code,
            t1.year_code,
            t1.week_code,
            t1.year_week_code
    ) itv
    ON
        itv.store_code = tr_base.store_code
        AND itv.skc_code = tr_base.skc_code
        AND itv.year_week_code = tr_base.year_week_code

    -- 门店属性
    LEFT JOIN (
        SELECT
            t1.store_code,
            t1.year_week_code,
            cast(max(t2.retail_amount) AS DECIMAL(18, 2))  store_retail_amount_highest_8week,
            cast(min(t2.retail_amount) AS DECIMAL(18, 2))  store_retail_amount_lowest_8week,
            cast(avg(t2.retail_amount) AS DECIMAL(18, 2))  store_retail_amount_mean_8week,
            cast(max(t2.retail_amount) - min(t2.retail_amount) AS DECIMAL(18, 2))  store_retail_amount_gap_8week,
            cast(max(t2.sales_amount) AS DECIMAL(18, 2))   store_sales_amount_highest_8week,
            cast(min(t2.sales_amount) AS DECIMAL(18, 2))   store_sales_amount_lowest_8week,
            cast(avg(t2.sales_amount) AS DECIMAL(18, 2))   store_sales_amount_mean_8week,
            cast(max(t2.sales_amount) - min(t2.sales_amount) AS DECIMAL(18, 2))  store_sales_amount_gap_8week,
            cast(avg(t2.discount_rate) AS DECIMAL(18, 2)) store_discount_rate_mean_8week
        FROM
            ods_mms.training_data_base_merge t1
        LEFT JOIN
            ods_mms.training_data_base_merge t2
        ON
            t1.store_code = t2.store_code
            AND t1.year_week_code > t2.year_week_code
            AND t2.year_week_code > IF(
                cast(t1.week_code AS INTEGER) - 8 <= 0,
                cast(cast(t1.year_week_code AS INTEGER) - 56 AS VARCHAR),
                cast(cast(t1.year_week_code AS INTEGER) - 8 AS VARCHAR)
            )
        GROUP BY
            t1.store_code,
            t1.year_week_code
    ) str_attr
    ON
        tr_base.store_code = str_attr.store_code
        AND tr_base.year_week_code = str_attr.year_week_code

    -- 小类属性
    LEFT JOIN (
        SELECT
            t1.store_code,
            t1.leaf_cate,
            t1.year_week_code,
            cast(max(t2.retail_amount) AS DECIMAL(18, 2))  leaf_cate_retail_amount_highest_8week,
            cast(min(t2.retail_amount) AS DECIMAL(18, 2))  leaf_cate_retail_amount_lowest_8week,
            cast(avg(t2.retail_amount) AS DECIMAL(18, 2))  leaf_cate_retail_amount_mean_8week,
            cast(max(t2.retail_amount) - min(t2.retail_amount) AS DECIMAL(18, 2))  leaf_cate_retail_amount_gap_8week,
            cast(max(t2.sales_amount) AS DECIMAL(18, 2))   leaf_cate_sales_amount_highest_8week,
            cast(min(t2.sales_amount) AS DECIMAL(18, 2))   leaf_cate_sales_amount_lowest_8week,
            cast(avg(t2.sales_amount) AS DECIMAL(18, 2))   leaf_cate_sales_amount_mean_8week,
            cast(max(t2.sales_amount) - min(t2.sales_amount) AS DECIMAL(18, 2))  leaf_cate_sales_amount_gap_8week,
            cast(avg(t2.discount_rate) AS DECIMAL(18, 2)) leaf_cate_discount_rate_mean_8week
        FROM
            ods_mms.training_data_base_merge t1
        LEFT JOIN
            ods_mms.training_data_base_merge t2
        ON
            t1.store_code = t2.store_code
            AND t1.year_week_code > t2.year_week_code
            AND t2.year_week_code > IF(
                cast(t1.week_code AS INTEGER) - 8 <= 0,
                cast(cast(t1.year_week_code AS INTEGER) - 56 AS VARCHAR),
                cast(cast(t1.year_week_code AS INTEGER) - 8 AS VARCHAR)
            )
        GROUP BY
            t1.store_code,
            t1.leaf_cate,
            t1.year_week_code
    ) lfc_attr
    ON
        tr_base.store_code = lfc_attr.store_code
        AND tr_base.leaf_cate = lfc_attr.leaf_cate
        AND tr_base.year_week_code = lfc_attr.year_week_code

    -- 上周到店交易的lv1顾客数
    LEFT JOIN (
        SELECT
            store_code,
            year_week_code,
            count(customer_level) customer_level_1
        FROM
            ods_mms.training_data_base_merge
        WHERE
            customer_level = '1'
        GROUP BY
            store_code,
            year_week_code
    ) cstm_l1_1
    ON
        tr_base.store_code = cstm_l1_1.store_code
        AND cstm_l1_1.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 1 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 49 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 1 AS VARCHAR)
        )

    -- 上周到店交易的lv2顾客数
    LEFT JOIN (
        SELECT
            store_code,
            year_week_code,
            count(customer_level) customer_level_2
        FROM
            ods_mms.training_data_base_merge
        WHERE
            customer_level = '2'
        GROUP BY
            store_code,
            year_week_code
    ) cstm_l1_2
    ON
        tr_base.store_code = cstm_l1_2.store_code
        AND cstm_l1_2.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 1 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 49 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 1 AS VARCHAR)
        )

    -- 上周到店交易的lv3顾客数
    LEFT JOIN (
        SELECT
            store_code,
            year_week_code,
            count(customer_level) customer_level_3
        FROM
            ods_mms.training_data_base_merge
        WHERE
            customer_level = '3'
        GROUP BY
            store_code,
            year_week_code
    ) cstm_l1_3
    ON
        tr_base.store_code = cstm_l1_3.store_code
        AND cstm_l1_3.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 1 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 49 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 1 AS VARCHAR)
        )

    -- 上2周到店交易的lv1顾客数
    LEFT JOIN (
        SELECT
            store_code,
            year_week_code,
            count(customer_level) customer_level_1
        FROM
            ods_mms.training_data_base_merge
        WHERE
            customer_level = '1'
        GROUP BY
            store_code,
            year_week_code
    ) cstm_l2_1
    ON
        tr_base.store_code = cstm_l2_1.store_code
        AND cstm_l2_1.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 2 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 50 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 2 AS VARCHAR)
        )
    
    -- 上2周到店交易的lv2顾客数
    LEFT JOIN (
        SELECT
            store_code,
            year_week_code,
            count(customer_level) customer_level_2
        FROM
            ods_mms.training_data_base_merge
        WHERE
            customer_level = '2'
        GROUP BY
            store_code,
            year_week_code
    ) cstm_l2_2
    ON
        tr_base.store_code = cstm_l2_2.store_code
        AND cstm_l2_2.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 2 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 50 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 2 AS VARCHAR)
        )

    -- 上2周到店交易的lv3顾客数
    LEFT JOIN (
        SELECT
            store_code,
            year_week_code,
            count(customer_level) customer_level_3
        FROM
            ods_mms.training_data_base_merge
        WHERE
            customer_level = '3'
        GROUP BY
            store_code,
            year_week_code
    ) cstm_l2_3
    ON
        tr_base.store_code = cstm_l2_3.store_code
        AND cstm_l2_3.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 2 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 50 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 2 AS VARCHAR)
        )
    
    -- weather_day_most
    LEFT JOIN (
        SELECT DISTINCT
            year_code,
            week_code,
            concat(city_name, '市') city_name,
            first_value(weather_day) OVER (
                PARTITION BY 
                    year_code,
                    week_code,
                    city_name
                ORDER BY weather_day_num DESC
            ) weather_day_most
        FROM ods_mms.weather_day_num
        ORDER BY city_name, year_code, week_code
    ) wdm
    ON
        tr_base.year_code = wdm.year_code
        AND tr_base.week_code = wdm.week_code
        AND tr_base.city_name = wdm.city_name

    -- weather_night_most
    LEFT JOIN (
        SELECT DISTINCT
            year_code,
            week_code,
            concat(city_name, '市') city_name,
            first_value(weather_night) OVER (
                PARTITION BY 
                    year_code,
                    week_code,
                    city_name
                ORDER BY weather_night_num DESC
            ) weather_night_most
        FROM ods_mms.weather_night_num
        ORDER BY city_name, year_code, week_code
    ) wnm
    ON
        tr_base.year_code = wnm.year_code
        AND tr_base.week_code = wnm.week_code
        AND tr_base.city_name = wnm.city_name

    -- tempreture_this_week
    LEFT JOIN
        ods_mms.city_weather_history_statistic tmptr
    ON
        tr_base.city_name = concat(tmptr.city_name, '市')
        AND tr_base.year_week_code = tmptr.year_week_code

    -- tempreture_last_week
    LEFT JOIN
        ods_mms.city_weather_history_statistic tmptr_lw
    ON
        tr_base.city_name = concat(tmptr_lw.city_name, '市')
        AND tmptr_lw.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 1 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 49 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 1 AS VARCHAR)
        )

    -- sales
    LEFT JOIN (
        SELECT
            tr_base.store_code,
            tr_base.skc_code,
            tr_base.year_week_code,
            cast(avg(tr_base.retail_amount) AS DECIMAL(30, 2))  retail_amount_mean,
            cast(sum(tr_base.sales_qty) AS INTEGER)  sales_qty
        FROM
            ods_mms.training_data_base_merge tr_base
        GROUP BY
            tr_base.store_code,
            tr_base.skc_code,
            tr_base.year_week_code
    ) sls
    ON
        tr_base.store_code = sls.store_code
        AND tr_base.skc_code = sls.skc_code
        AND tr_base.year_week_code = sls.year_week_code

    -- sales_last_week
    LEFT JOIN (
        SELECT
            tr_base.store_code,
            tr_base.skc_code,
            tr_base.year_week_code,
            cast(avg(tr_base.discount_rate) AS DECIMAL(20, 2))  discount_rate_mean_last_week,
            cast(sum(tr_base.sales_qty) AS INTEGER)  sales_qty_last_week
        FROM
            ods_mms.training_data_base_merge tr_base
        GROUP BY
            tr_base.store_code,
            tr_base.skc_code,
            tr_base.year_week_code
    ) sls_lw
    ON
        tr_base.store_code = sls_lw.store_code
        AND tr_base.skc_code = sls_lw.skc_code
        AND sls_lw.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 1 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 49 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 1 AS VARCHAR)
        )

    -- sales_last_2week
    LEFT JOIN (
        SELECT
            tr_base.store_code,
            tr_base.skc_code,
            tr_base.year_week_code,
            cast(avg(tr_base.discount_rate) AS DECIMAL(20, 2))  discount_rate_mean_last_2week,
            cast(sum(tr_base.sales_qty) AS INTEGER)  sales_qty_last_2week
        FROM
            ods_mms.training_data_base_merge tr_base
        GROUP BY
            tr_base.store_code,
            tr_base.skc_code,
            tr_base.year_week_code
    ) sls_l2w
    ON
        tr_base.store_code = sls_l2w.store_code
        AND tr_base.skc_code = sls_l2w.skc_code
        AND sls_l2w.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 2 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 50 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 2 AS VARCHAR)
        )

    -- skc_last_week
    LEFT JOIN (
        SELECT
            store_code,
            skc_code,
            concat(year_code, week_code)  year_week_code,
            cast(IF(
                sum(all_order_amount) != 0,
                sum(repeat_order_amount) * 1.0 / sum(all_order_amount),
                0
            )AS DECIMAL(18, 2)) skc_con_sale_rate_last_week
        FROM
            ods_mms.skc_con_sale_rate_merge
        GROUP BY
            store_code,
            skc_code,
            concat(year_code, week_code)
    ) skc_csr_lw
    ON
        skc_csr_lw.store_code = tr_base.store_code
        AND skc_csr_lw.skc_code = tr_base.skc_code
        AND skc_csr_lw.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 1 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 49 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 1 AS VARCHAR)
        )
    
    -- skc_last_2week
    LEFT JOIN (
        SELECT
            store_code,
            skc_code,
            concat(year_code, week_code)  year_week_code,
            cast(IF(
                sum(all_order_amount) != 0,
                sum(repeat_order_amount) * 1.0 / sum(all_order_amount),
                0
            ) AS DECIMAL(18, 2)) skc_con_sale_rate_last_2week
        FROM
            ods_mms.skc_con_sale_rate_merge
        GROUP BY
            store_code,
            skc_code,
            concat(year_code, week_code)
    ) skc_csr_l2w
    ON
        skc_csr_l2w.store_code = tr_base.store_code
        AND skc_csr_l2w.skc_code = tr_base.skc_code
        AND skc_csr_l2w.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 2 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 50 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 2 AS VARCHAR)
        )
    WHERE
        tr_base.year_week_code > (SELECT max(year_week_code) FROM cdm_mms.training_data_store_skc_week_merge);
