INSERT INTO ads_mms.training_data_store_skc_week_merge
    SELECT
        tr_str_skc_w.store_code,
        tr_str_skc_w.skc_code,
        tr_str_skc_w.year_code,
        tr_str_skc_w.week_code,
        tr_str_skc_w.year_week_code,
        tr_str_skc_w.prev_year_week_code,
        tr_str_skc_w.interval_weeks_to_prev,
        tr_str_skc_w.list_dates_year_code,
        tr_str_skc_w.list_dates_week_code,
        tr_str_skc_w.list_dates_year_week_code,
        tr_str_skc_w.interval_weeks_to_list,
        tr_str_skc_w.city_name,
        tr_str_skc_w.area_code,
        tr_str_skc_w.sales_channel,
        tr_str_skc_w.store_type,
        tr_str_skc_w.store_level,
        str_am_m.store_retail_amount_mean_8weeks,
        str_am_m.store_sales_amount_mean_8weeks,
        tr_str_skc_w.main_cate,
        tr_str_skc_w.sub_cate,
        tr_str_skc_w.leaf_cate,
        -- lc_agg.leaf_cate_retail_amount_mean_8weeks,
        -- lc_agg.leaf_cate_sales_amount_mean_8weeks,
        tr_str_skc_w.color_code,
        tr_str_skc_w.lining,
        IF((tr_str_skc_w.customer_level_1_last_week + tr_str_skc_w.customer_level_2_last_week + tr_str_skc_w.customer_level_3_last_week) != 0, tr_str_skc_w.customer_level_1_last_week * 1.0 / (tr_str_skc_w.customer_level_1_last_week + tr_str_skc_w.customer_level_2_last_week + tr_str_skc_w.customer_level_3_last_week), 0),
        IF((tr_str_skc_w.customer_level_1_last_week + tr_str_skc_w.customer_level_2_last_week + tr_str_skc_w.customer_level_3_last_week) != 0, tr_str_skc_w.customer_level_2_last_week * 1.0 / (tr_str_skc_w.customer_level_1_last_week + tr_str_skc_w.customer_level_2_last_week + tr_str_skc_w.customer_level_3_last_week), 0),
        IF((tr_str_skc_w.customer_level_1_last_week + tr_str_skc_w.customer_level_2_last_week + tr_str_skc_w.customer_level_3_last_week) != 0, tr_str_skc_w.customer_level_3_last_week * 1.0 / (tr_str_skc_w.customer_level_1_last_week + tr_str_skc_w.customer_level_2_last_week + tr_str_skc_w.customer_level_3_last_week), 0),
        tr_str_skc_w.special_day_type,
        tr_str_skc_w.weather_day_most,
        tr_str_skc_w.weather_night_most,
        tr_str_skc_w.tempreture_day_highest,
        tr_str_skc_w.tempreture_day_avg,
        tr_str_skc_w.tempreture_day_lowest,
        tr_str_skc_w.tempreture_day_gap,
        tr_str_skc_w.tempreture_night_highest,
        tr_str_skc_w.tempreture_night_avg,
        tr_str_skc_w.tempreture_night_lowest,
        tr_str_skc_w.tempreture_night_gap,
        tr_str_skc_w.tempreture_avg_gap,
        tr_str_skc_w.retail_amount_mean,
        (tr_str_skc_w.retail_amount_mean - str_am_m.store_retail_amount_mean_8weeks),
        tr_str_skc_w.sales_amount_mean,
        (tr_str_skc_w.sales_amount_mean - str_am_m.store_sales_amount_mean_8weeks),
        IF(
            dr_mean_l4w.discount_rate_mean_last_4week IS NOT NULL,
            dr_mean_l4w.discount_rate_mean_last_4week,
            tr_str_skc_w.discount_rate_mean
            -- -1
        ),
        IF(
            dr_mean_l3w.discount_rate_mean_last_3week IS NOT NULL,
            dr_mean_l3w.discount_rate_mean_last_3week,
            tr_str_skc_w.discount_rate_mean
            -- -1
        ),
        IF(
            dr_mean_l2w.discount_rate_mean_last_2week IS NOT NULL,
            dr_mean_l2w.discount_rate_mean_last_2week,
            tr_str_skc_w.discount_rate_mean
            -- -1
        ),
        IF(
            dr_mean_lw.discount_rate_mean_last_week IS NOT NULL,
            dr_mean_lw.discount_rate_mean_last_week,
            tr_str_skc_w.discount_rate_mean
        ),
        tr_str_skc_w.discount_rate_mean,
        
        (tr_str_skc_w.discount_rate_mean - IF(dr_mean_lw.discount_rate_mean_last_week IS NOT NULL, dr_mean_lw.discount_rate_mean_last_week, tr_str_skc_w.discount_rate_mean)) / IF(dr_mean_lw.discount_rate_mean_last_week IS NOT NULL, dr_mean_lw.discount_rate_mean_last_week,  tr_str_skc_w.discount_rate_mean),

        IF(
            skc_csr_lw.skc_con_sale_rate_last_week IS NOT NULL,
            skc_csr_lw.skc_con_sale_rate_last_week,
            0
        ),
        tr_str_skc_w.skc_con_sale_rate,
        
        IF(
            s_qty_l2w.sales_qty_last_2week IS NOT NULL,
            s_qty_l2w.sales_qty_last_2week,
            0
        ),
        IF(
            s_qty_lw.sales_qty_last_week IS NOT NULL,
            s_qty_lw.sales_qty_last_week,
            0
        ),
        IF(s_qty_lw.sales_qty_last_week IS NOT NULL, s_qty_lw.sales_qty_last_week, 0) - IF(s_qty_l2w.sales_qty_last_2week IS NOT NULL, s_qty_l2w.sales_qty_last_2week, 0),

        tr_str_skc_w.sales_qty
    FROM
        cdm_mms.training_data_store_skc_week_merge tr_str_skc_w
    LEFT JOIN (
        SELECT
            t1.store_code,
            t1.year_week_code,
            avg(t2.retail_amount_mean)  store_retail_amount_mean_8weeks,
            avg(t2.sales_amount_mean)  store_sales_amount_mean_8weeks
        FROM
            cdm_mms.training_data_store_skc_week_merge t1
        LEFT JOIN
            cdm_mms.training_data_store_skc_week_merge t2
        ON
            t1.store_code = t2.store_code
            AND t1.year_week_code > t2.year_week_code
            AND t2.year_week_code > IF(
                cast(t1.week_code AS BIGINT) - 8 <= 0,
                cast(cast(t1.year_week_code AS BIGINT) - 56 AS VARCHAR),
                cast(cast(t1.year_week_code AS BIGINT) - 8 AS VARCHAR)
            )
        GROUP BY
            t1.store_code,
            t1.year_week_code
    ) str_am_m
    ON
        tr_str_skc_w.store_code = str_am_m.store_code
        AND tr_str_skc_w.year_week_code = str_am_m.year_week_code
    -- LEFT JOIN (
    --     SELECT
    --         t1.leaf_cate,
    --         t1.year_week_code,
    --         avg(t2.retail_amount_mean) leaf_cate_retail_amount_mean_8weeks,
    --         avg(t2.sales_amount_mean) leaf_cate_sales_amount_mean_8weeks
    --     FROM
    --         cdm_mms.training_data_store_skc_week_merge t1
    --     LEFT JOIN
    --         cdm_mms.training_data_store_skc_week_merge t2
    --     ON
    --         t1.leaf_cate = t2.leaf_cate
    --         AND t1.year_week_code > t2.year_week_code
    --         AND t2.year_week_code > IF(
    --             cast(t1.week_code AS BIGINT) - 8 <= 0,
    --             cast(cast(t1.year_week_code AS BIGINT) - 56 AS VARCHAR),
    --             cast(cast(t1.year_week_code AS BIGINT) - 8 AS VARCHAR)
    --         )
    --     GROUP BY
    --         t1.leaf_cate,
    --         t1.year_week_code
    -- ) lc_agg
    -- ON
    --     tr_str_skc_w.leaf_cate = lc_agg.leaf_cate
    --     AND tr_str_skc_w.year_week_code = lc_agg.year_week_code
    LEFT JOIN (
        SELECT
            t1.store_code,
            t1.year_week_code,
            t1.skc_code,
            t2.discount_rate_mean discount_rate_mean_last_4week
        FROM
            cdm_mms.training_data_store_skc_week_merge t1
        LEFT JOIN
            cdm_mms.training_data_store_skc_week_merge t2
        ON
            t1.store_code = t2.store_code
            AND t1.skc_code = t2.skc_code
            AND t2.year_week_code = IF(
                cast(t1.week_code AS BIGINT) - 4 <= 0,
                cast(cast(t1.year_week_code AS BIGINT) - 52 AS VARCHAR),
                cast(cast(t1.year_week_code AS BIGINT) - 4 AS VARCHAR)
            )
    ) dr_mean_l4w
    ON
        tr_str_skc_w.store_code = dr_mean_l4w.store_code
        AND tr_str_skc_w.year_week_code = dr_mean_l4w.year_week_code
        AND tr_str_skc_w.skc_code = dr_mean_l4w.skc_code
    LEFT JOIN (
        SELECT
            t1.store_code,
            t1.year_week_code,
            t1.skc_code,
            t2.discount_rate_mean discount_rate_mean_last_3week
        FROM
            cdm_mms.training_data_store_skc_week_merge t1
        LEFT JOIN
            cdm_mms.training_data_store_skc_week_merge t2
        ON
            t1.store_code = t2.store_code
            AND t1.skc_code = t2.skc_code
            AND t2.year_week_code = IF(
                cast(t1.week_code AS BIGINT) - 3 <= 0,
                cast(cast(t1.year_week_code AS BIGINT) - 51 AS VARCHAR),
                cast(cast(t1.year_week_code AS BIGINT) - 3 AS VARCHAR)
            )
    ) dr_mean_l3w
    ON
        tr_str_skc_w.store_code = dr_mean_l3w.store_code
        AND tr_str_skc_w.year_week_code = dr_mean_l3w.year_week_code
        AND tr_str_skc_w.skc_code = dr_mean_l3w.skc_code
    LEFT JOIN (
        SELECT
            t1.store_code,
            t1.year_week_code,
            t1.skc_code,
            t2.discount_rate_mean discount_rate_mean_last_2week
        FROM
            cdm_mms.training_data_store_skc_week_merge t1
        LEFT JOIN
            cdm_mms.training_data_store_skc_week_merge t2
        ON
            t1.store_code = t2.store_code
            AND t1.skc_code = t2.skc_code
            AND t2.year_week_code = IF(
                cast(t1.week_code AS BIGINT) - 2 <= 0,
                cast(cast(t1.year_week_code AS BIGINT) - 50 AS VARCHAR),
                cast(cast(t1.year_week_code AS BIGINT) - 2 AS VARCHAR)
            )
    ) dr_mean_l2w
    ON
        tr_str_skc_w.store_code = dr_mean_l2w.store_code
        AND tr_str_skc_w.year_week_code = dr_mean_l2w.year_week_code
        AND tr_str_skc_w.skc_code = dr_mean_l2w.skc_code
    LEFT JOIN (
        SELECT
            t1.store_code,
            t1.year_week_code,
            t1.skc_code,
            t2.discount_rate_mean discount_rate_mean_last_week
        FROM
            cdm_mms.training_data_store_skc_week_merge t1
        LEFT JOIN
            cdm_mms.training_data_store_skc_week_merge t2
        ON
            t1.store_code = t2.store_code
            AND t1.skc_code = t2.skc_code
            AND t2.year_week_code = IF(
                cast(t1.week_code AS BIGINT) - 1 <= 0,
                cast(cast(t1.year_week_code AS BIGINT) - 49 AS VARCHAR),
                cast(cast(t1.year_week_code AS BIGINT) - 1 AS VARCHAR)
            )
    ) dr_mean_lw
    ON
        tr_str_skc_w.store_code = dr_mean_lw.store_code
        AND tr_str_skc_w.year_week_code = dr_mean_lw.year_week_code
        AND tr_str_skc_w.skc_code = dr_mean_lw.skc_code
    LEFT JOIN (
        SELECT
            t1.store_code,
            t1.year_week_code,
            t1.skc_code,
            t2.skc_con_sale_rate skc_con_sale_rate_last_week
        FROM
            cdm_mms.training_data_store_skc_week_merge t1
        LEFT JOIN
            cdm_mms.training_data_store_skc_week_merge t2
        ON
            t1.store_code = t2.store_code
            AND t1.skc_code = t2.skc_code
            AND t2.year_week_code = IF(
                cast(t1.week_code AS BIGINT) - 1 <= 0,
                cast(cast(t1.year_week_code AS BIGINT) - 49 AS VARCHAR),
                cast(cast(t1.year_week_code AS BIGINT) - 1 AS VARCHAR)
            )
    ) skc_csr_lw
    ON
        tr_str_skc_w.store_code = skc_csr_lw.store_code
        AND tr_str_skc_w.year_week_code = skc_csr_lw.year_week_code
        AND tr_str_skc_w.skc_code = skc_csr_lw.skc_code
    LEFT JOIN (
        SELECT
            t1.store_code,
            t1.year_week_code,
            t1.skc_code,
            t2.sales_qty sales_qty_last_2week
        FROM
            cdm_mms.training_data_store_skc_week_merge t1
        LEFT JOIN
            cdm_mms.training_data_store_skc_week_merge t2
        ON
            t1.store_code = t2.store_code
            AND t1.skc_code = t2.skc_code
            AND t2.year_week_code = IF(
                cast(t1.week_code AS BIGINT) - 2 <= 0,
                cast(cast(t1.year_week_code AS BIGINT) - 50 AS VARCHAR),
                cast(cast(t1.year_week_code AS BIGINT) - 2 AS VARCHAR)
            )
    ) s_qty_l2w
    ON
        tr_str_skc_w.store_code = s_qty_l2w.store_code
        AND tr_str_skc_w.year_week_code = s_qty_l2w.year_week_code
        AND tr_str_skc_w.skc_code = s_qty_l2w.skc_code
    LEFT JOIN (
        SELECT
            t1.store_code,
            t1.year_week_code,
            t1.skc_code,
            t2.sales_qty sales_qty_last_week
        FROM
            cdm_mms.training_data_store_skc_week_merge t1
        LEFT JOIN
            cdm_mms.training_data_store_skc_week_merge t2
        ON
            t1.store_code = t2.store_code
            AND t1.skc_code = t2.skc_code
            AND t2.year_week_code = IF(
                cast(t1.week_code AS BIGINT) - 1 <= 0,
                cast(cast(t1.year_week_code AS BIGINT) - 49 AS VARCHAR),
                cast(cast(t1.year_week_code AS BIGINT) - 1 AS VARCHAR)
            )
    ) s_qty_lw
    ON
        tr_str_skc_w.store_code = s_qty_lw.store_code
        AND tr_str_skc_w.year_week_code = s_qty_lw.year_week_code
        AND tr_str_skc_w.skc_code = s_qty_lw.skc_code
    WHERE
        tr_str_skc_w.year_week_code > (SELECT max(year_week_code) FROM ads_mms.training_data_store_skc_week_merge);
