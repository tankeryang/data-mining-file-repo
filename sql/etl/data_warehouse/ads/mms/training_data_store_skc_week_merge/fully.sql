DROP TABLE IF EXISTS ads_mms.training_data_store_skc_week_merge;


CREATE TABLE IF NOT EXISTS ads_mms.training_data_store_skc_week_merge (
    store_code                                  VARCHAR,
    skc_code                                    VARCHAR,
    year_code                                   VARCHAR,
    week_code                                   VARCHAR,
    year_week_code                              VARCHAR,
    prev_year_week_code                         VARCHAR,
    interval_weeks_to_prev                      INTEGER,
    list_dates_year_code                        VARCHAR,
    list_dates_week_code                        VARCHAR,
    list_dates_year_week_code                   VARCHAR,
    interval_weeks_to_list                      INTEGER,
    city_name                                   VARCHAR,
    area_code                                   VARCHAR,
    sales_channel                               VARCHAR,
    store_type                                  VARCHAR,
    store_level                                 VARCHAR,
    store_retail_amount_mean_8weeks             DECIMAL(18, 2),
    store_sales_amount_mean_8weeks              DECIMAL(18, 2),
    main_cate                                   VARCHAR,
    sub_cate                                    VARCHAR,
    leaf_cate                                   VARCHAR,
    -- leaf_cate_retail_amount_mean_8weeks         DOUBLE,
    -- leaf_cate_sales_amount_mean_8weeks          DOUBLE,
    color_code                                  VARCHAR,
    lining                                      VARCHAR,
    customer_level_1_proportion_last_week       DECIMAL(18, 2),
    customer_level_2_proportion_last_week       DECIMAL(18, 2),
    customer_level_3_proportion_last_week       DECIMAL(18, 2),
    special_day_type                            VARCHAR,
    weather_day_most                            VARCHAR,
    weather_night_most                          VARCHAR,
    tempreture_day_highest                      TINYINT,
    tempreture_day_avg                          DECIMAL(18, 2),
    tempreture_day_lowest                       TINYINT,
    tempreture_day_gap                          TINYINT,
    tempreture_night_highest                    TINYINT,
    tempreture_night_avg                        DECIMAL(18, 2),
    tempreture_night_lowest                     TINYINT,
    tempreture_night_gap                        TINYINT,
    tempreture_avg_gap                          DECIMAL(18, 2),
    retail_amount_mean                          DECIMAL(30, 2),
    retail_amount_mean_gap_with_store           DECIMAL(18, 2),
    sales_amount_mean                           DECIMAL(30, 2),
    sales_amount_mean_gap_with_store            DECIMAL(18, 2),
    discount_rate_mean_last_4week               DECIMAL(20, 2),
    discount_rate_mean_last_3week               DECIMAL(20, 2),
    discount_rate_mean_last_2week               DECIMAL(20, 2),
    discount_rate_mean_last_week                DECIMAL(20, 2),
    discount_rate_mean                          DECIMAL(20, 2),
    discount_rate_mean_change_rate              DECIMAL(18, 2),
    skc_con_sale_rate_last_week                 DECIMAL(18, 2),
    skc_con_sale_rate                           DECIMAL(18, 2),
    sales_qty_last_2week                        INTEGER,
    sales_qty_last_week                         INTEGER,
    sales_qty_last_week_and_last_2week_gap      INTEGER,
    sales_qty                                   INTEGER,
    sales_qty_type                              VARCHAR
);


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
        cast(str_am_m.store_retail_amount_mean_8weeks AS DECIMAL(18, 2)),
        cast(str_am_m.store_sales_amount_mean_8weeks AS DECIMAL(18, 2)),
        tr_str_skc_w.main_cate,
        tr_str_skc_w.sub_cate,
        tr_str_skc_w.leaf_cate,
        -- lc_agg.leaf_cate_retail_amount_mean_8weeks,
        -- lc_agg.leaf_cate_sales_amount_mean_8weeks,
        tr_str_skc_w.color_code,
        tr_str_skc_w.lining,
        ---
        cast(IF((tr_str_skc_w.customer_level_1_last_week + tr_str_skc_w.customer_level_2_last_week + tr_str_skc_w.customer_level_3_last_week) != 0, tr_str_skc_w.customer_level_1_last_week * 1.0 / (tr_str_skc_w.customer_level_1_last_week + tr_str_skc_w.customer_level_2_last_week + tr_str_skc_w.customer_level_3_last_week), 0) AS DECIMAL(18, 2)),
        ---
        cast(IF((tr_str_skc_w.customer_level_1_last_week + tr_str_skc_w.customer_level_2_last_week + tr_str_skc_w.customer_level_3_last_week) != 0, tr_str_skc_w.customer_level_2_last_week * 1.0 / (tr_str_skc_w.customer_level_1_last_week + tr_str_skc_w.customer_level_2_last_week + tr_str_skc_w.customer_level_3_last_week), 0) AS DECIMAL(18, 2)),
        ---
        cast(IF((tr_str_skc_w.customer_level_1_last_week + tr_str_skc_w.customer_level_2_last_week + tr_str_skc_w.customer_level_3_last_week) != 0, tr_str_skc_w.customer_level_3_last_week * 1.0 / (tr_str_skc_w.customer_level_1_last_week + tr_str_skc_w.customer_level_2_last_week + tr_str_skc_w.customer_level_3_last_week), 0) AS DECIMAL(18, 2)),
        ---
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
        cast((tr_str_skc_w.retail_amount_mean - str_am_m.store_retail_amount_mean_8weeks) AS DECIMAL(18, 2)),
        tr_str_skc_w.sales_amount_mean,
        cast((tr_str_skc_w.sales_amount_mean - str_am_m.store_sales_amount_mean_8weeks) AS DECIMAL(18, 2)),
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
        ---
        tr_str_skc_w.discount_rate_mean,
        ---
        cast((tr_str_skc_w.discount_rate_mean - IF(dr_mean_lw.discount_rate_mean_last_week IS NOT NULL, dr_mean_lw.discount_rate_mean_last_week, tr_str_skc_w.discount_rate_mean)) / IF(dr_mean_lw.discount_rate_mean_last_week IS NOT NULL, dr_mean_lw.discount_rate_mean_last_week,  tr_str_skc_w.discount_rate_mean) AS DECIMAL(18, 2)),

        cast(IF(
            skc_csr_lw.skc_con_sale_rate_last_week IS NOT NULL,
            skc_csr_lw.skc_con_sale_rate_last_week,
            0
        ) AS DECIMAL(18, 2)),

        tr_str_skc_w.skc_con_sale_rate,
        
        cast(IF(
            s_qty_l2w.sales_qty_last_2week IS NOT NULL,
            s_qty_l2w.sales_qty_last_2week,
            0
        ) AS INTEGER),

        cast(IF(
            s_qty_lw.sales_qty_last_week IS NOT NULL,
            s_qty_lw.sales_qty_last_week,
            0
        ) AS INTEGER),

        cast(IF(s_qty_lw.sales_qty_last_week IS NOT NULL, s_qty_lw.sales_qty_last_week, 0) - IF(s_qty_l2w.sales_qty_last_2week IS NOT NULL, s_qty_l2w.sales_qty_last_2week, 0) AS INTEGER),

        tr_str_skc_w.sales_qty,

        (CASE
            WHEN tr_str_skc_w.sales_qty = 1 THEN '1'
            WHEN tr_str_skc_w.sales_qty = 2 THEN '2'
            WHEN tr_str_skc_w.sales_qty IN (3, 4, 5) THEN '3-5'
            ELSE '>5'
        END)

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
                cast(t1.week_code AS INTEGER) - 8 <= 0,
                cast(cast(t1.year_week_code AS INTEGER) - 56 AS VARCHAR),
                cast(cast(t1.year_week_code AS INTEGER) - 8 AS VARCHAR)
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
    --             cast(t1.week_code AS INTEGER) - 8 <= 0,
    --             cast(cast(t1.year_week_code AS INTEGER) - 56 AS VARCHAR),
    --             cast(cast(t1.year_week_code AS INTEGER) - 8 AS VARCHAR)
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
                cast(t1.week_code AS INTEGER) - 4 <= 0,
                cast(cast(t1.year_week_code AS INTEGER) - 52 AS VARCHAR),
                cast(cast(t1.year_week_code AS INTEGER) - 4 AS VARCHAR)
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
                cast(t1.week_code AS INTEGER) - 3 <= 0,
                cast(cast(t1.year_week_code AS INTEGER) - 51 AS VARCHAR),
                cast(cast(t1.year_week_code AS INTEGER) - 3 AS VARCHAR)
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
                cast(t1.week_code AS INTEGER) - 2 <= 0,
                cast(cast(t1.year_week_code AS INTEGER) - 50 AS VARCHAR),
                cast(cast(t1.year_week_code AS INTEGER) - 2 AS VARCHAR)
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
                cast(t1.week_code AS INTEGER) - 1 <= 0,
                cast(cast(t1.year_week_code AS INTEGER) - 49 AS VARCHAR),
                cast(cast(t1.year_week_code AS INTEGER) - 1 AS VARCHAR)
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
                cast(t1.week_code AS INTEGER) - 1 <= 0,
                cast(cast(t1.year_week_code AS INTEGER) - 49 AS VARCHAR),
                cast(cast(t1.year_week_code AS INTEGER) - 1 AS VARCHAR)
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
                cast(t1.week_code AS INTEGER) - 2 <= 0,
                cast(cast(t1.year_week_code AS INTEGER) - 50 AS VARCHAR),
                cast(cast(t1.year_week_code AS INTEGER) - 2 AS VARCHAR)
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
                cast(t1.week_code AS INTEGER) - 1 <= 0,
                cast(cast(t1.year_week_code AS INTEGER) - 49 AS VARCHAR),
                cast(cast(t1.year_week_code AS INTEGER) - 1 AS VARCHAR)
            )
    ) s_qty_lw
    ON
        tr_str_skc_w.store_code = s_qty_lw.store_code
        AND tr_str_skc_w.year_week_code = s_qty_lw.year_week_code
        AND tr_str_skc_w.skc_code = s_qty_lw.skc_code;
