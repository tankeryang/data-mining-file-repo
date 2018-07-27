DROP TABLE IF EXISTS cdm_mms.training_data_store_skc_week_merge;


CREATE TABLE IF NOT EXISTS cdm_mms.training_data_store_skc_week_merge (
    store_code                           VARCHAR,
    skc_code                             VARCHAR,
    year_code                            VARCHAR,
    week_code                            VARCHAR,
    year_week_code                       VARCHAR,
    prev_year_week_code                  VARCHAR,
    interval_weeks_to_prev               INTEGER,
    list_dates_year_code                 VARCHAR,
    list_dates_week_code                 VARCHAR,
    list_dates_year_week_code            VARCHAR,
    interval_weeks_to_list               INTEGER,
    city_name                            VARCHAR,
    area_code                            VARCHAR,
    sales_channel                        VARCHAR,
    store_type                           VARCHAR,
    store_level                          VARCHAR,
    main_cate                            VARCHAR,
    sub_cate                             VARCHAR,
    leaf_cate                            VARCHAR,
    color_code                           VARCHAR,
    lining                               VARCHAR,
    customer_level_1_last_week           INTEGER,
    customer_level_2_last_week           INTEGER,
    customer_level_3_last_week           INTEGER,
    special_day_type                     VARCHAR,
    weather_day_most                     VARCHAR,
    weather_night_most                   VARCHAR,
    tempreture_day_highest               TINYINT,
    tempreture_day_avg                   DECIMAL(18, 2),
    tempreture_day_lowest                TINYINT,
    tempreture_day_gap                   TINYINT,
    tempreture_night_highest             TINYINT,
    tempreture_night_avg                 DECIMAL(18, 2),
    tempreture_night_lowest              TINYINT,
    tempreture_night_gap                 TINYINT,
    tempreture_avg_gap                   DECIMAL(18, 2),
    retail_amount_mean                   DECIMAL(30, 2),
    sales_amount_mean                    DECIMAL(30, 2),
    discount_rate_mean                   DECIMAL(20, 2),
    skc_con_sale_rate                    DECIMAL(18, 2)  COMMENT 'join tmp table to calculate',
    sales_qty                            INTEGER
);


INSERT INTO cdm_mms.training_data_store_skc_week_merge
    SELECT DISTINCT
        tr_base.store_code,
        tr_base.skc_code,
        tr_base.year_code,
        tr_base.week_code,
        tr_base.year_week_code,
        IF(itv.prev_year_week_code IS NOT NULL, itv.prev_year_week_code, tr_base.year_week_code),
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
        cast(IF (
            tr_base.year_code = tr_base.list_dates_year_code, 
            cast(tr_base.year_week_code AS INTEGER)
                - cast(concat(tr_base.list_dates_year_code, tr_base.list_dates_week_code) AS INTEGER),
            cast(tr_base.year_week_code AS INTEGER)
                - cast(concat(tr_base.list_dates_year_code, tr_base.list_dates_week_code) AS INTEGER)
                - ((cast(tr_base.year_code AS INTEGER) - cast(tr_base.list_dates_year_code AS INTEGER)) * 48)
        ) AS INTEGER),
        tr_base.city_name,
        tr_base.area_code,
        tr_base.sales_channel,
        tr_base.store_type,
        tr_base.store_level,
        tr_base.main_cate,
        tr_base.sub_cate,
        tr_base.leaf_cate,
        tr_base.color_code,
        tr_base.lining,
        cast(IF(num_1.customer_level_1 IS NOT NULL, num_1.customer_level_1, 0) AS INTEGER),
        cast(IF(num_2.customer_level_2 IS NOT NULL, num_2.customer_level_2, 0) AS INTEGER),
        cast(IF(num_3.customer_level_3 IS NOT NULL, num_3.customer_level_3, 0) AS INTEGER),
        tr_base.special_day_type,
        wdm.weather_day_most,
        wnm.weather_night_most,
        cast(max(tr_base.tempreture_day) AS TINYINT),
        cast(avg(tr_base.tempreture_day) AS DECIMAL(18, 2)),
        cast(min(tr_base.tempreture_day) AS TINYINT),
        cast(max(tr_base.tempreture_day) - min(tr_base.tempreture_day) AS TINYINT),
        cast(max(tr_base.tempreture_night) AS TINYINT),
        cast(avg(tr_base.tempreture_night) AS DECIMAL(18, 2)),
        cast(min(tr_base.tempreture_night) AS TINYINT),
        cast(max(tr_base.tempreture_night) - min(tr_base.tempreture_night) AS TINYINT),
        cast(avg(tr_base.tempreture_day) - avg(tr_base.tempreture_night) AS DECIMAL(18, 2)),
        avg(tr_base.retail_amount),
        avg(tr_base.sales_amount),
        avg(tr_base.discount_rate),
        cast(IF(skc_csr.skc_con_sale_rate IS NOT NULL, skc_csr.skc_con_sale_rate, 0) AS DECIMAL(18, 2)),
        cast(sum(tr_base.sales_qty) AS INTEGER)
    FROM
        ods_mms.training_data_base_merge tr_base
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
    ) num_1
    ON
        tr_base.store_code = num_1.store_code
        AND num_1.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 1 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 49 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 1 AS VARCHAR)
        )
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
    ) num_2
    ON
        tr_base.store_code = num_2.store_code
        AND num_2.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 1 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 49 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 1 AS VARCHAR)
        )
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
    ) num_3
    ON
        tr_base.store_code = num_3.store_code
        AND num_3.year_week_code = IF(
            cast(tr_base.week_code AS INTEGER) - 1 <= 0,
            cast(cast(tr_base.year_week_code AS INTEGER) - 49 AS VARCHAR),
            cast(cast(tr_base.year_week_code AS INTEGER) - 1 AS VARCHAR)
        )
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
            ) weather_night_most
        FROM ods_mms.weather_day_num
        ORDER BY city_name, year_code, week_code
    ) wnm
    ON
        tr_base.year_code = wnm.year_code
        AND tr_base.week_code = wnm.week_code
        AND tr_base.city_name = wnm.city_name
    LEFT JOIN (
        SELECT
            a.terminal_store_code terminal_store_code,
            a.skc_code skc_code,
            a.year_code year_code,
            a.week_code week_code,
            a.same_number * 1.0 / b.total_num skc_con_sale_rate
        FROM (
            SELECT
                terminal_store_code,
                skc_code,
                year_code,
                week_code,
                count(sales_order_no) same_number
            FROM 
                ods_mms.skc_con_sale_rate_merge
            WHERE
                order_ntype = 1
            GROUP BY
                terminal_store_code,
                skc_code,
                year_code,
                week_code
        ) a
        LEFT JOIN (
            SELECT
                terminal_store_code,
                skc_code,
                year_code,
                week_code,
                count(sales_order_no) total_num
            FROM
                ods_mms.skc_con_sale_rate_merge
            GROUP BY
                terminal_store_code,
                skc_code,
                year_code,
                week_code
        ) b
        ON a.terminal_store_code = b.terminal_store_code  
        AND a.skc_code = b.skc_code 
        AND a.year_code = b.year_code
        AND a.week_code = b.week_code
    ) skc_csr
    ON
        skc_csr.terminal_store_code = tr_base.store_code
        AND skc_csr.skc_code = tr_base.skc_code
        AND skc_csr.year_code = tr_base.year_code
        AND skc_csr.week_code = tr_base.week_code
    GROUP BY
        tr_base.store_code,
        tr_base.skc_code,
        tr_base.year_code,
        tr_base.week_code,
        tr_base.year_week_code,
        IF(itv.prev_year_week_code IS NOT NULL, itv.prev_year_week_code, tr_base.year_week_code),
        IF(
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
        ),
        tr_base.list_dates_year_code,
        tr_base.list_dates_week_code,
        concat(tr_base.list_dates_year_code, tr_base.list_dates_week_code),
        IF (
            tr_base.year_code = tr_base.list_dates_year_code, 
            cast(concat(tr_base.year_code, tr_base.week_code) AS INTEGER)
                - cast(concat(tr_base.list_dates_year_code, tr_base.list_dates_week_code) AS INTEGER),
            cast(concat(tr_base.year_code, tr_base.week_code) AS INTEGER)
                - cast(concat(tr_base.list_dates_year_code, tr_base.list_dates_week_code) AS INTEGER)
                - ((cast(tr_base.year_code AS INTEGER) - cast(tr_base.list_dates_year_code AS INTEGER)) * 48)
        ),
        tr_base.city_name,
        tr_base.area_code,
        tr_base.sales_channel,
        tr_base.store_type,
        tr_base.store_level,
        tr_base.main_cate,
        tr_base.sub_cate,
        tr_base.leaf_cate,
        tr_base.color_code,
        tr_base.lining,
        IF(num_1.customer_level_1 IS NOT NULL, num_1.customer_level_1, 0),
        IF(num_2.customer_level_2 IS NOT NULL, num_2.customer_level_2, 0),
        IF(num_3.customer_level_3 IS NOT NULL, num_3.customer_level_3, 0),
        tr_base.special_day_type,
        wdm.weather_day_most,
        wnm.weather_night_most,
        IF(skc_csr.skc_con_sale_rate IS NOT NULL, skc_csr.skc_con_sale_rate, 0);
