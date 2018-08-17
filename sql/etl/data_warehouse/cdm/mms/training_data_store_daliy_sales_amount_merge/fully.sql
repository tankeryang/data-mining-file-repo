DROP TABLE IF EXISTS cdm_mms.training_data_store_daliy_sales_amount_merge;


CREATE TABLE cdm_mms.training_data_store_daliy_sales_amount_merge(
    store_code                     VARCHAR,
    dates                          VARCHAR,
    day_of_month                   VARCHAR,
    day_of_week                    VARCHAR,
    special_day_type               VARCHAR,
    city_name                      VARCHAR,
    area_code                      VARCHAR,
    store_type                     VARCHAR,
    store_level                    VARCHAR,
    customer_level_1_last_15day    INTEGER,
    customer_level_1_last_14day    INTEGER,
    customer_level_1_gap           INTEGER,
    customer_level_2_last_15day    INTEGER,
    customer_level_2_last_14day    INTEGER,
    customer_level_2_gap           INTEGER,
    customer_level_3_last_15day    INTEGER,
    customer_level_3_last_14day    INTEGER,
    customer_level_3_gap           INTEGER,
    weather_day                    VARCHAR,
    weather_night                  VARCHAR,
    tempreture_day                 TINYINT,
    tempreture_night               TINYINT,
    tempreture_gap                 TINYINT,
    discount_rate_mean_last_15day  DECIMAL(18, 2),
    discount_rate_mean_last_14day  DECIMAL(18, 2),
    discount_rate_mean_gap         DECIMAL(18, 2),
    -- discount_rate_mean             DECIMAL(18, 2),
    sales_amount_last_15day        DECIMAL(30, 2),
    sales_amount_last_14day        DECIMAL(30, 2),
    sales_amount_gap               DECIMAL(30, 2),
    sales_amount                   DECIMAL(38, 2)
);


INSERT INTO cdm_mms.training_data_store_daliy_sales_amount_merge
    -- customer_level_1,2,3-counts
    WITH
        num_1_ld AS (
            SELECT
                store_code,
                date_parse(dates, '%Y-%m-%d') dates,
                count(customer_level) customer_level_1
            FROM
                ods_mms.training_data_base_merge
            WHERE
                customer_level = '1'
            GROUP BY
                store_code,
                dates
        ),
        num_1_l2d AS (
            SELECT
                store_code,
                date_parse(dates, '%Y-%m-%d') dates,
                count(customer_level) customer_level_1
            FROM
                ods_mms.training_data_base_merge
            WHERE
                customer_level = '1'
            GROUP BY
                store_code,
                dates
        ),
        num_2_ld AS (
            SELECT
                store_code,
                date_parse(dates, '%Y-%m-%d') dates,
                count(customer_level) customer_level_2
            FROM
                ods_mms.training_data_base_merge
            WHERE
                customer_level = '2'
            GROUP BY
                store_code,
                dates
        ),
        num_2_l2d AS (
            SELECT
                store_code,
                date_parse(dates, '%Y-%m-%d') dates,
                count(customer_level) customer_level_2
            FROM
                ods_mms.training_data_base_merge
            WHERE
                customer_level = '2'
            GROUP BY
                store_code,
                dates
        ),
        num_3_ld AS (
            SELECT
                store_code,
                date_parse(dates, '%Y-%m-%d') dates,
                count(customer_level) customer_level_3
            FROM
                ods_mms.training_data_base_merge
            WHERE
                customer_level = '3'
            GROUP BY
                store_code,
                dates
        ),
        num_3_l2d AS (
            SELECT
                store_code,
                date_parse(dates, '%Y-%m-%d') dates,
                count(customer_level) customer_level_3
            FROM
                ods_mms.training_data_base_merge
            WHERE
                customer_level = '3'
            GROUP BY
                store_code,
                dates
        ),
    -- discount_rate
        dr_ld AS (
            SELECT
                store_code,
                date_parse(dates, '%Y-%m-%d') dates,
                avg(discount_rate) discount_rate_mean
            FROM
                ods_mms.training_data_base_merge
            GROUP BY
                store_code,
                dates
        ),
        dr_l2d AS (
            SELECT
                store_code,
                date_parse(dates, '%Y-%m-%d') dates,
                avg(discount_rate) discount_rate_mean
            FROM
                ods_mms.training_data_base_merge
            GROUP BY
                store_code,
                dates
        ),
        -- sales_amount
        sa_ld AS (
            SELECT
                store_code,
                date_parse(dates, '%Y-%m-%d') dates,
                sum(sales_amount) sales_amount
            FROM
                ods_mms.training_data_base_merge
            GROUP BY
                store_code,
                dates
        ),
        sa_l2d AS (
            SELECT
                store_code,
                date_parse(dates, '%Y-%m-%d') dates,
                sum(sales_amount) sales_amount
            FROM
                ods_mms.training_data_base_merge
            GROUP BY
                store_code,
                dates
        )

    SELECT
        tr_base.store_code,
        tr_base.dates,
        tr_base.day_of_month,
        tr_base.day_of_week,
        tr_base.special_day_type,
        tr_base.city_name,
        tr_base.area_code,
        tr_base.store_type,
        tr_base.store_level,

        cast(IF(num_1_l2d.customer_level_1 IS NOT NULL, num_1_l2d.customer_level_1, 0) AS INTEGER),
        cast(IF(num_1_ld.customer_level_1 IS NOT NULL, num_1_ld.customer_level_1, 0) AS INTEGER),
        cast(
            IF(num_1_ld.customer_level_1 IS NOT NULL, num_1_ld.customer_level_1, 0)
            - IF(num_1_l2d.customer_level_1 IS NOT NULL, num_1_l2d.customer_level_1, 0) AS INTEGER
        ),

        cast(IF(num_2_l2d.customer_level_2 IS NOT NULL, num_2_l2d.customer_level_2, 0) AS INTEGER),
        cast(IF(num_2_ld.customer_level_2 IS NOT NULL, num_2_ld.customer_level_2, 0) AS INTEGER),
        cast(
            IF(num_2_ld.customer_level_2 IS NOT NULL, num_2_ld.customer_level_2, 0)
            - IF(num_2_l2d.customer_level_2 IS NOT NULL, num_2_l2d.customer_level_2, 0) AS INTEGER
        ),

        cast(IF(num_3_l2d.customer_level_3 IS NOT NULL, num_3_l2d.customer_level_3, 0) AS INTEGER),
        cast(IF(num_3_ld.customer_level_3 IS NOT NULL, num_3_ld.customer_level_3, 0) AS INTEGER),
        cast(IF(num_3_ld.customer_level_3 IS NOT NULL, num_3_ld.customer_level_3, 0)
            - IF(num_3_l2d.customer_level_3 IS NOT NULL, num_3_l2d.customer_level_3, 0) AS INTEGER
        ),

        tr_base.weather_day,
        tr_base.weather_night,
        tr_base.tempreture_day,
        tr_base.tempreture_night,
        cast(tr_base.tempreture_day - tr_base.tempreture_night AS TINYINT),
        
        cast(IF(dr_l2d.discount_rate_mean IS NOT NULL, dr_l2d.discount_rate_mean, 1) AS DECIMAL(18, 2)),
        cast(IF(dr_ld.discount_rate_mean IS NOT NULL, dr_ld.discount_rate_mean, 1) AS DECIMAL(18, 2)),
        cast(
            (IF(dr_ld.discount_rate_mean IS NOT NULL, dr_ld.discount_rate_mean, 1)
            - IF(dr_l2d.discount_rate_mean IS NOT NULL, dr_l2d.discount_rate_mean, 1))
            AS DECIMAL(18, 2)
        ),

        cast(IF(sa_l2d.sales_amount IS NOT NULL, sa_l2d.sales_amount, 0) AS DECIMAL(30, 2)),
        cast(IF(sa_ld.sales_amount IS NOT NULL, sa_ld.sales_amount, 0) AS DECIMAL(30, 2)),
        cast(
            (IF(sa_ld.sales_amount IS NOT NULL, sa_ld.sales_amount, 0)
            - IF(sa_l2d.sales_amount IS NOT NULL, sa_l2d.sales_amount, 0))
            AS DECIMAL(30, 2)
        ),

        sum(tr_base.sales_amount)

    FROM
        ods_mms.training_data_base_merge tr_base
    LEFT JOIN num_1_ld
    ON
        tr_base.store_code = num_1_ld.store_code
        AND date_parse(tr_base.dates, '%Y-%m-%d') - interval '14' day = num_1_ld.dates
    LEFT JOIN num_2_ld
    ON
        tr_base.store_code = num_2_ld.store_code
        AND date_parse(tr_base.dates, '%Y-%m-%d') - interval '14' day = num_2_ld.dates
    LEFT JOIN num_3_ld
    ON
        tr_base.store_code = num_3_ld.store_code
        AND date_parse(tr_base.dates, '%Y-%m-%d') - interval '14' day = num_3_ld.dates
    LEFT JOIN num_1_l2d
    ON
        tr_base.store_code = num_1_l2d.store_code
        AND date_parse(tr_base.dates, '%Y-%m-%d') - interval '15' day = num_1_l2d.dates
    LEFT JOIN num_2_l2d
    ON
        tr_base.store_code = num_2_l2d.store_code
        AND date_parse(tr_base.dates, '%Y-%m-%d') - interval '15' day = num_2_l2d.dates
    LEFT JOIN num_3_l2d
    ON
        tr_base.store_code = num_3_l2d.store_code
        AND date_parse(tr_base.dates, '%Y-%m-%d') - interval '15' day = num_3_l2d.dates
    LEFT JOIN dr_ld
    ON
        tr_base.store_code = dr_ld.store_code
        AND date_parse(tr_base.dates, '%Y-%m-%d') - interval '14' day = dr_ld.dates
    LEFT JOIN dr_l2d
    ON
        tr_base.store_code = dr_l2d.store_code
        AND date_parse(tr_base.dates, '%Y-%m-%d') - interval '15' day = dr_l2d.dates
    LEFT JOIN sa_ld
    ON
        tr_base.store_code = sa_ld.store_code
        AND date_parse(tr_base.dates, '%Y-%m-%d') - interval '14' day = sa_ld.dates
    LEFT JOIN sa_l2d
    ON
        tr_base.store_code = sa_l2d.store_code
        AND date_parse(tr_base.dates, '%Y-%m-%d') - interval '15' day = sa_l2d.dates
    GROUP BY
        tr_base.store_code,
        tr_base.dates,
        tr_base.day_of_month,
        tr_base.day_of_week,
        tr_base.special_day_type,
        tr_base.city_name,
        tr_base.area_code,
        tr_base.store_type,
        tr_base.store_level,

        cast(IF(num_1_l2d.customer_level_1 IS NOT NULL, num_1_l2d.customer_level_1, 0) AS INTEGER),
        cast(IF(num_1_ld.customer_level_1 IS NOT NULL, num_1_ld.customer_level_1, 0) AS INTEGER),
        cast(
            IF(num_1_ld.customer_level_1 IS NOT NULL, num_1_ld.customer_level_1, 0)
            - IF(num_1_l2d.customer_level_1 IS NOT NULL, num_1_l2d.customer_level_1, 0) AS INTEGER
        ),

        cast(IF(num_2_l2d.customer_level_2 IS NOT NULL, num_2_l2d.customer_level_2, 0) AS INTEGER),
        cast(IF(num_2_ld.customer_level_2 IS NOT NULL, num_2_ld.customer_level_2, 0) AS INTEGER),
        cast(
            IF(num_2_ld.customer_level_2 IS NOT NULL, num_2_ld.customer_level_2, 0)
            - IF(num_2_l2d.customer_level_2 IS NOT NULL, num_2_l2d.customer_level_2, 0) AS INTEGER
        ),

        cast(IF(num_3_l2d.customer_level_3 IS NOT NULL, num_3_l2d.customer_level_3, 0) AS INTEGER),
        cast(IF(num_3_ld.customer_level_3 IS NOT NULL, num_3_ld.customer_level_3, 0) AS INTEGER),
        cast(IF(num_3_ld.customer_level_3 IS NOT NULL, num_3_ld.customer_level_3, 0)
            - IF(num_3_l2d.customer_level_3 IS NOT NULL, num_3_l2d.customer_level_3, 0) AS INTEGER
        ),

        tr_base.weather_day,
        tr_base.weather_night,
        tr_base.tempreture_day,
        tr_base.tempreture_night,
        cast(tr_base.tempreture_day - tr_base.tempreture_night AS TINYINT),
        
        cast(IF(dr_l2d.discount_rate_mean IS NOT NULL, dr_l2d.discount_rate_mean, 1) AS DECIMAL(18, 2)),
        cast(IF(dr_ld.discount_rate_mean IS NOT NULL, dr_ld.discount_rate_mean, 1) AS DECIMAL(18, 2)),
        cast(
            (IF(dr_ld.discount_rate_mean IS NOT NULL, dr_ld.discount_rate_mean, 1)
            - IF(dr_l2d.discount_rate_mean IS NOT NULL, dr_l2d.discount_rate_mean, 1))
            AS DECIMAL(18, 2)
        ),

        cast(IF(sa_l2d.sales_amount IS NOT NULL, sa_l2d.sales_amount, 0) AS DECIMAL(30, 2)),
        cast(IF(sa_ld.sales_amount IS NOT NULL, sa_ld.sales_amount, 0) AS DECIMAL(30, 2)),
        cast(
            (IF(sa_ld.sales_amount IS NOT NULL, sa_ld.sales_amount, 0)
            - IF(sa_l2d.sales_amount IS NOT NULL, sa_l2d.sales_amount, 0))
            AS DECIMAL(30, 2)
        );
