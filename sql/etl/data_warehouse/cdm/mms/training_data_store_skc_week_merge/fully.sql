DROP TABLE IF EXISTS cdm_mms.training_data_store_skc_week_merge;


CREATE TABLE IF NOT EXISTS cdm_mms.training_data_store_skc_week_merge (
    store_code                              VARCHAR         COMMENT '门店编号',
    skc_code                                VARCHAR         COMMENT 'skc编号',
    year_code                               VARCHAR         COMMENT '年',
    week_code                               VARCHAR         COMMENT '周',
    year_week_code                          VARCHAR         COMMENT '年-周',
    prev_year_week_code                     VARCHAR         COMMENT '上一次交易所处年-周',
    interval_weeks_to_prev                  INTEGER         COMMENT '距离上一次交易周数',
    list_dates_year_code                    VARCHAR         COMMENT '上市年',
    list_dates_week_code                    VARCHAR         COMMENT '上市周',
    list_dates_year_week_code               VARCHAR         COMMENT '上市年-周',
    interval_weeks_to_list                  INTEGER         COMMENT '距离上市时间周数',
    -- 门店相关
    city_name                               VARCHAR         COMMENT '城市名',
    area_code                               VARCHAR         COMMENT '地区编码(邮政编码)',
    sales_channel                           VARCHAR         COMMENT '销售类型',
    store_type                              VARCHAR         COMMENT '门店类型',
    store_level                             VARCHAR         COMMENT '门店等级',
    store_retail_amount_highest_8week       DECIMAL(18, 2)  COMMENT '门店8周内的最高零售价',
    store_retail_amount_lowest_8week        DECIMAL(18, 2)  COMMENT '门店8周内的最低零售价',
    store_retail_amount_mean_8week          DECIMAL(18, 2)  COMMENT '门店8周内平均零售价',
    store_retail_amount_gap_8week           DECIMAL(18, 2)  COMMENT '门店8周内最高最低零售价差值',
    store_sales_amount_highest_8week        DECIMAL(18, 2)  COMMENT '门店8周内最高销售价',
    store_sales_amount_lowest_8week         DECIMAL(18, 2)  COMMENT '门店8周内最低销售价',
    store_sales_amount_mean_8week           DECIMAL(18, 2)  COMMENT '门店8周内平均销售价',
    store_sales_amount_gap_8week            DECIMAL(18, 2)  COMMENT '门店8周内最高最低销售价差值',     
    store_discount_rate_mean_8week          DECIMAL(18, 2)  COMMENT '门店8周内的平均折扣率',
    -- skc相关
    main_cate                               VARCHAR         COMMENT '大类',
    sub_cate                                VARCHAR         COMMENT '中类',
    leaf_cate                               VARCHAR         COMMENT '小类',
    leaf_cate_retail_amount_highest_8week   DECIMAL(18, 2)  COMMENT '小类8周内最高零售价',
    leaf_cate_retail_amount_lowest_8week    DECIMAL(18, 2)  COMMENT '小类8周内最低零售价',
    leaf_cate_retail_amount_mean_8week      DECIMAL(18, 2)  COMMENT '小类8周内平均零售价',
    leaf_cate_retail_amount_gap_8week       DECIMAL(18, 2)  COMMENT '小类8周内最高最低零售价差值',
    leaf_cate_sales_amount_highest_8week    DECIMAL(18, 2)  COMMENT '小类8周内最高销售价',
    leaf_cate_sales_amount_lowest_8week     DECIMAL(18, 2)  COMMENT '小类8周内最低销售价',
    leaf_cate_sales_amount_mean_8week       DECIMAL(18, 2)  COMMENT '小类8周内平均销售价',
    leaf_cate_sales_amount_gap_8week        DECIMAL(18, 2)  COMMENT '小类8周内最高最低销售价差值',     
    leaf_cate_discount_rate_mean_8week      DECIMAL(18, 2)  COMMENT '小类8周内的平均折扣率',
    color_code                              VARCHAR         COMMENT '颜色编码',
    lining                                  VARCHAR         COMMENT '面料',
    customer_level_1_last_week              INTEGER         COMMENT '上周到店交易lv1的顾客数',
    customer_level_2_last_week              INTEGER         COMMENT '上周到店交易lv2的顾客数',
    customer_level_3_last_week              INTEGER         COMMENT '上周到店交易lv3的顾客数',
    customer_level_1_proportion_last_week   DECIMAL(18, 2)  COMMENT '上周到店交易lv1的顾客占比',
    customer_level_2_proportion_last_week   DECIMAL(18, 2)  COMMENT '上周到店交易lv2的顾客占比',
    customer_level_3_proportion_last_week   DECIMAL(18, 2)  COMMENT '上周到店交易lv3的顾客占比',
    customer_level_1_last_2week             INTEGER         COMMENT '上2周到店交易lv1的顾客数',
    customer_level_2_last_2week             INTEGER         COMMENT '上2周到店交易lv2的顾客数',
    customer_level_3_last_2week             INTEGER         COMMENT '上2周到店交易lv3的顾客数',
    customer_level_1_proportion_last_2week  DECIMAL(18, 2)  COMMENT '上2周到店交易lv1的顾客占比',
    customer_level_2_proportion_last_2week  DECIMAL(18, 2)  COMMENT '上2周到店交易lv2的顾客占比',
    customer_level_3_proportion_last_2week  DECIMAL(18, 2)  COMMENT '上2周到店交易lv3的顾客占比',
    customer_level_1_gap                    INTEGER         COMMENT '上周与上2周lv1顾客交易的差值',
    customer_level_2_gap                    INTEGER         COMMENT '上周与上2周lv2顾客交易的差值',
    customer_level_3_gap                    INTEGER         COMMENT '上周与上2周lv3顾客交易的差值',
    special_day_type                        VARCHAR         COMMENT '特殊日期类型',
    -- 天气
    weather_day_most                        VARCHAR         COMMENT '一周出现最多的日间天气类型',
    weather_night_most                      VARCHAR         COMMENT '一周出现最多的夜间天气类型',
    tempreture_day_highest                  TINYINT         COMMENT '一周最高日间气温',
    tempreture_day_highest_last_week        TINYINT         COMMENT '上周最高日间气温',
    tempreture_day_highest_gap              TINYINT         COMMENT '这周和上周最高日间温差',
    tempreture_day_lowest                   TINYINT         COMMENT '一周最低日间气温',
    tempreture_day_lowest_last_week         TINYINT         COMMENT '上周最低日间气温',
    tempreture_day_lowest_gap               TINYINT         COMMENT '这周和上周最低日间温差',
    tempreture_day_avg                      DECIMAL(18, 2)  COMMENT '一周平均日间气温',
    tempreture_day_avg_last_week            DECIMAL(18, 2)  COMMENT '上周平均日间气温',
    tempreture_day_avg_gap                  DECIMAL(18, 2)  COMMENT '这周和上周的平均日间气温温差',
    tempreture_day_gap                      TINYINT         COMMENT '一周最高最低日间温差',
    tempreture_day_gap_last_week            TINYINT         COMMENT '上周最高最低日间温差',
    tempreture_night_highest                TINYINT         COMMENT '一周最高夜间气温',
    tempreture_night_highest_last_week      TINYINT         COMMENT '上周最高夜间气温',
    tempreture_night_highest_gap            TINYINT         COMMENT '这周和上周最高夜间温差',
    tempreture_night_lowest                 TINYINT         COMMENT '一周最低夜间气温',
    tempreture_night_lowest_last_week       TINYINT         COMMENT '上周最低夜间气温',
    tempreture_night_lowest_gap             TINYINT         COMMENT '这周和上周最低夜间温差',
    tempreture_night_avg                    DECIMAL(18, 2)  COMMENT '一周平均夜间气温',
    tempreture_night_avg_last_week          DECIMAL(18, 2)  COMMENT '上周平均夜间气温',
    tempreture_night_avg_gap                DECIMAL(18, 2)  COMMENT '这周和上周平均夜间气温温差',
    tempreture_night_gap                    TINYINT         COMMENT '一周最高最低夜间温差',
    tempreture_night_gap_last_week          TINYINT         COMMENT '上周最高最低夜间温差',
    tempreture_avg_gap                      DECIMAL(18, 2)  COMMENT '一周日间夜间平均温差',
    tempreture_avg_gap_last_week            DECIMAL(18, 2)  COMMENT '上周日间夜间平均温差',
    -- 价格相关
    retail_amount_mean                      DECIMAL(30, 2)  COMMENT '平均零售价',
    retail_amount_mean_gap_with_store       DECIMAL(18, 2)  COMMENT 'skc零售价与门店8周内平均零售价的差值',
    -- sales_amount_mean                       DECIMAL(30, 2)  COMMENT '平均销售价',
    -- sales_amount_mean_gap_with_store        DECIMAL(18, 2)  COMMENT 'skc销售价与门店8周内平均销售价的差值'
    -- discount_rate_mean                      DECIMAL(20, 2)  COMMENT '平均折扣率',
    discount_rate_mean_last_week            DECIMAL(20, 2)  COMMENT '上周平均折扣率',
    discount_rate_mean_last_2week           DECIMAL(20, 2)  COMMENT '上2周平均折扣率',
    discount_rate_mean_gap_1_2              DECIMAL(18, 2)  COMMENT '上周和上2周的平均折扣率的差值',
    skc_con_sale_rate_last_week             DECIMAL(20, 2)  COMMENT '上周skc连销率',
    skc_con_sale_rate_last_2week            DECIMAL(20, 2)  COMMENT '上2周skc连销率',
    skc_con_sale_rate_gap_1_2               DECIMAL(18, 2)  COMMENT '上周和上2周的skc连销率的差值',
    sales_qty_last_week                     INTEGER         COMMENT '上周销量',
    sales_qty_last_2week                    INTEGER         COMMENT '上2周销量',
    sales_qty_gap_1_2                       INTEGER         COMMENT '上周和上2周的销量差值',
    sales_qty                               INTEGER         COMMENT '实际销量'
);


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
        );
