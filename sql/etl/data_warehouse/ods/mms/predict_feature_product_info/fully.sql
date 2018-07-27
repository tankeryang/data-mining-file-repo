DROP TABLE IF EXISTS ods_mms.predict_feature_product_info;


CREATE TABLE IF NOT EXISTS ods_mms.predict_feature_product_info (
    skc_code      VARCHAR,
    list_date     VARCHAR,
    main_cate     VARCHAR,
    sub_cate      VARCHAR,
    leaf_cate     VARCHAR,
    color_code    VARCHAR,
    lining        VARCHAR,
    retail_price  DECIMAL(10, 2),
    key           VARCHAR(1)
);


INSERT INTO ods_mms.predict_feature_product_info
    SELECT DISTINCT
        substr(sku_code, 1, 13),
        list_date,
        main_cate,
        sub_cate,
        leaf_cate,
        color_code,
        lining,
        retail_price,
        'k'
    FROM
        cdm_cic.product_sku
    WHERE
        year >= cast(year(current_date - INTERVAL '1' year) AS VARCHAR);
