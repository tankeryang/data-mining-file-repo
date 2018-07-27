DROP TABLE IF EXISTS ods_mms.predict_feature_index;


CREATE TABLE IF NOT EXISTS ods_mms.predict_feature_index (
    store_code    VARCHAR,
    skc_code      VARCHAR,
    store_type    VARCHAR,
    store_level   VARCHAR,
    city_name     VARCHAR,
    area_code     VARCHAR,
    list_date     VARCHAR,
    main_cate     VARCHAR,
    sub_cate      VARCHAR,
    leaf_cate     VARCHAR,
    color_code    VARCHAR,
    lining        VARCHAR,
    retail_amount  DECIMAL(10, 2)
);


INSERT INTO ods_mms.predict_feature_index
    SELECT
        st.store_code,
        pd.skc_code,
        st.store_type,
        st.store_level,
        st.city_name,
        st.area_code,
        pd.list_date,
        pd.main_cate,
        pd.sub_cate,
        pd.leaf_cate,
        pd.color_code,
        pd.lining,
        pd.retail_price
    FROM
        ods_mms.predict_feature_store_info st
    FULL JOIN
        ods_mms.predict_feature_product_info pd
    ON
        st.key = pd.key;
