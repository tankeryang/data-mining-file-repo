DROP TABLE IF EXISTS ods_mms.predict_feature_store_info;


CREATE TABLE IF NOT EXISTS ods_mms.predict_feature_store_info (
    store_code   VARCHAR,
    store_type   VARCHAR,
    store_level  VARCHAR,
    city_name    VARCHAR,
    area_code    VARCHAR,
    key          VARCHAR(1)
);


INSERT INTO predict_feature_store_info
    SELECT
        base_str_info.store_code,
        mms_str_info.store_type,
        base_str_info.store_level,
        base_str_info.city_name,
        ods_cic_str_info.area_code,
        'k'
    FROM
        cdm_base.store_info base_str_info
    LEFT JOIN
        ods_mms.store_info mms_str_info
    ON
        base_str_info.store_code = mms_str_info.store_code
    LEFT JOIN
        ods_cic.store_info ods_cic_str_info
    ON
        base_str_info.store_code = ods_cic_str_info.store_code;
