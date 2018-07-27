DROP TABLE IF EXISTS ods_predict.city_weather_future;


CREATE TABLE IF NOT EXISTS ods_predict.city_weather_future (
    area_id           VARCHAR,
    city_name         VARCHAR,
    dates             VARCHAR,
    year_code         VARCHAR,
    month_code        VARCHAR,
    week_code         VARCHAR,
    year_week_code    VARCHAR,
    weather_day       VARCHAR,
    weather_day_id    VARCHAR,
    weather_night     VARCHAR,
    weather_night_id  VARCHAR,
    tempreture_day    BIGINT,
    tempreture_night  BIGINT,
    wind              VARCHAR,
    wind_id           VARCHAR,
    wind_comp         BIGINT,
    sunrise           VARCHAR,
    sunset            VARCHAr
);


INSERT INTO ods_predict.city_weather_future
    SELECT
        cwf.area_id,
        ct_ar.city,
        cwf.dates,
        cast(year(date_parse(cwf.dates, '%Y-%m-%d')) AS VARCHAR),
        IF(
            month(date_parse(cwf.dates, '%Y-%m-%d')) < 10,
            concat('0', cast(month(date_parse(cwf.dates, '%Y-%m-%d')) AS VARCHAR)),
            cast(month(date_parse(cwf.dates, '%Y-%m-%d')) AS VARCHAR)
        ),
        IF(
            week(date_parse(cwf.dates, '%Y-%m-%d')) < 10,
            concat('0', cast(week(date_parse(cwf.dates, '%Y-%m-%d')) AS VARCHAR)),
            cast(week(date_parse(cwf.dates, '%Y-%m-%d')) AS VARCHAR)
        ),
        concat(cast(year(date_parse(cwf.dates, '%Y-%m-%d')) AS VARCHAR), IF(
            week(date_parse(cwf.dates, '%Y-%m-%d')) < 10,
            concat('0', cast(week(date_parse(cwf.dates, '%Y-%m-%d')) AS VARCHAR)),
            cast(week(date_parse(cwf.dates, '%Y-%m-%d')) AS VARCHAR)
        )),
        cwf.weather_day,
        cwf.weather_day_id,
        cwf.weather_night,
        cwf.weather_night_id,
        cwf.tempreture_day,
        cwf.tempreture_night,
        cwf.wind,
        cwf.wind_id,
        cwf.wind_comp,
        cwf.sunrise,
        cwf.sunset
    FROM ods_test_.city_weather_future cwf
    LEFT JOIN ods_test_.weather_support_area ct_ar
    ON cwf.area_id = ct_ar.areaid;
