DROP TABLE IF EXISTS ods_mms.weather_day_num;


CREATE TABLE IF NOT EXISTS ods_mms.weather_day_num (
    year_code        VARCHAR,
    week_code        VARCHAR,
    city_name        VARCHAR,
    weather_day      VARCHAR,
    weather_day_num  TINYINT
);


INSERT INTO ods_mms.weather_day_num
    SELECT
        cast(year(date_parse(cwh.dates, '%Y-%m-%d')) AS VARCHAR)  year_code,
        IF(
            week(date_parse(cwh.dates, '%Y-%m-%d')) < 10,
            concat('0', cast(week(date_parse(cwh.dates, '%Y-%m-%d')) AS VARCHAR)),
            cast(week(date_parse(cwh.dates, '%Y-%m-%d')) AS VARCHAR)
        )                                                         week_code,
        cwh.city_name                                             city_name,
        cwh.weather_day                                           weather_day,
        cast(count(cwh.weather_day) AS TINYINT)                   weather_day_num
    FROM
        ods_mms.city_weather_history cwh
    GROUP BY
        cast(year(date_parse(cwh.dates, '%Y-%m-%d')) AS VARCHAR),
        IF(
            week(date_parse(cwh.dates, '%Y-%m-%d')) < 10,
            concat('0', cast(week(date_parse(cwh.dates, '%Y-%m-%d')) AS VARCHAR)),
            cast(week(date_parse(cwh.dates, '%Y-%m-%d')) AS VARCHAR)
        ),
        cwh.city_name,
        cwh.weather_day
    ORDER BY
        year_code,
        week_code,
        weather_day,
        weather_day_num
    DESC;
