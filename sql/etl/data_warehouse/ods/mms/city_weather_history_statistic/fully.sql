DROP TABLE IF EXISTS ods_mms.city_weather_history_statistic;


CREATE TABLE IF NOT EXISTS ods_mms.city_weather_history_statistic (
    city_id                   VARCHAR,
    city_name                 VARCHAR,
    year_week_code            VARCHAR,
    tempreture_day_highest    TINYINT,
    tempreture_day_avg        DECIMAL(18, 2),
    tempreture_day_lowest     TINYINT,
    tempreture_day_gap        TINYINT,
    tempreture_night_highest  TINYINT,
    tempreture_night_avg      DECIMAL(18, 2),
    tempreture_night_lowest   TINYINT,
    tempreture_night_gap      TINYINT,
    tempreture_avg_gap        DECIMAL(18, 2)
);


INSERT INTO ods_mms.city_weather_history_statistic
    SELECT
        city_id,
        city_name,
        concat(
            cast(year(date_parse(dates, '%Y-%m-%d')) AS VARCHAR), 
            IF(
                week(date_parse(dates, '%Y-%m-%d')) < 10,
                concat('0', cast(week(date_parse(dates, '%Y-%m-%d')) AS VARCHAR)),
                cast(week(date_parse(dates, '%Y-%m-%d')) AS VARCHAR)
            )
        )  year_week_code,
        cast(max(tempreture_day) AS TINYINT)  tempreture_day_highest,
        cast(avg(tempreture_day) AS DECIMAL(18, 2))  tempreture_day_avg,
        cast(min(tempreture_day) AS TINYINT)  tempreture_day_lowest,
        cast(max(tempreture_day) - min(tempreture_day) AS TINYINT)  tempreture_day_gap,
        cast(max(tempreture_night) AS TINYINT)  tempreture_night_highest,
        cast(avg(tempreture_night) AS DECIMAL(18, 2))  tempreture_night_avg,
        cast(min(tempreture_night) AS TINYINT)  tempreture_night_lowest,
        cast(max(tempreture_night) - min(tempreture_night) AS TINYINT)  tempreture_night_gap,
        cast(avg(tempreture_day) - avg(tempreture_night) AS DECIMAL(18, 2)) tempreture_avg_gap
    FROM
        ods_mms.city_weather_history
    GROUP BY
        city_id,
        city_name,
        concat(
            cast(year(date_parse(dates, '%Y-%m-%d')) AS VARCHAR), 
            IF(
                week(date_parse(dates, '%Y-%m-%d')) < 10,
                concat('0', cast(week(date_parse(dates, '%Y-%m-%d')) AS VARCHAR)),
                cast(week(date_parse(dates, '%Y-%m-%d')) AS VARCHAR)
            )
        );
