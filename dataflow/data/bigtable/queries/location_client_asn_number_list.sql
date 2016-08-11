
        select
        location_id,
client_asn_number,
 client_asn_name,
type,
client_city,
client_region,
client_region_code,
client_country,
client_country_code,
client_continent,
client_continent_code,
 last_week_test_count,
last_week_download_speed_mbps_median,
last_week_upload_speed_mbps_median,
last_week_download_speed_mbps_avg,
last_week_upload_speed_mbps_avg,
last_week_download_speed_mbps_min,
last_week_upload_speed_mbps_min,
last_week_download_speed_mbps_max,
last_week_upload_speed_mbps_max,
last_week_download_speed_mbps_stddev,
last_week_upload_speed_mbps_stddev,
last_month_test_count,
last_month_download_speed_mbps_median,
last_month_upload_speed_mbps_median,
last_month_download_speed_mbps_avg,
last_month_upload_speed_mbps_avg,
last_month_download_speed_mbps_min,
last_month_upload_speed_mbps_min,
last_month_download_speed_mbps_max,
last_month_upload_speed_mbps_max,
last_month_download_speed_mbps_stddev,
last_month_upload_speed_mbps_stddev,
last_year_test_count,
last_year_download_speed_mbps_median,
last_year_upload_speed_mbps_median,
last_year_download_speed_mbps_avg,
last_year_upload_speed_mbps_avg,
last_year_download_speed_mbps_min,
last_year_upload_speed_mbps_min,
last_year_download_speed_mbps_max,
last_year_upload_speed_mbps_max,
last_year_download_speed_mbps_stddev,
last_year_upload_speed_mbps_stddev,
        "0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64,68,72,76,80,84,88,92,96,100" as speed_mbps_bins,
        last_week_download_speed_mbps_bins,
last_week_upload_speed_mbps_bins,
last_month_download_speed_mbps_bins,
last_month_upload_speed_mbps_bins,
last_year_download_speed_mbps_bins,
last_year_upload_speed_mbps_bins
        from
    -- ============
-- Type: city
-- ============
(SELECT
  REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), ""
,IFNULL(all.client_country_code, ""), ""
,IFNULL(all.client_region_code, ""), ""
,IFNULL(all.client_city, ""), ""
)), " ", "") as location_id,

  -- what is its type?
  "city" AS type,

  -- meta fields we are selecting
  all.client_asn_number as client_asn_number,
all.client_asn_name as client_asn_name,
all.client_city as client_city,
all.client_region as client_region,
all.client_country as client_country,
all.client_continent as client_continent,
all.client_region_code as client_region_code,
all.client_country_code as client_country_code,
all.client_continent_code as client_continent_code,

  -- timed fields
  last_week.test_count as last_week_test_count,
last_week.download_speed_mbps_median as last_week_download_speed_mbps_median,
last_week.upload_speed_mbps_median as last_week_upload_speed_mbps_median,
last_week.download_speed_mbps_avg as last_week_download_speed_mbps_avg,
last_week.upload_speed_mbps_avg as last_week_upload_speed_mbps_avg,
last_week.download_speed_mbps_min as last_week_download_speed_mbps_min,
last_week.upload_speed_mbps_min as last_week_upload_speed_mbps_min,
last_week.download_speed_mbps_max as last_week_download_speed_mbps_max,
last_week.upload_speed_mbps_max as last_week_upload_speed_mbps_max,
last_week.download_speed_mbps_stddev as last_week_download_speed_mbps_stddev,
last_week.upload_speed_mbps_stddev as last_week_upload_speed_mbps_stddev,
last_month.test_count as last_month_test_count,
last_month.download_speed_mbps_median as last_month_download_speed_mbps_median,
last_month.upload_speed_mbps_median as last_month_upload_speed_mbps_median,
last_month.download_speed_mbps_avg as last_month_download_speed_mbps_avg,
last_month.upload_speed_mbps_avg as last_month_upload_speed_mbps_avg,
last_month.download_speed_mbps_min as last_month_download_speed_mbps_min,
last_month.upload_speed_mbps_min as last_month_upload_speed_mbps_min,
last_month.download_speed_mbps_max as last_month_download_speed_mbps_max,
last_month.upload_speed_mbps_max as last_month_upload_speed_mbps_max,
last_month.download_speed_mbps_stddev as last_month_download_speed_mbps_stddev,
last_month.upload_speed_mbps_stddev as last_month_upload_speed_mbps_stddev,
last_year.test_count as last_year_test_count,
last_year.download_speed_mbps_median as last_year_download_speed_mbps_median,
last_year.upload_speed_mbps_median as last_year_upload_speed_mbps_median,
last_year.download_speed_mbps_avg as last_year_download_speed_mbps_avg,
last_year.upload_speed_mbps_avg as last_year_upload_speed_mbps_avg,
last_year.download_speed_mbps_min as last_year_download_speed_mbps_min,
last_year.upload_speed_mbps_min as last_year_upload_speed_mbps_min,
last_year.download_speed_mbps_max as last_year_download_speed_mbps_max,
last_year.upload_speed_mbps_max as last_year_upload_speed_mbps_max,
last_year.download_speed_mbps_stddev as last_year_download_speed_mbps_stddev,
last_year.upload_speed_mbps_stddev as last_year_upload_speed_mbps_stddev,
last_month.download_speed_mbps_bins as last_month_download_speed_mbps_bins,
last_month.upload_speed_mbps_bins as last_month_upload_speed_mbps_bins,
last_week.download_speed_mbps_bins as last_week_download_speed_mbps_bins,
last_week.upload_speed_mbps_bins as last_week_upload_speed_mbps_bins,
last_year.download_speed_mbps_bins as last_year_download_speed_mbps_bins,
last_year.upload_speed_mbps_bins as last_year_upload_speed_mbps_bins

  FROM {0} all

  -- left join madness here!
  left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    client_asn_number,
client_asn_name,
client_city,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code,

    -- measurements:
    nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
    nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

    AVG(download_speed_mbps) AS download_speed_mbps_avg,
    AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

    MIN(download_speed_mbps) AS download_speed_mbps_min,
    MAX(download_speed_mbps) AS download_speed_mbps_max,

    MIN(upload_speed_mbps) AS upload_speed_mbps_min,
    MAX(upload_speed_mbps) AS upload_speed_mbps_max,

    STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
    STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

    -- bins:
    concat(STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 0 AND IFNULL(download_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 12 AND IFNULL(download_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 16 AND IFNULL(download_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 20 AND IFNULL(download_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 24 AND IFNULL(download_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 28 AND IFNULL(download_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 32 AND IFNULL(download_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 36 AND IFNULL(download_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 40 AND IFNULL(download_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 44 AND IFNULL(download_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 48 AND IFNULL(download_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 52 AND IFNULL(download_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 56 AND IFNULL(download_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 60 AND IFNULL(download_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 64 AND IFNULL(download_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 68 AND IFNULL(download_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 72 AND IFNULL(download_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 76 AND IFNULL(download_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 80 AND IFNULL(download_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 84 AND IFNULL(download_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 88 AND IFNULL(download_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 92 AND IFNULL(download_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 96 AND IFNULL(download_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
) as download_speed_mbps_bins,
concat(STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 0 AND IFNULL(upload_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 4 AND IFNULL(upload_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 8 AND IFNULL(upload_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 12 AND IFNULL(upload_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 16 AND IFNULL(upload_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 20 AND IFNULL(upload_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 24 AND IFNULL(upload_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 28 AND IFNULL(upload_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 32 AND IFNULL(upload_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 36 AND IFNULL(upload_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 40 AND IFNULL(upload_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 44 AND IFNULL(upload_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 48 AND IFNULL(upload_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 52 AND IFNULL(upload_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 56 AND IFNULL(upload_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 60 AND IFNULL(upload_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 64 AND IFNULL(upload_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 68 AND IFNULL(upload_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 72 AND IFNULL(upload_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 76 AND IFNULL(upload_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 80 AND IFNULL(upload_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 84 AND IFNULL(upload_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 88 AND IFNULL(upload_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 92 AND IFNULL(upload_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 96 AND IFNULL(upload_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 100, 1,0)))
) as upload_speed_mbps_bins

  from {0}
  where
    test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH") and
    client_city is not null
  group by
    -- group by location fields
    client_asn_number,
client_asn_name,
client_city,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code

) last_month on
  -- join on location fields from the all table.
  all.client_asn_number = last_month.client_asn_number and
all.client_asn_name = last_month.client_asn_name and
all.client_city = last_month.client_city and
all.client_region = last_month.client_region and
all.client_country = last_month.client_country and
all.client_continent = last_month.client_continent and
all.client_region_code = last_month.client_region_code and
all.client_country_code = last_month.client_country_code and
all.client_continent_code = last_month.client_continent_code
left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    client_asn_number,
client_asn_name,
client_city,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code,

    -- measurements:
    nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
    nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

    AVG(download_speed_mbps) AS download_speed_mbps_avg,
    AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

    MIN(download_speed_mbps) AS download_speed_mbps_min,
    MAX(download_speed_mbps) AS download_speed_mbps_max,

    MIN(upload_speed_mbps) AS upload_speed_mbps_min,
    MAX(upload_speed_mbps) AS upload_speed_mbps_max,

    STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
    STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

    -- bins:
    concat(STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 0 AND IFNULL(download_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 12 AND IFNULL(download_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 16 AND IFNULL(download_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 20 AND IFNULL(download_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 24 AND IFNULL(download_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 28 AND IFNULL(download_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 32 AND IFNULL(download_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 36 AND IFNULL(download_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 40 AND IFNULL(download_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 44 AND IFNULL(download_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 48 AND IFNULL(download_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 52 AND IFNULL(download_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 56 AND IFNULL(download_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 60 AND IFNULL(download_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 64 AND IFNULL(download_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 68 AND IFNULL(download_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 72 AND IFNULL(download_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 76 AND IFNULL(download_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 80 AND IFNULL(download_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 84 AND IFNULL(download_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 88 AND IFNULL(download_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 92 AND IFNULL(download_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 96 AND IFNULL(download_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
) as download_speed_mbps_bins,
concat(STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 0 AND IFNULL(upload_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 4 AND IFNULL(upload_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 8 AND IFNULL(upload_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 12 AND IFNULL(upload_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 16 AND IFNULL(upload_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 20 AND IFNULL(upload_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 24 AND IFNULL(upload_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 28 AND IFNULL(upload_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 32 AND IFNULL(upload_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 36 AND IFNULL(upload_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 40 AND IFNULL(upload_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 44 AND IFNULL(upload_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 48 AND IFNULL(upload_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 52 AND IFNULL(upload_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 56 AND IFNULL(upload_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 60 AND IFNULL(upload_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 64 AND IFNULL(upload_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 68 AND IFNULL(upload_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 72 AND IFNULL(upload_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 76 AND IFNULL(upload_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 80 AND IFNULL(upload_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 84 AND IFNULL(upload_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 88 AND IFNULL(upload_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 92 AND IFNULL(upload_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 96 AND IFNULL(upload_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 100, 1,0)))
) as upload_speed_mbps_bins

  from {0}
  where
    test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, "DAY") and
    client_city is not null
  group by
    -- group by location fields
    client_asn_number,
client_asn_name,
client_city,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code

) last_week on
  -- join on location fields from the all table.
  all.client_asn_number = last_week.client_asn_number and
all.client_asn_name = last_week.client_asn_name and
all.client_city = last_week.client_city and
all.client_region = last_week.client_region and
all.client_country = last_week.client_country and
all.client_continent = last_week.client_continent and
all.client_region_code = last_week.client_region_code and
all.client_country_code = last_week.client_country_code and
all.client_continent_code = last_week.client_continent_code
left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    client_asn_number,
client_asn_name,
client_city,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code,

    -- measurements:
    nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
    nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

    AVG(download_speed_mbps) AS download_speed_mbps_avg,
    AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

    MIN(download_speed_mbps) AS download_speed_mbps_min,
    MAX(download_speed_mbps) AS download_speed_mbps_max,

    MIN(upload_speed_mbps) AS upload_speed_mbps_min,
    MAX(upload_speed_mbps) AS upload_speed_mbps_max,

    STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
    STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

    -- bins:
    concat(STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 0 AND IFNULL(download_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 12 AND IFNULL(download_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 16 AND IFNULL(download_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 20 AND IFNULL(download_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 24 AND IFNULL(download_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 28 AND IFNULL(download_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 32 AND IFNULL(download_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 36 AND IFNULL(download_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 40 AND IFNULL(download_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 44 AND IFNULL(download_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 48 AND IFNULL(download_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 52 AND IFNULL(download_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 56 AND IFNULL(download_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 60 AND IFNULL(download_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 64 AND IFNULL(download_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 68 AND IFNULL(download_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 72 AND IFNULL(download_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 76 AND IFNULL(download_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 80 AND IFNULL(download_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 84 AND IFNULL(download_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 88 AND IFNULL(download_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 92 AND IFNULL(download_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 96 AND IFNULL(download_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
) as download_speed_mbps_bins,
concat(STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 0 AND IFNULL(upload_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 4 AND IFNULL(upload_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 8 AND IFNULL(upload_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 12 AND IFNULL(upload_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 16 AND IFNULL(upload_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 20 AND IFNULL(upload_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 24 AND IFNULL(upload_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 28 AND IFNULL(upload_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 32 AND IFNULL(upload_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 36 AND IFNULL(upload_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 40 AND IFNULL(upload_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 44 AND IFNULL(upload_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 48 AND IFNULL(upload_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 52 AND IFNULL(upload_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 56 AND IFNULL(upload_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 60 AND IFNULL(upload_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 64 AND IFNULL(upload_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 68 AND IFNULL(upload_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 72 AND IFNULL(upload_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 76 AND IFNULL(upload_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 80 AND IFNULL(upload_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 84 AND IFNULL(upload_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 88 AND IFNULL(upload_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 92 AND IFNULL(upload_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 96 AND IFNULL(upload_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 100, 1,0)))
) as upload_speed_mbps_bins

  from {0}
  where
    test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR") and
    client_city is not null
  group by
    -- group by location fields
    client_asn_number,
client_asn_name,
client_city,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code

) last_year on
  -- join on location fields from the all table.
  all.client_asn_number = last_year.client_asn_number and
all.client_asn_name = last_year.client_asn_name and
all.client_city = last_year.client_city and
all.client_region = last_year.client_region and
all.client_country = last_year.client_country and
all.client_continent = last_year.client_continent and
all.client_region_code = last_year.client_region_code and
all.client_country_code = last_year.client_country_code and
all.client_continent_code = last_year.client_continent_code


  GROUP BY

  location_id,
client_asn_number,
client_asn_number,
client_asn_name,
client_city,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code,
last_week_test_count,
last_week_download_speed_mbps_median,
last_week_upload_speed_mbps_median,
last_week_download_speed_mbps_avg,
last_week_upload_speed_mbps_avg,
last_week_download_speed_mbps_min,
last_week_upload_speed_mbps_min,
last_week_download_speed_mbps_max,
last_week_upload_speed_mbps_max,
last_week_download_speed_mbps_stddev,
last_week_upload_speed_mbps_stddev,
last_month_test_count,
last_month_download_speed_mbps_median,
last_month_upload_speed_mbps_median,
last_month_download_speed_mbps_avg,
last_month_upload_speed_mbps_avg,
last_month_download_speed_mbps_min,
last_month_upload_speed_mbps_min,
last_month_download_speed_mbps_max,
last_month_upload_speed_mbps_max,
last_month_download_speed_mbps_stddev,
last_month_upload_speed_mbps_stddev,
last_year_test_count,
last_year_download_speed_mbps_median,
last_year_upload_speed_mbps_median,
last_year_download_speed_mbps_avg,
last_year_upload_speed_mbps_avg,
last_year_download_speed_mbps_min,
last_year_upload_speed_mbps_min,
last_year_download_speed_mbps_max,
last_year_upload_speed_mbps_max,
last_year_download_speed_mbps_stddev,
last_year_upload_speed_mbps_stddev,
last_month_download_speed_mbps_bins,
last_month_upload_speed_mbps_bins,
last_week_download_speed_mbps_bins,
last_week_upload_speed_mbps_bins,
last_year_download_speed_mbps_bins,
last_year_upload_speed_mbps_bins

)
,
-- ============
-- Type: region
-- ============
(SELECT
  REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), ""
,IFNULL(all.client_country_code, ""), ""
,IFNULL(all.client_region_code, ""), ""
)), " ", "") as location_id,

  -- what is its type?
  "region" AS type,

  -- meta fields we are selecting
  all.client_asn_number as client_asn_number,
all.client_asn_name as client_asn_name,
all.client_region as client_region,
all.client_country as client_country,
all.client_continent as client_continent,
all.client_region_code as client_region_code,
all.client_country_code as client_country_code,
all.client_continent_code as client_continent_code,

  -- timed fields
  last_week.test_count as last_week_test_count,
last_week.download_speed_mbps_median as last_week_download_speed_mbps_median,
last_week.upload_speed_mbps_median as last_week_upload_speed_mbps_median,
last_week.download_speed_mbps_avg as last_week_download_speed_mbps_avg,
last_week.upload_speed_mbps_avg as last_week_upload_speed_mbps_avg,
last_week.download_speed_mbps_min as last_week_download_speed_mbps_min,
last_week.upload_speed_mbps_min as last_week_upload_speed_mbps_min,
last_week.download_speed_mbps_max as last_week_download_speed_mbps_max,
last_week.upload_speed_mbps_max as last_week_upload_speed_mbps_max,
last_week.download_speed_mbps_stddev as last_week_download_speed_mbps_stddev,
last_week.upload_speed_mbps_stddev as last_week_upload_speed_mbps_stddev,
last_month.test_count as last_month_test_count,
last_month.download_speed_mbps_median as last_month_download_speed_mbps_median,
last_month.upload_speed_mbps_median as last_month_upload_speed_mbps_median,
last_month.download_speed_mbps_avg as last_month_download_speed_mbps_avg,
last_month.upload_speed_mbps_avg as last_month_upload_speed_mbps_avg,
last_month.download_speed_mbps_min as last_month_download_speed_mbps_min,
last_month.upload_speed_mbps_min as last_month_upload_speed_mbps_min,
last_month.download_speed_mbps_max as last_month_download_speed_mbps_max,
last_month.upload_speed_mbps_max as last_month_upload_speed_mbps_max,
last_month.download_speed_mbps_stddev as last_month_download_speed_mbps_stddev,
last_month.upload_speed_mbps_stddev as last_month_upload_speed_mbps_stddev,
last_year.test_count as last_year_test_count,
last_year.download_speed_mbps_median as last_year_download_speed_mbps_median,
last_year.upload_speed_mbps_median as last_year_upload_speed_mbps_median,
last_year.download_speed_mbps_avg as last_year_download_speed_mbps_avg,
last_year.upload_speed_mbps_avg as last_year_upload_speed_mbps_avg,
last_year.download_speed_mbps_min as last_year_download_speed_mbps_min,
last_year.upload_speed_mbps_min as last_year_upload_speed_mbps_min,
last_year.download_speed_mbps_max as last_year_download_speed_mbps_max,
last_year.upload_speed_mbps_max as last_year_upload_speed_mbps_max,
last_year.download_speed_mbps_stddev as last_year_download_speed_mbps_stddev,
last_year.upload_speed_mbps_stddev as last_year_upload_speed_mbps_stddev,
last_month.download_speed_mbps_bins as last_month_download_speed_mbps_bins,
last_month.upload_speed_mbps_bins as last_month_upload_speed_mbps_bins,
last_week.download_speed_mbps_bins as last_week_download_speed_mbps_bins,
last_week.upload_speed_mbps_bins as last_week_upload_speed_mbps_bins,
last_year.download_speed_mbps_bins as last_year_download_speed_mbps_bins,
last_year.upload_speed_mbps_bins as last_year_upload_speed_mbps_bins

  FROM {0} all

  -- left join madness here!
  left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    client_asn_number,
client_asn_name,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code,

    -- measurements:
    nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
    nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

    AVG(download_speed_mbps) AS download_speed_mbps_avg,
    AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

    MIN(download_speed_mbps) AS download_speed_mbps_min,
    MAX(download_speed_mbps) AS download_speed_mbps_max,

    MIN(upload_speed_mbps) AS upload_speed_mbps_min,
    MAX(upload_speed_mbps) AS upload_speed_mbps_max,

    STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
    STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

    -- bins:
    concat(STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 0 AND IFNULL(download_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 12 AND IFNULL(download_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 16 AND IFNULL(download_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 20 AND IFNULL(download_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 24 AND IFNULL(download_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 28 AND IFNULL(download_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 32 AND IFNULL(download_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 36 AND IFNULL(download_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 40 AND IFNULL(download_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 44 AND IFNULL(download_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 48 AND IFNULL(download_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 52 AND IFNULL(download_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 56 AND IFNULL(download_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 60 AND IFNULL(download_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 64 AND IFNULL(download_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 68 AND IFNULL(download_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 72 AND IFNULL(download_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 76 AND IFNULL(download_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 80 AND IFNULL(download_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 84 AND IFNULL(download_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 88 AND IFNULL(download_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 92 AND IFNULL(download_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 96 AND IFNULL(download_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
) as download_speed_mbps_bins,
concat(STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 0 AND IFNULL(upload_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 4 AND IFNULL(upload_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 8 AND IFNULL(upload_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 12 AND IFNULL(upload_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 16 AND IFNULL(upload_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 20 AND IFNULL(upload_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 24 AND IFNULL(upload_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 28 AND IFNULL(upload_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 32 AND IFNULL(upload_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 36 AND IFNULL(upload_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 40 AND IFNULL(upload_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 44 AND IFNULL(upload_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 48 AND IFNULL(upload_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 52 AND IFNULL(upload_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 56 AND IFNULL(upload_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 60 AND IFNULL(upload_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 64 AND IFNULL(upload_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 68 AND IFNULL(upload_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 72 AND IFNULL(upload_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 76 AND IFNULL(upload_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 80 AND IFNULL(upload_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 84 AND IFNULL(upload_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 88 AND IFNULL(upload_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 92 AND IFNULL(upload_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 96 AND IFNULL(upload_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 100, 1,0)))
) as upload_speed_mbps_bins

  from {0}
  where
    test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH") and
    client_region is not null
  group by
    -- group by location fields
    client_asn_number,
client_asn_name,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code

) last_month on
  -- join on location fields from the all table.
  all.client_asn_number = last_month.client_asn_number and
all.client_asn_name = last_month.client_asn_name and
all.client_region = last_month.client_region and
all.client_country = last_month.client_country and
all.client_continent = last_month.client_continent and
all.client_region_code = last_month.client_region_code and
all.client_country_code = last_month.client_country_code and
all.client_continent_code = last_month.client_continent_code
left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    client_asn_number,
client_asn_name,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code,

    -- measurements:
    nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
    nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

    AVG(download_speed_mbps) AS download_speed_mbps_avg,
    AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

    MIN(download_speed_mbps) AS download_speed_mbps_min,
    MAX(download_speed_mbps) AS download_speed_mbps_max,

    MIN(upload_speed_mbps) AS upload_speed_mbps_min,
    MAX(upload_speed_mbps) AS upload_speed_mbps_max,

    STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
    STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

    -- bins:
    concat(STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 0 AND IFNULL(download_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 12 AND IFNULL(download_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 16 AND IFNULL(download_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 20 AND IFNULL(download_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 24 AND IFNULL(download_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 28 AND IFNULL(download_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 32 AND IFNULL(download_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 36 AND IFNULL(download_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 40 AND IFNULL(download_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 44 AND IFNULL(download_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 48 AND IFNULL(download_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 52 AND IFNULL(download_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 56 AND IFNULL(download_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 60 AND IFNULL(download_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 64 AND IFNULL(download_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 68 AND IFNULL(download_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 72 AND IFNULL(download_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 76 AND IFNULL(download_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 80 AND IFNULL(download_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 84 AND IFNULL(download_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 88 AND IFNULL(download_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 92 AND IFNULL(download_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 96 AND IFNULL(download_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
) as download_speed_mbps_bins,
concat(STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 0 AND IFNULL(upload_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 4 AND IFNULL(upload_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 8 AND IFNULL(upload_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 12 AND IFNULL(upload_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 16 AND IFNULL(upload_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 20 AND IFNULL(upload_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 24 AND IFNULL(upload_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 28 AND IFNULL(upload_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 32 AND IFNULL(upload_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 36 AND IFNULL(upload_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 40 AND IFNULL(upload_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 44 AND IFNULL(upload_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 48 AND IFNULL(upload_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 52 AND IFNULL(upload_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 56 AND IFNULL(upload_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 60 AND IFNULL(upload_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 64 AND IFNULL(upload_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 68 AND IFNULL(upload_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 72 AND IFNULL(upload_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 76 AND IFNULL(upload_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 80 AND IFNULL(upload_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 84 AND IFNULL(upload_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 88 AND IFNULL(upload_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 92 AND IFNULL(upload_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 96 AND IFNULL(upload_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 100, 1,0)))
) as upload_speed_mbps_bins

  from {0}
  where
    test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, "DAY") and
    client_region is not null
  group by
    -- group by location fields
    client_asn_number,
client_asn_name,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code

) last_week on
  -- join on location fields from the all table.
  all.client_asn_number = last_week.client_asn_number and
all.client_asn_name = last_week.client_asn_name and
all.client_region = last_week.client_region and
all.client_country = last_week.client_country and
all.client_continent = last_week.client_continent and
all.client_region_code = last_week.client_region_code and
all.client_country_code = last_week.client_country_code and
all.client_continent_code = last_week.client_continent_code
left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    client_asn_number,
client_asn_name,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code,

    -- measurements:
    nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
    nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

    AVG(download_speed_mbps) AS download_speed_mbps_avg,
    AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

    MIN(download_speed_mbps) AS download_speed_mbps_min,
    MAX(download_speed_mbps) AS download_speed_mbps_max,

    MIN(upload_speed_mbps) AS upload_speed_mbps_min,
    MAX(upload_speed_mbps) AS upload_speed_mbps_max,

    STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
    STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

    -- bins:
    concat(STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 0 AND IFNULL(download_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 12 AND IFNULL(download_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 16 AND IFNULL(download_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 20 AND IFNULL(download_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 24 AND IFNULL(download_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 28 AND IFNULL(download_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 32 AND IFNULL(download_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 36 AND IFNULL(download_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 40 AND IFNULL(download_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 44 AND IFNULL(download_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 48 AND IFNULL(download_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 52 AND IFNULL(download_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 56 AND IFNULL(download_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 60 AND IFNULL(download_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 64 AND IFNULL(download_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 68 AND IFNULL(download_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 72 AND IFNULL(download_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 76 AND IFNULL(download_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 80 AND IFNULL(download_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 84 AND IFNULL(download_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 88 AND IFNULL(download_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 92 AND IFNULL(download_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 96 AND IFNULL(download_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
) as download_speed_mbps_bins,
concat(STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 0 AND IFNULL(upload_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 4 AND IFNULL(upload_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 8 AND IFNULL(upload_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 12 AND IFNULL(upload_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 16 AND IFNULL(upload_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 20 AND IFNULL(upload_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 24 AND IFNULL(upload_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 28 AND IFNULL(upload_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 32 AND IFNULL(upload_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 36 AND IFNULL(upload_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 40 AND IFNULL(upload_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 44 AND IFNULL(upload_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 48 AND IFNULL(upload_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 52 AND IFNULL(upload_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 56 AND IFNULL(upload_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 60 AND IFNULL(upload_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 64 AND IFNULL(upload_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 68 AND IFNULL(upload_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 72 AND IFNULL(upload_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 76 AND IFNULL(upload_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 80 AND IFNULL(upload_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 84 AND IFNULL(upload_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 88 AND IFNULL(upload_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 92 AND IFNULL(upload_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 96 AND IFNULL(upload_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 100, 1,0)))
) as upload_speed_mbps_bins

  from {0}
  where
    test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR") and
    client_region is not null
  group by
    -- group by location fields
    client_asn_number,
client_asn_name,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code

) last_year on
  -- join on location fields from the all table.
  all.client_asn_number = last_year.client_asn_number and
all.client_asn_name = last_year.client_asn_name and
all.client_region = last_year.client_region and
all.client_country = last_year.client_country and
all.client_continent = last_year.client_continent and
all.client_region_code = last_year.client_region_code and
all.client_country_code = last_year.client_country_code and
all.client_continent_code = last_year.client_continent_code


  GROUP BY

  location_id,
client_asn_number,
client_asn_number,
client_asn_name,
client_region,
client_country,
client_continent,
client_region_code,
client_country_code,
client_continent_code,
last_week_test_count,
last_week_download_speed_mbps_median,
last_week_upload_speed_mbps_median,
last_week_download_speed_mbps_avg,
last_week_upload_speed_mbps_avg,
last_week_download_speed_mbps_min,
last_week_upload_speed_mbps_min,
last_week_download_speed_mbps_max,
last_week_upload_speed_mbps_max,
last_week_download_speed_mbps_stddev,
last_week_upload_speed_mbps_stddev,
last_month_test_count,
last_month_download_speed_mbps_median,
last_month_upload_speed_mbps_median,
last_month_download_speed_mbps_avg,
last_month_upload_speed_mbps_avg,
last_month_download_speed_mbps_min,
last_month_upload_speed_mbps_min,
last_month_download_speed_mbps_max,
last_month_upload_speed_mbps_max,
last_month_download_speed_mbps_stddev,
last_month_upload_speed_mbps_stddev,
last_year_test_count,
last_year_download_speed_mbps_median,
last_year_upload_speed_mbps_median,
last_year_download_speed_mbps_avg,
last_year_upload_speed_mbps_avg,
last_year_download_speed_mbps_min,
last_year_upload_speed_mbps_min,
last_year_download_speed_mbps_max,
last_year_upload_speed_mbps_max,
last_year_download_speed_mbps_stddev,
last_year_upload_speed_mbps_stddev,
last_month_download_speed_mbps_bins,
last_month_upload_speed_mbps_bins,
last_week_download_speed_mbps_bins,
last_week_upload_speed_mbps_bins,
last_year_download_speed_mbps_bins,
last_year_upload_speed_mbps_bins

)
,
-- ============
-- Type: country
-- ============
(SELECT
  REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), ""
,IFNULL(all.client_country_code, ""), ""
)), " ", "") as location_id,

  -- what is its type?
  "country" AS type,

  -- meta fields we are selecting
  all.client_asn_number as client_asn_number,
all.client_asn_name as client_asn_name,
all.client_country as client_country,
all.client_continent as client_continent,
all.client_country_code as client_country_code,
all.client_continent_code as client_continent_code,

  -- timed fields
  last_week.test_count as last_week_test_count,
last_week.download_speed_mbps_median as last_week_download_speed_mbps_median,
last_week.upload_speed_mbps_median as last_week_upload_speed_mbps_median,
last_week.download_speed_mbps_avg as last_week_download_speed_mbps_avg,
last_week.upload_speed_mbps_avg as last_week_upload_speed_mbps_avg,
last_week.download_speed_mbps_min as last_week_download_speed_mbps_min,
last_week.upload_speed_mbps_min as last_week_upload_speed_mbps_min,
last_week.download_speed_mbps_max as last_week_download_speed_mbps_max,
last_week.upload_speed_mbps_max as last_week_upload_speed_mbps_max,
last_week.download_speed_mbps_stddev as last_week_download_speed_mbps_stddev,
last_week.upload_speed_mbps_stddev as last_week_upload_speed_mbps_stddev,
last_month.test_count as last_month_test_count,
last_month.download_speed_mbps_median as last_month_download_speed_mbps_median,
last_month.upload_speed_mbps_median as last_month_upload_speed_mbps_median,
last_month.download_speed_mbps_avg as last_month_download_speed_mbps_avg,
last_month.upload_speed_mbps_avg as last_month_upload_speed_mbps_avg,
last_month.download_speed_mbps_min as last_month_download_speed_mbps_min,
last_month.upload_speed_mbps_min as last_month_upload_speed_mbps_min,
last_month.download_speed_mbps_max as last_month_download_speed_mbps_max,
last_month.upload_speed_mbps_max as last_month_upload_speed_mbps_max,
last_month.download_speed_mbps_stddev as last_month_download_speed_mbps_stddev,
last_month.upload_speed_mbps_stddev as last_month_upload_speed_mbps_stddev,
last_year.test_count as last_year_test_count,
last_year.download_speed_mbps_median as last_year_download_speed_mbps_median,
last_year.upload_speed_mbps_median as last_year_upload_speed_mbps_median,
last_year.download_speed_mbps_avg as last_year_download_speed_mbps_avg,
last_year.upload_speed_mbps_avg as last_year_upload_speed_mbps_avg,
last_year.download_speed_mbps_min as last_year_download_speed_mbps_min,
last_year.upload_speed_mbps_min as last_year_upload_speed_mbps_min,
last_year.download_speed_mbps_max as last_year_download_speed_mbps_max,
last_year.upload_speed_mbps_max as last_year_upload_speed_mbps_max,
last_year.download_speed_mbps_stddev as last_year_download_speed_mbps_stddev,
last_year.upload_speed_mbps_stddev as last_year_upload_speed_mbps_stddev,
last_month.download_speed_mbps_bins as last_month_download_speed_mbps_bins,
last_month.upload_speed_mbps_bins as last_month_upload_speed_mbps_bins,
last_week.download_speed_mbps_bins as last_week_download_speed_mbps_bins,
last_week.upload_speed_mbps_bins as last_week_upload_speed_mbps_bins,
last_year.download_speed_mbps_bins as last_year_download_speed_mbps_bins,
last_year.upload_speed_mbps_bins as last_year_upload_speed_mbps_bins

  FROM {0} all

  -- left join madness here!
  left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    client_asn_number,
client_asn_name,
client_country,
client_continent,
client_country_code,
client_continent_code,

    -- measurements:
    nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
    nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

    AVG(download_speed_mbps) AS download_speed_mbps_avg,
    AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

    MIN(download_speed_mbps) AS download_speed_mbps_min,
    MAX(download_speed_mbps) AS download_speed_mbps_max,

    MIN(upload_speed_mbps) AS upload_speed_mbps_min,
    MAX(upload_speed_mbps) AS upload_speed_mbps_max,

    STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
    STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

    -- bins:
    concat(STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 0 AND IFNULL(download_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 12 AND IFNULL(download_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 16 AND IFNULL(download_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 20 AND IFNULL(download_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 24 AND IFNULL(download_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 28 AND IFNULL(download_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 32 AND IFNULL(download_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 36 AND IFNULL(download_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 40 AND IFNULL(download_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 44 AND IFNULL(download_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 48 AND IFNULL(download_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 52 AND IFNULL(download_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 56 AND IFNULL(download_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 60 AND IFNULL(download_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 64 AND IFNULL(download_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 68 AND IFNULL(download_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 72 AND IFNULL(download_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 76 AND IFNULL(download_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 80 AND IFNULL(download_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 84 AND IFNULL(download_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 88 AND IFNULL(download_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 92 AND IFNULL(download_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 96 AND IFNULL(download_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
) as download_speed_mbps_bins,
concat(STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 0 AND IFNULL(upload_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 4 AND IFNULL(upload_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 8 AND IFNULL(upload_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 12 AND IFNULL(upload_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 16 AND IFNULL(upload_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 20 AND IFNULL(upload_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 24 AND IFNULL(upload_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 28 AND IFNULL(upload_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 32 AND IFNULL(upload_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 36 AND IFNULL(upload_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 40 AND IFNULL(upload_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 44 AND IFNULL(upload_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 48 AND IFNULL(upload_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 52 AND IFNULL(upload_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 56 AND IFNULL(upload_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 60 AND IFNULL(upload_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 64 AND IFNULL(upload_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 68 AND IFNULL(upload_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 72 AND IFNULL(upload_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 76 AND IFNULL(upload_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 80 AND IFNULL(upload_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 84 AND IFNULL(upload_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 88 AND IFNULL(upload_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 92 AND IFNULL(upload_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 96 AND IFNULL(upload_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 100, 1,0)))
) as upload_speed_mbps_bins

  from {0}
  where
    test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH") and
    client_country is not null
  group by
    -- group by location fields
    client_asn_number,
client_asn_name,
client_country,
client_continent,
client_country_code,
client_continent_code

) last_month on
  -- join on location fields from the all table.
  all.client_asn_number = last_month.client_asn_number and
all.client_asn_name = last_month.client_asn_name and
all.client_country = last_month.client_country and
all.client_continent = last_month.client_continent and
all.client_country_code = last_month.client_country_code and
all.client_continent_code = last_month.client_continent_code
left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    client_asn_number,
client_asn_name,
client_country,
client_continent,
client_country_code,
client_continent_code,

    -- measurements:
    nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
    nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

    AVG(download_speed_mbps) AS download_speed_mbps_avg,
    AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

    MIN(download_speed_mbps) AS download_speed_mbps_min,
    MAX(download_speed_mbps) AS download_speed_mbps_max,

    MIN(upload_speed_mbps) AS upload_speed_mbps_min,
    MAX(upload_speed_mbps) AS upload_speed_mbps_max,

    STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
    STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

    -- bins:
    concat(STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 0 AND IFNULL(download_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 12 AND IFNULL(download_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 16 AND IFNULL(download_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 20 AND IFNULL(download_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 24 AND IFNULL(download_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 28 AND IFNULL(download_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 32 AND IFNULL(download_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 36 AND IFNULL(download_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 40 AND IFNULL(download_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 44 AND IFNULL(download_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 48 AND IFNULL(download_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 52 AND IFNULL(download_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 56 AND IFNULL(download_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 60 AND IFNULL(download_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 64 AND IFNULL(download_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 68 AND IFNULL(download_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 72 AND IFNULL(download_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 76 AND IFNULL(download_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 80 AND IFNULL(download_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 84 AND IFNULL(download_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 88 AND IFNULL(download_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 92 AND IFNULL(download_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 96 AND IFNULL(download_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
) as download_speed_mbps_bins,
concat(STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 0 AND IFNULL(upload_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 4 AND IFNULL(upload_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 8 AND IFNULL(upload_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 12 AND IFNULL(upload_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 16 AND IFNULL(upload_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 20 AND IFNULL(upload_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 24 AND IFNULL(upload_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 28 AND IFNULL(upload_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 32 AND IFNULL(upload_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 36 AND IFNULL(upload_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 40 AND IFNULL(upload_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 44 AND IFNULL(upload_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 48 AND IFNULL(upload_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 52 AND IFNULL(upload_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 56 AND IFNULL(upload_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 60 AND IFNULL(upload_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 64 AND IFNULL(upload_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 68 AND IFNULL(upload_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 72 AND IFNULL(upload_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 76 AND IFNULL(upload_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 80 AND IFNULL(upload_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 84 AND IFNULL(upload_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 88 AND IFNULL(upload_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 92 AND IFNULL(upload_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 96 AND IFNULL(upload_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 100, 1,0)))
) as upload_speed_mbps_bins

  from {0}
  where
    test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, "DAY") and
    client_country is not null
  group by
    -- group by location fields
    client_asn_number,
client_asn_name,
client_country,
client_continent,
client_country_code,
client_continent_code

) last_week on
  -- join on location fields from the all table.
  all.client_asn_number = last_week.client_asn_number and
all.client_asn_name = last_week.client_asn_name and
all.client_country = last_week.client_country and
all.client_continent = last_week.client_continent and
all.client_country_code = last_week.client_country_code and
all.client_continent_code = last_week.client_continent_code
left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    client_asn_number,
client_asn_name,
client_country,
client_continent,
client_country_code,
client_continent_code,

    -- measurements:
    nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
    nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

    AVG(download_speed_mbps) AS download_speed_mbps_avg,
    AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

    MIN(download_speed_mbps) AS download_speed_mbps_min,
    MAX(download_speed_mbps) AS download_speed_mbps_max,

    MIN(upload_speed_mbps) AS upload_speed_mbps_min,
    MAX(upload_speed_mbps) AS upload_speed_mbps_max,

    STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
    STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

    -- bins:
    concat(STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 0 AND IFNULL(download_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 12 AND IFNULL(download_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 16 AND IFNULL(download_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 20 AND IFNULL(download_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 24 AND IFNULL(download_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 28 AND IFNULL(download_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 32 AND IFNULL(download_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 36 AND IFNULL(download_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 40 AND IFNULL(download_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 44 AND IFNULL(download_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 48 AND IFNULL(download_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 52 AND IFNULL(download_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 56 AND IFNULL(download_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 60 AND IFNULL(download_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 64 AND IFNULL(download_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 68 AND IFNULL(download_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 72 AND IFNULL(download_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 76 AND IFNULL(download_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 80 AND IFNULL(download_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 84 AND IFNULL(download_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 88 AND IFNULL(download_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 92 AND IFNULL(download_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 96 AND IFNULL(download_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
) as download_speed_mbps_bins,
concat(STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 0 AND IFNULL(upload_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 4 AND IFNULL(upload_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 8 AND IFNULL(upload_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 12 AND IFNULL(upload_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 16 AND IFNULL(upload_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 20 AND IFNULL(upload_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 24 AND IFNULL(upload_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 28 AND IFNULL(upload_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 32 AND IFNULL(upload_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 36 AND IFNULL(upload_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 40 AND IFNULL(upload_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 44 AND IFNULL(upload_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 48 AND IFNULL(upload_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 52 AND IFNULL(upload_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 56 AND IFNULL(upload_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 60 AND IFNULL(upload_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 64 AND IFNULL(upload_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 68 AND IFNULL(upload_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 72 AND IFNULL(upload_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 76 AND IFNULL(upload_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 80 AND IFNULL(upload_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 84 AND IFNULL(upload_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 88 AND IFNULL(upload_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 92 AND IFNULL(upload_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 96 AND IFNULL(upload_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 100, 1,0)))
) as upload_speed_mbps_bins

  from {0}
  where
    test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR") and
    client_country is not null
  group by
    -- group by location fields
    client_asn_number,
client_asn_name,
client_country,
client_continent,
client_country_code,
client_continent_code

) last_year on
  -- join on location fields from the all table.
  all.client_asn_number = last_year.client_asn_number and
all.client_asn_name = last_year.client_asn_name and
all.client_country = last_year.client_country and
all.client_continent = last_year.client_continent and
all.client_country_code = last_year.client_country_code and
all.client_continent_code = last_year.client_continent_code


  GROUP BY

  location_id,
client_asn_number,
client_asn_number,
client_asn_name,
client_country,
client_continent,
client_country_code,
client_continent_code,
last_week_test_count,
last_week_download_speed_mbps_median,
last_week_upload_speed_mbps_median,
last_week_download_speed_mbps_avg,
last_week_upload_speed_mbps_avg,
last_week_download_speed_mbps_min,
last_week_upload_speed_mbps_min,
last_week_download_speed_mbps_max,
last_week_upload_speed_mbps_max,
last_week_download_speed_mbps_stddev,
last_week_upload_speed_mbps_stddev,
last_month_test_count,
last_month_download_speed_mbps_median,
last_month_upload_speed_mbps_median,
last_month_download_speed_mbps_avg,
last_month_upload_speed_mbps_avg,
last_month_download_speed_mbps_min,
last_month_upload_speed_mbps_min,
last_month_download_speed_mbps_max,
last_month_upload_speed_mbps_max,
last_month_download_speed_mbps_stddev,
last_month_upload_speed_mbps_stddev,
last_year_test_count,
last_year_download_speed_mbps_median,
last_year_upload_speed_mbps_median,
last_year_download_speed_mbps_avg,
last_year_upload_speed_mbps_avg,
last_year_download_speed_mbps_min,
last_year_upload_speed_mbps_min,
last_year_download_speed_mbps_max,
last_year_upload_speed_mbps_max,
last_year_download_speed_mbps_stddev,
last_year_upload_speed_mbps_stddev,
last_month_download_speed_mbps_bins,
last_month_upload_speed_mbps_bins,
last_week_download_speed_mbps_bins,
last_week_upload_speed_mbps_bins,
last_year_download_speed_mbps_bins,
last_year_upload_speed_mbps_bins

)
,
-- ============
-- Type: continent
-- ============
(SELECT
  LOWER(IFNULL(all.client_continent_code, "")) as location_id,

  -- what is its type?
  "continent" AS type,

  -- meta fields we are selecting
  all.client_asn_number as client_asn_number,
all.client_asn_name as client_asn_name,
all.client_continent as client_continent,
all.client_continent_code as client_continent_code,

  -- timed fields
  last_week.test_count as last_week_test_count,
last_week.download_speed_mbps_median as last_week_download_speed_mbps_median,
last_week.upload_speed_mbps_median as last_week_upload_speed_mbps_median,
last_week.download_speed_mbps_avg as last_week_download_speed_mbps_avg,
last_week.upload_speed_mbps_avg as last_week_upload_speed_mbps_avg,
last_week.download_speed_mbps_min as last_week_download_speed_mbps_min,
last_week.upload_speed_mbps_min as last_week_upload_speed_mbps_min,
last_week.download_speed_mbps_max as last_week_download_speed_mbps_max,
last_week.upload_speed_mbps_max as last_week_upload_speed_mbps_max,
last_week.download_speed_mbps_stddev as last_week_download_speed_mbps_stddev,
last_week.upload_speed_mbps_stddev as last_week_upload_speed_mbps_stddev,
last_month.test_count as last_month_test_count,
last_month.download_speed_mbps_median as last_month_download_speed_mbps_median,
last_month.upload_speed_mbps_median as last_month_upload_speed_mbps_median,
last_month.download_speed_mbps_avg as last_month_download_speed_mbps_avg,
last_month.upload_speed_mbps_avg as last_month_upload_speed_mbps_avg,
last_month.download_speed_mbps_min as last_month_download_speed_mbps_min,
last_month.upload_speed_mbps_min as last_month_upload_speed_mbps_min,
last_month.download_speed_mbps_max as last_month_download_speed_mbps_max,
last_month.upload_speed_mbps_max as last_month_upload_speed_mbps_max,
last_month.download_speed_mbps_stddev as last_month_download_speed_mbps_stddev,
last_month.upload_speed_mbps_stddev as last_month_upload_speed_mbps_stddev,
last_year.test_count as last_year_test_count,
last_year.download_speed_mbps_median as last_year_download_speed_mbps_median,
last_year.upload_speed_mbps_median as last_year_upload_speed_mbps_median,
last_year.download_speed_mbps_avg as last_year_download_speed_mbps_avg,
last_year.upload_speed_mbps_avg as last_year_upload_speed_mbps_avg,
last_year.download_speed_mbps_min as last_year_download_speed_mbps_min,
last_year.upload_speed_mbps_min as last_year_upload_speed_mbps_min,
last_year.download_speed_mbps_max as last_year_download_speed_mbps_max,
last_year.upload_speed_mbps_max as last_year_upload_speed_mbps_max,
last_year.download_speed_mbps_stddev as last_year_download_speed_mbps_stddev,
last_year.upload_speed_mbps_stddev as last_year_upload_speed_mbps_stddev,
last_month.download_speed_mbps_bins as last_month_download_speed_mbps_bins,
last_month.upload_speed_mbps_bins as last_month_upload_speed_mbps_bins,
last_week.download_speed_mbps_bins as last_week_download_speed_mbps_bins,
last_week.upload_speed_mbps_bins as last_week_upload_speed_mbps_bins,
last_year.download_speed_mbps_bins as last_year_download_speed_mbps_bins,
last_year.upload_speed_mbps_bins as last_year_upload_speed_mbps_bins

  FROM {0} all

  -- left join madness here!
  left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    client_asn_number,
client_asn_name,
client_continent,
client_continent_code,

    -- measurements:
    nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
    nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

    AVG(download_speed_mbps) AS download_speed_mbps_avg,
    AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

    MIN(download_speed_mbps) AS download_speed_mbps_min,
    MAX(download_speed_mbps) AS download_speed_mbps_max,

    MIN(upload_speed_mbps) AS upload_speed_mbps_min,
    MAX(upload_speed_mbps) AS upload_speed_mbps_max,

    STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
    STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

    -- bins:
    concat(STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 0 AND IFNULL(download_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 12 AND IFNULL(download_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 16 AND IFNULL(download_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 20 AND IFNULL(download_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 24 AND IFNULL(download_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 28 AND IFNULL(download_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 32 AND IFNULL(download_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 36 AND IFNULL(download_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 40 AND IFNULL(download_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 44 AND IFNULL(download_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 48 AND IFNULL(download_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 52 AND IFNULL(download_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 56 AND IFNULL(download_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 60 AND IFNULL(download_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 64 AND IFNULL(download_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 68 AND IFNULL(download_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 72 AND IFNULL(download_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 76 AND IFNULL(download_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 80 AND IFNULL(download_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 84 AND IFNULL(download_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 88 AND IFNULL(download_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 92 AND IFNULL(download_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 96 AND IFNULL(download_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
) as download_speed_mbps_bins,
concat(STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 0 AND IFNULL(upload_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 4 AND IFNULL(upload_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 8 AND IFNULL(upload_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 12 AND IFNULL(upload_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 16 AND IFNULL(upload_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 20 AND IFNULL(upload_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 24 AND IFNULL(upload_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 28 AND IFNULL(upload_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 32 AND IFNULL(upload_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 36 AND IFNULL(upload_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 40 AND IFNULL(upload_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 44 AND IFNULL(upload_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 48 AND IFNULL(upload_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 52 AND IFNULL(upload_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 56 AND IFNULL(upload_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 60 AND IFNULL(upload_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 64 AND IFNULL(upload_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 68 AND IFNULL(upload_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 72 AND IFNULL(upload_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 76 AND IFNULL(upload_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 80 AND IFNULL(upload_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 84 AND IFNULL(upload_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 88 AND IFNULL(upload_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 92 AND IFNULL(upload_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 96 AND IFNULL(upload_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 100, 1,0)))
) as upload_speed_mbps_bins

  from {0}
  where
    test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH") and
    client_continent is not null
  group by
    -- group by location fields
    client_asn_number,
client_asn_name,
client_continent,
client_continent_code

) last_month on
  -- join on location fields from the all table.
  all.client_asn_number = last_month.client_asn_number and
all.client_asn_name = last_month.client_asn_name and
all.client_continent = last_month.client_continent and
all.client_continent_code = last_month.client_continent_code
left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    client_asn_number,
client_asn_name,
client_continent,
client_continent_code,

    -- measurements:
    nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
    nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

    AVG(download_speed_mbps) AS download_speed_mbps_avg,
    AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

    MIN(download_speed_mbps) AS download_speed_mbps_min,
    MAX(download_speed_mbps) AS download_speed_mbps_max,

    MIN(upload_speed_mbps) AS upload_speed_mbps_min,
    MAX(upload_speed_mbps) AS upload_speed_mbps_max,

    STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
    STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

    -- bins:
    concat(STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 0 AND IFNULL(download_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 12 AND IFNULL(download_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 16 AND IFNULL(download_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 20 AND IFNULL(download_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 24 AND IFNULL(download_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 28 AND IFNULL(download_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 32 AND IFNULL(download_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 36 AND IFNULL(download_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 40 AND IFNULL(download_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 44 AND IFNULL(download_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 48 AND IFNULL(download_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 52 AND IFNULL(download_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 56 AND IFNULL(download_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 60 AND IFNULL(download_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 64 AND IFNULL(download_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 68 AND IFNULL(download_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 72 AND IFNULL(download_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 76 AND IFNULL(download_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 80 AND IFNULL(download_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 84 AND IFNULL(download_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 88 AND IFNULL(download_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 92 AND IFNULL(download_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 96 AND IFNULL(download_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
) as download_speed_mbps_bins,
concat(STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 0 AND IFNULL(upload_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 4 AND IFNULL(upload_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 8 AND IFNULL(upload_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 12 AND IFNULL(upload_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 16 AND IFNULL(upload_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 20 AND IFNULL(upload_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 24 AND IFNULL(upload_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 28 AND IFNULL(upload_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 32 AND IFNULL(upload_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 36 AND IFNULL(upload_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 40 AND IFNULL(upload_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 44 AND IFNULL(upload_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 48 AND IFNULL(upload_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 52 AND IFNULL(upload_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 56 AND IFNULL(upload_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 60 AND IFNULL(upload_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 64 AND IFNULL(upload_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 68 AND IFNULL(upload_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 72 AND IFNULL(upload_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 76 AND IFNULL(upload_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 80 AND IFNULL(upload_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 84 AND IFNULL(upload_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 88 AND IFNULL(upload_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 92 AND IFNULL(upload_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 96 AND IFNULL(upload_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 100, 1,0)))
) as upload_speed_mbps_bins

  from {0}
  where
    test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, "DAY") and
    client_continent is not null
  group by
    -- group by location fields
    client_asn_number,
client_asn_name,
client_continent,
client_continent_code

) last_week on
  -- join on location fields from the all table.
  all.client_asn_number = last_week.client_asn_number and
all.client_asn_name = last_week.client_asn_name and
all.client_continent = last_week.client_continent and
all.client_continent_code = last_week.client_continent_code
left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    client_asn_number,
client_asn_name,
client_continent,
client_continent_code,

    -- measurements:
    nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
    nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

    AVG(download_speed_mbps) AS download_speed_mbps_avg,
    AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

    MIN(download_speed_mbps) AS download_speed_mbps_min,
    MAX(download_speed_mbps) AS download_speed_mbps_max,

    MIN(upload_speed_mbps) AS upload_speed_mbps_min,
    MAX(upload_speed_mbps) AS upload_speed_mbps_max,

    STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
    STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

    -- bins:
    concat(STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 0 AND IFNULL(download_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 12 AND IFNULL(download_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 16 AND IFNULL(download_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 20 AND IFNULL(download_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 24 AND IFNULL(download_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 28 AND IFNULL(download_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 32 AND IFNULL(download_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 36 AND IFNULL(download_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 40 AND IFNULL(download_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 44 AND IFNULL(download_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 48 AND IFNULL(download_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 52 AND IFNULL(download_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 56 AND IFNULL(download_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 60 AND IFNULL(download_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 64 AND IFNULL(download_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 68 AND IFNULL(download_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 72 AND IFNULL(download_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 76 AND IFNULL(download_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 80 AND IFNULL(download_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 84 AND IFNULL(download_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 88 AND IFNULL(download_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 92 AND IFNULL(download_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 96 AND IFNULL(download_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
) as download_speed_mbps_bins,
concat(STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 0 AND IFNULL(upload_speed_mbps, 0) < 4, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 4 AND IFNULL(upload_speed_mbps, 0) < 8, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 8 AND IFNULL(upload_speed_mbps, 0) < 12, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 12 AND IFNULL(upload_speed_mbps, 0) < 16, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 16 AND IFNULL(upload_speed_mbps, 0) < 20, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 20 AND IFNULL(upload_speed_mbps, 0) < 24, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 24 AND IFNULL(upload_speed_mbps, 0) < 28, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 28 AND IFNULL(upload_speed_mbps, 0) < 32, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 32 AND IFNULL(upload_speed_mbps, 0) < 36, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 36 AND IFNULL(upload_speed_mbps, 0) < 40, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 40 AND IFNULL(upload_speed_mbps, 0) < 44, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 44 AND IFNULL(upload_speed_mbps, 0) < 48, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 48 AND IFNULL(upload_speed_mbps, 0) < 52, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 52 AND IFNULL(upload_speed_mbps, 0) < 56, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 56 AND IFNULL(upload_speed_mbps, 0) < 60, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 60 AND IFNULL(upload_speed_mbps, 0) < 64, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 64 AND IFNULL(upload_speed_mbps, 0) < 68, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 68 AND IFNULL(upload_speed_mbps, 0) < 72, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 72 AND IFNULL(upload_speed_mbps, 0) < 76, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 76 AND IFNULL(upload_speed_mbps, 0) < 80, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 80 AND IFNULL(upload_speed_mbps, 0) < 84, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 84 AND IFNULL(upload_speed_mbps, 0) < 88, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 88 AND IFNULL(upload_speed_mbps, 0) < 92, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 92 AND IFNULL(upload_speed_mbps, 0) < 96, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 96 AND IFNULL(upload_speed_mbps, 0) < 100, 1,0))), ",",
STRING(SUM(IF(IFNULL(upload_speed_mbps, 0) >= 100, 1,0)))
) as upload_speed_mbps_bins

  from {0}
  where
    test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR") and
    client_continent is not null
  group by
    -- group by location fields
    client_asn_number,
client_asn_name,
client_continent,
client_continent_code

) last_year on
  -- join on location fields from the all table.
  all.client_asn_number = last_year.client_asn_number and
all.client_asn_name = last_year.client_asn_name and
all.client_continent = last_year.client_continent and
all.client_continent_code = last_year.client_continent_code


  GROUP BY

  location_id,
client_asn_number,
client_asn_number,
client_asn_name,
client_continent,
client_continent_code,
last_week_test_count,
last_week_download_speed_mbps_median,
last_week_upload_speed_mbps_median,
last_week_download_speed_mbps_avg,
last_week_upload_speed_mbps_avg,
last_week_download_speed_mbps_min,
last_week_upload_speed_mbps_min,
last_week_download_speed_mbps_max,
last_week_upload_speed_mbps_max,
last_week_download_speed_mbps_stddev,
last_week_upload_speed_mbps_stddev,
last_month_test_count,
last_month_download_speed_mbps_median,
last_month_upload_speed_mbps_median,
last_month_download_speed_mbps_avg,
last_month_upload_speed_mbps_avg,
last_month_download_speed_mbps_min,
last_month_upload_speed_mbps_min,
last_month_download_speed_mbps_max,
last_month_upload_speed_mbps_max,
last_month_download_speed_mbps_stddev,
last_month_upload_speed_mbps_stddev,
last_year_test_count,
last_year_download_speed_mbps_median,
last_year_upload_speed_mbps_median,
last_year_download_speed_mbps_avg,
last_year_upload_speed_mbps_avg,
last_year_download_speed_mbps_min,
last_year_upload_speed_mbps_min,
last_year_download_speed_mbps_max,
last_year_upload_speed_mbps_max,
last_year_download_speed_mbps_stddev,
last_year_upload_speed_mbps_stddev,
last_month_download_speed_mbps_bins,
last_month_upload_speed_mbps_bins,
last_week_download_speed_mbps_bins,
last_week_upload_speed_mbps_bins,
last_year_download_speed_mbps_bins,
last_year_upload_speed_mbps_bins

)
