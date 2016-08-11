
select parent_location_id,
       child_location_name,
       type,
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
       last_year_upload_speed_mbps_stddev
from -- ============
-- Type: city
-- ============

  (SELECT REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), "",IFNULL(all.client_country_code, ""), "",IFNULL(all.client_region_code, ""), "")), " ", "") as parent_location_id, -- which field is the child location?
 all.client_city AS child_location_name, -- what is its type?
 "city" AS type, -- meta fields we are selecting
 all.client_city as client_city,
 all.client_region as client_region,
 all.client_country as client_country,
 all.client_continent as client_continent,
 all.client_region_code as client_region_code,
 all.client_country_code as client_country_code,
 all.client_continent_code as client_continent_code, -- timed fields
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
 last_year.upload_speed_mbps_stddev as last_year_upload_speed_mbps_stddev
   FROM {0} all -- left join madness here!

   left join
     ( SELECT count(*) as test_count, -- which location fields?
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code, -- measurements:
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
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH")
        and client_city is not null
      group by -- group by location fields
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_month on -- join on location fields from the all table.
 all.client_city = last_month.client_city
   and all.client_region = last_month.client_region
   and all.client_country = last_month.client_country
   and all.client_continent = last_month.client_continent
   and all.client_region_code = last_month.client_region_code
   and all.client_country_code = last_month.client_country_code
   and all.client_continent_code = last_month.client_continent_code
   left join
     ( SELECT count(*) as test_count, -- which location fields?
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code, -- measurements:
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
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, "DAY")
        and client_city is not null
      group by -- group by location fields
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_week on -- join on location fields from the all table.
 all.client_city = last_week.client_city
   and all.client_region = last_week.client_region
   and all.client_country = last_week.client_country
   and all.client_continent = last_week.client_continent
   and all.client_region_code = last_week.client_region_code
   and all.client_country_code = last_week.client_country_code
   and all.client_continent_code = last_week.client_continent_code
   left join
     ( SELECT count(*) as test_count, -- which location fields?
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code, -- measurements:
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
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR")
        and client_city is not null
      group by -- group by location fields
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_year on -- join on location fields from the all table.
 all.client_city = last_year.client_city
   and all.client_region = last_year.client_region
   and all.client_country = last_year.client_country
   and all.client_continent = last_year.client_continent
   and all.client_region_code = last_year.client_region_code
   and all.client_country_code = last_year.client_country_code
   and all.client_continent_code = last_year.client_continent_code
   GROUP BY parent_location_id,
            child_location_name,
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
            last_year_upload_speed_mbps_stddev ) , -- ============
-- Type: region
-- ============

  (SELECT REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), "",IFNULL(all.client_country_code, ""), "")), " ", "") as parent_location_id, -- which field is the child location?
 all.client_region AS child_location_name, -- what is its type?
 "region" AS type, -- meta fields we are selecting
 all.client_region as client_region,
 all.client_country as client_country,
 all.client_continent as client_continent,
 all.client_region_code as client_region_code,
 all.client_country_code as client_country_code,
 all.client_continent_code as client_continent_code, -- timed fields
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
 last_year.upload_speed_mbps_stddev as last_year_upload_speed_mbps_stddev
   FROM {0} all -- left join madness here!

   left join
     ( SELECT count(*) as test_count, -- which location fields?
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code, -- measurements:
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
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH")
        and client_region is not null
      group by -- group by location fields
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_month on -- join on location fields from the all table.
 all.client_region = last_month.client_region
   and all.client_country = last_month.client_country
   and all.client_continent = last_month.client_continent
   and all.client_region_code = last_month.client_region_code
   and all.client_country_code = last_month.client_country_code
   and all.client_continent_code = last_month.client_continent_code
   left join
     ( SELECT count(*) as test_count, -- which location fields?
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code, -- measurements:
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
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, "DAY")
        and client_region is not null
      group by -- group by location fields
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_week on -- join on location fields from the all table.
 all.client_region = last_week.client_region
   and all.client_country = last_week.client_country
   and all.client_continent = last_week.client_continent
   and all.client_region_code = last_week.client_region_code
   and all.client_country_code = last_week.client_country_code
   and all.client_continent_code = last_week.client_continent_code
   left join
     ( SELECT count(*) as test_count, -- which location fields?
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code, -- measurements:
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
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR")
        and client_region is not null
      group by -- group by location fields
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_year on -- join on location fields from the all table.
 all.client_region = last_year.client_region
   and all.client_country = last_year.client_country
   and all.client_continent = last_year.client_continent
   and all.client_region_code = last_year.client_region_code
   and all.client_country_code = last_year.client_country_code
   and all.client_continent_code = last_year.client_continent_code
   GROUP BY parent_location_id,
            child_location_name,
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
            last_year_upload_speed_mbps_stddev ) , -- ============
-- Type: country
-- ============

  (SELECT LOWER(IFNULL(all.client_continent_code, "")) as parent_location_id, -- which field is the child location?
 all.client_country AS child_location_name, -- what is its type?
 "country" AS type, -- meta fields we are selecting
 all.client_country as client_country,
 all.client_continent as client_continent,
 all.client_country_code as client_country_code,
 all.client_continent_code as client_continent_code, -- timed fields
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
 last_year.upload_speed_mbps_stddev as last_year_upload_speed_mbps_stddev
   FROM {0} all -- left join madness here!

   left join
     ( SELECT count(*) as test_count, -- which location fields?
 client_country,
 client_continent,
 client_country_code,
 client_continent_code, -- measurements:
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
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH")
        and client_country is not null
      group by -- group by location fields
 client_country,
 client_continent,
 client_country_code,
 client_continent_code ) last_month on -- join on location fields from the all table.
 all.client_country = last_month.client_country
   and all.client_continent = last_month.client_continent
   and all.client_country_code = last_month.client_country_code
   and all.client_continent_code = last_month.client_continent_code
   left join
     ( SELECT count(*) as test_count, -- which location fields?
 client_country,
 client_continent,
 client_country_code,
 client_continent_code, -- measurements:
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
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, "DAY")
        and client_country is not null
      group by -- group by location fields
 client_country,
 client_continent,
 client_country_code,
 client_continent_code ) last_week on -- join on location fields from the all table.
 all.client_country = last_week.client_country
   and all.client_continent = last_week.client_continent
   and all.client_country_code = last_week.client_country_code
   and all.client_continent_code = last_week.client_continent_code
   left join
     ( SELECT count(*) as test_count, -- which location fields?
 client_country,
 client_continent,
 client_country_code,
 client_continent_code, -- measurements:
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
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR")
        and client_country is not null
      group by -- group by location fields
 client_country,
 client_continent,
 client_country_code,
 client_continent_code ) last_year on -- join on location fields from the all table.
 all.client_country = last_year.client_country
   and all.client_continent = last_year.client_continent
   and all.client_country_code = last_year.client_country_code
   and all.client_continent_code = last_year.client_continent_code
   GROUP BY parent_location_id,
            child_location_name,
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
            last_year_upload_speed_mbps_stddev ) , -- ============
-- Type: continent
-- ============

  (SELECT "" as parent_location_id, -- which field is the child location?
 all.client_continent AS child_location_name, -- what is its type?
 "continent" AS type, -- meta fields we are selecting
 all.client_continent as client_continent,
 all.client_continent_code as client_continent_code, -- timed fields
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
 last_year.upload_speed_mbps_stddev as last_year_upload_speed_mbps_stddev
   FROM {0} all -- left join madness here!

   left join
     ( SELECT count(*) as test_count, -- which location fields?
 client_continent,
 client_continent_code, -- measurements:
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
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH")
        and client_continent is not null
      group by -- group by location fields
 client_continent,
 client_continent_code ) last_month on -- join on location fields from the all table.
 all.client_continent = last_month.client_continent
   and all.client_continent_code = last_month.client_continent_code
   left join
     ( SELECT count(*) as test_count, -- which location fields?
 client_continent,
 client_continent_code, -- measurements:
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
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, "DAY")
        and client_continent is not null
      group by -- group by location fields
 client_continent,
 client_continent_code ) last_week on -- join on location fields from the all table.
 all.client_continent = last_week.client_continent
   and all.client_continent_code = last_week.client_continent_code
   left join
     ( SELECT count(*) as test_count, -- which location fields?
 client_continent,
 client_continent_code, -- measurements:
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
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR")
        and client_continent is not null
      group by -- group by location fields
 client_continent,
 client_continent_code ) last_year on -- join on location fields from the all table.
 all.client_continent = last_year.client_continent
   and all.client_continent_code = last_year.client_continent_code
   GROUP BY parent_location_id,
            child_location_name,
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
            last_year_upload_speed_mbps_stddev )
