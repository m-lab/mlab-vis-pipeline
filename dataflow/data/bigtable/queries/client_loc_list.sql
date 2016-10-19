
select parent_location_key,
       child_location_key,
       type,
       client_city,
       client_region,
       client_country,
       client_continent,
       client_region_code,
       client_country_code,
       client_continent_code,
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
       last_month_rtt_avg,
       last_month_retransmit_avg,
       last_three_months_test_count,
       last_three_months_download_speed_mbps_median,
       last_three_months_upload_speed_mbps_median,
       last_three_months_download_speed_mbps_avg,
       last_three_months_upload_speed_mbps_avg,
       last_three_months_download_speed_mbps_min,
       last_three_months_upload_speed_mbps_min,
       last_three_months_download_speed_mbps_max,
       last_three_months_upload_speed_mbps_max,
       last_three_months_download_speed_mbps_stddev,
       last_three_months_upload_speed_mbps_stddev,
       last_three_months_rtt_avg,
       last_three_months_retransmit_avg,
       last_six_months_test_count,
       last_six_months_download_speed_mbps_median,
       last_six_months_upload_speed_mbps_median,
       last_six_months_download_speed_mbps_avg,
       last_six_months_upload_speed_mbps_avg,
       last_six_months_download_speed_mbps_min,
       last_six_months_upload_speed_mbps_min,
       last_six_months_download_speed_mbps_max,
       last_six_months_upload_speed_mbps_max,
       last_six_months_download_speed_mbps_stddev,
       last_six_months_upload_speed_mbps_stddev,
       last_six_months_rtt_avg,
       last_six_months_retransmit_avg,
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
       last_year_rtt_avg,
       last_year_retransmit_avg
from -- ============
-- Type: city
-- ============

  (SELECT REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), "",IFNULL(all.client_country_code, ""), "",IFNULL(all.client_region_code, ""), "")), " ", "") as parent_location_key, -- parent key
 REPLACE(LOWER(CONCAT(IFNULL(all.client_city, ""), "")), " ", "") as child_location_key, -- child key
 "city" AS type, -- type
 count(*) as test_count,
 all.client_city as client_city,
 all.client_region as client_region,
 all.client_country as client_country,
 all.client_continent as client_continent,
 all.client_region_code as client_region_code,
 all.client_country_code as client_country_code,
 all.client_continent_code as client_continent_code, -- meta fields we are selecting
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
 last_month.rtt_avg as last_month_rtt_avg,
 last_month.retransmit_avg as last_month_retransmit_avg,
 last_three_months.test_count as last_three_months_test_count,
 last_three_months.download_speed_mbps_median as last_three_months_download_speed_mbps_median,
 last_three_months.upload_speed_mbps_median as last_three_months_upload_speed_mbps_median,
 last_three_months.download_speed_mbps_avg as last_three_months_download_speed_mbps_avg,
 last_three_months.upload_speed_mbps_avg as last_three_months_upload_speed_mbps_avg,
 last_three_months.download_speed_mbps_min as last_three_months_download_speed_mbps_min,
 last_three_months.upload_speed_mbps_min as last_three_months_upload_speed_mbps_min,
 last_three_months.download_speed_mbps_max as last_three_months_download_speed_mbps_max,
 last_three_months.upload_speed_mbps_max as last_three_months_upload_speed_mbps_max,
 last_three_months.download_speed_mbps_stddev as last_three_months_download_speed_mbps_stddev,
 last_three_months.upload_speed_mbps_stddev as last_three_months_upload_speed_mbps_stddev,
 last_three_months.rtt_avg as last_three_months_rtt_avg,
 last_three_months.retransmit_avg as last_three_months_retransmit_avg,
 last_six_months.test_count as last_six_months_test_count,
 last_six_months.download_speed_mbps_median as last_six_months_download_speed_mbps_median,
 last_six_months.upload_speed_mbps_median as last_six_months_upload_speed_mbps_median,
 last_six_months.download_speed_mbps_avg as last_six_months_download_speed_mbps_avg,
 last_six_months.upload_speed_mbps_avg as last_six_months_upload_speed_mbps_avg,
 last_six_months.download_speed_mbps_min as last_six_months_download_speed_mbps_min,
 last_six_months.upload_speed_mbps_min as last_six_months_upload_speed_mbps_min,
 last_six_months.download_speed_mbps_max as last_six_months_download_speed_mbps_max,
 last_six_months.upload_speed_mbps_max as last_six_months_upload_speed_mbps_max,
 last_six_months.download_speed_mbps_stddev as last_six_months_download_speed_mbps_stddev,
 last_six_months.upload_speed_mbps_stddev as last_six_months_upload_speed_mbps_stddev,
 last_six_months.rtt_avg as last_six_months_rtt_avg,
 last_six_months.retransmit_avg as last_six_months_retransmit_avg,
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
 last_year.rtt_avg as last_year_rtt_avg,
 last_year.retransmit_avg as last_year_retransmit_avg -- static fields

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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
        and client_city is not null
      group by -- group by location fields
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_three_months on -- join on location fields from the all table.
 all.client_city = last_three_months.client_city
   and all.client_region = last_three_months.client_region
   and all.client_country = last_three_months.client_country
   and all.client_continent = last_three_months.client_continent
   and all.client_region_code = last_three_months.client_region_code
   and all.client_country_code = last_three_months.client_country_code
   and all.client_continent_code = last_three_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -6, "MONTH")
        and client_city is not null
      group by -- group by location fields
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_six_months on -- join on location fields from the all table.
 all.client_city = last_six_months.client_city
   and all.client_region = last_six_months.client_region
   and all.client_country = last_six_months.client_country
   and all.client_continent = last_six_months.client_continent
   and all.client_region_code = last_six_months.client_region_code
   and all.client_country_code = last_six_months.client_country_code
   and all.client_continent_code = last_six_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
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
   GROUP BY parent_location_key,
            child_location_key,
            client_city,
            client_region,
            client_country,
            client_continent,
            client_region_code,
            client_country_code,
            client_continent_code,
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
            last_month_rtt_avg,
            last_month_retransmit_avg,
            last_three_months_test_count,
            last_three_months_download_speed_mbps_median,
            last_three_months_upload_speed_mbps_median,
            last_three_months_download_speed_mbps_avg,
            last_three_months_upload_speed_mbps_avg,
            last_three_months_download_speed_mbps_min,
            last_three_months_upload_speed_mbps_min,
            last_three_months_download_speed_mbps_max,
            last_three_months_upload_speed_mbps_max,
            last_three_months_download_speed_mbps_stddev,
            last_three_months_upload_speed_mbps_stddev,
            last_three_months_rtt_avg,
            last_three_months_retransmit_avg,
            last_six_months_test_count,
            last_six_months_download_speed_mbps_median,
            last_six_months_upload_speed_mbps_median,
            last_six_months_download_speed_mbps_avg,
            last_six_months_upload_speed_mbps_avg,
            last_six_months_download_speed_mbps_min,
            last_six_months_upload_speed_mbps_min,
            last_six_months_download_speed_mbps_max,
            last_six_months_upload_speed_mbps_max,
            last_six_months_download_speed_mbps_stddev,
            last_six_months_upload_speed_mbps_stddev,
            last_six_months_rtt_avg,
            last_six_months_retransmit_avg,
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
            last_year_rtt_avg,
            last_year_retransmit_avg ) , -- ============
-- Type: region
-- ============

  (SELECT REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), "",IFNULL(all.client_country_code, ""), "")), " ", "") as parent_location_key, -- parent key
 REPLACE(LOWER(CONCAT(IFNULL(all.client_region_code, ""), "")), " ", "") as child_location_key, -- child key
 "region" AS type, -- type
 count(*) as test_count,
 all.client_region as client_region,
 all.client_country as client_country,
 all.client_continent as client_continent,
 all.client_region_code as client_region_code,
 all.client_country_code as client_country_code,
 all.client_continent_code as client_continent_code, -- meta fields we are selecting
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
 last_month.rtt_avg as last_month_rtt_avg,
 last_month.retransmit_avg as last_month_retransmit_avg,
 last_three_months.test_count as last_three_months_test_count,
 last_three_months.download_speed_mbps_median as last_three_months_download_speed_mbps_median,
 last_three_months.upload_speed_mbps_median as last_three_months_upload_speed_mbps_median,
 last_three_months.download_speed_mbps_avg as last_three_months_download_speed_mbps_avg,
 last_three_months.upload_speed_mbps_avg as last_three_months_upload_speed_mbps_avg,
 last_three_months.download_speed_mbps_min as last_three_months_download_speed_mbps_min,
 last_three_months.upload_speed_mbps_min as last_three_months_upload_speed_mbps_min,
 last_three_months.download_speed_mbps_max as last_three_months_download_speed_mbps_max,
 last_three_months.upload_speed_mbps_max as last_three_months_upload_speed_mbps_max,
 last_three_months.download_speed_mbps_stddev as last_three_months_download_speed_mbps_stddev,
 last_three_months.upload_speed_mbps_stddev as last_three_months_upload_speed_mbps_stddev,
 last_three_months.rtt_avg as last_three_months_rtt_avg,
 last_three_months.retransmit_avg as last_three_months_retransmit_avg,
 last_six_months.test_count as last_six_months_test_count,
 last_six_months.download_speed_mbps_median as last_six_months_download_speed_mbps_median,
 last_six_months.upload_speed_mbps_median as last_six_months_upload_speed_mbps_median,
 last_six_months.download_speed_mbps_avg as last_six_months_download_speed_mbps_avg,
 last_six_months.upload_speed_mbps_avg as last_six_months_upload_speed_mbps_avg,
 last_six_months.download_speed_mbps_min as last_six_months_download_speed_mbps_min,
 last_six_months.upload_speed_mbps_min as last_six_months_upload_speed_mbps_min,
 last_six_months.download_speed_mbps_max as last_six_months_download_speed_mbps_max,
 last_six_months.upload_speed_mbps_max as last_six_months_upload_speed_mbps_max,
 last_six_months.download_speed_mbps_stddev as last_six_months_download_speed_mbps_stddev,
 last_six_months.upload_speed_mbps_stddev as last_six_months_upload_speed_mbps_stddev,
 last_six_months.rtt_avg as last_six_months_rtt_avg,
 last_six_months.retransmit_avg as last_six_months_retransmit_avg,
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
 last_year.rtt_avg as last_year_rtt_avg,
 last_year.retransmit_avg as last_year_retransmit_avg -- static fields

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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
        and client_region is not null
      group by -- group by location fields
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_three_months on -- join on location fields from the all table.
 all.client_region = last_three_months.client_region
   and all.client_country = last_three_months.client_country
   and all.client_continent = last_three_months.client_continent
   and all.client_region_code = last_three_months.client_region_code
   and all.client_country_code = last_three_months.client_country_code
   and all.client_continent_code = last_three_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -6, "MONTH")
        and client_region is not null
      group by -- group by location fields
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_six_months on -- join on location fields from the all table.
 all.client_region = last_six_months.client_region
   and all.client_country = last_six_months.client_country
   and all.client_continent = last_six_months.client_continent
   and all.client_region_code = last_six_months.client_region_code
   and all.client_country_code = last_six_months.client_country_code
   and all.client_continent_code = last_six_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
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
   GROUP BY parent_location_key,
            child_location_key,
            client_region,
            client_country,
            client_continent,
            client_region_code,
            client_country_code,
            client_continent_code,
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
            last_month_rtt_avg,
            last_month_retransmit_avg,
            last_three_months_test_count,
            last_three_months_download_speed_mbps_median,
            last_three_months_upload_speed_mbps_median,
            last_three_months_download_speed_mbps_avg,
            last_three_months_upload_speed_mbps_avg,
            last_three_months_download_speed_mbps_min,
            last_three_months_upload_speed_mbps_min,
            last_three_months_download_speed_mbps_max,
            last_three_months_upload_speed_mbps_max,
            last_three_months_download_speed_mbps_stddev,
            last_three_months_upload_speed_mbps_stddev,
            last_three_months_rtt_avg,
            last_three_months_retransmit_avg,
            last_six_months_test_count,
            last_six_months_download_speed_mbps_median,
            last_six_months_upload_speed_mbps_median,
            last_six_months_download_speed_mbps_avg,
            last_six_months_upload_speed_mbps_avg,
            last_six_months_download_speed_mbps_min,
            last_six_months_upload_speed_mbps_min,
            last_six_months_download_speed_mbps_max,
            last_six_months_upload_speed_mbps_max,
            last_six_months_download_speed_mbps_stddev,
            last_six_months_upload_speed_mbps_stddev,
            last_six_months_rtt_avg,
            last_six_months_retransmit_avg,
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
            last_year_rtt_avg,
            last_year_retransmit_avg ) , -- ============
-- Type: country
-- ============

  (SELECT REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), "")), " ", "") as parent_location_key, -- parent key
 REPLACE(LOWER(CONCAT(IFNULL(all.client_country_code, ""), "")), " ", "") as child_location_key, -- child key
 "country" AS type, -- type
 count(*) as test_count,
 all.client_country as client_country,
 all.client_continent as client_continent,
 all.client_country_code as client_country_code,
 all.client_continent_code as client_continent_code, -- meta fields we are selecting
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
 last_month.rtt_avg as last_month_rtt_avg,
 last_month.retransmit_avg as last_month_retransmit_avg,
 last_three_months.test_count as last_three_months_test_count,
 last_three_months.download_speed_mbps_median as last_three_months_download_speed_mbps_median,
 last_three_months.upload_speed_mbps_median as last_three_months_upload_speed_mbps_median,
 last_three_months.download_speed_mbps_avg as last_three_months_download_speed_mbps_avg,
 last_three_months.upload_speed_mbps_avg as last_three_months_upload_speed_mbps_avg,
 last_three_months.download_speed_mbps_min as last_three_months_download_speed_mbps_min,
 last_three_months.upload_speed_mbps_min as last_three_months_upload_speed_mbps_min,
 last_three_months.download_speed_mbps_max as last_three_months_download_speed_mbps_max,
 last_three_months.upload_speed_mbps_max as last_three_months_upload_speed_mbps_max,
 last_three_months.download_speed_mbps_stddev as last_three_months_download_speed_mbps_stddev,
 last_three_months.upload_speed_mbps_stddev as last_three_months_upload_speed_mbps_stddev,
 last_three_months.rtt_avg as last_three_months_rtt_avg,
 last_three_months.retransmit_avg as last_three_months_retransmit_avg,
 last_six_months.test_count as last_six_months_test_count,
 last_six_months.download_speed_mbps_median as last_six_months_download_speed_mbps_median,
 last_six_months.upload_speed_mbps_median as last_six_months_upload_speed_mbps_median,
 last_six_months.download_speed_mbps_avg as last_six_months_download_speed_mbps_avg,
 last_six_months.upload_speed_mbps_avg as last_six_months_upload_speed_mbps_avg,
 last_six_months.download_speed_mbps_min as last_six_months_download_speed_mbps_min,
 last_six_months.upload_speed_mbps_min as last_six_months_upload_speed_mbps_min,
 last_six_months.download_speed_mbps_max as last_six_months_download_speed_mbps_max,
 last_six_months.upload_speed_mbps_max as last_six_months_upload_speed_mbps_max,
 last_six_months.download_speed_mbps_stddev as last_six_months_download_speed_mbps_stddev,
 last_six_months.upload_speed_mbps_stddev as last_six_months_upload_speed_mbps_stddev,
 last_six_months.rtt_avg as last_six_months_rtt_avg,
 last_six_months.retransmit_avg as last_six_months_retransmit_avg,
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
 last_year.rtt_avg as last_year_rtt_avg,
 last_year.retransmit_avg as last_year_retransmit_avg -- static fields

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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
        and client_country is not null
      group by -- group by location fields
 client_country,
 client_continent,
 client_country_code,
 client_continent_code ) last_three_months on -- join on location fields from the all table.
 all.client_country = last_three_months.client_country
   and all.client_continent = last_three_months.client_continent
   and all.client_country_code = last_three_months.client_country_code
   and all.client_continent_code = last_three_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -6, "MONTH")
        and client_country is not null
      group by -- group by location fields
 client_country,
 client_continent,
 client_country_code,
 client_continent_code ) last_six_months on -- join on location fields from the all table.
 all.client_country = last_six_months.client_country
   and all.client_continent = last_six_months.client_continent
   and all.client_country_code = last_six_months.client_country_code
   and all.client_continent_code = last_six_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
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
   GROUP BY parent_location_key,
            child_location_key,
            client_country,
            client_continent,
            client_country_code,
            client_continent_code,
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
            last_month_rtt_avg,
            last_month_retransmit_avg,
            last_three_months_test_count,
            last_three_months_download_speed_mbps_median,
            last_three_months_upload_speed_mbps_median,
            last_three_months_download_speed_mbps_avg,
            last_three_months_upload_speed_mbps_avg,
            last_three_months_download_speed_mbps_min,
            last_three_months_upload_speed_mbps_min,
            last_three_months_download_speed_mbps_max,
            last_three_months_upload_speed_mbps_max,
            last_three_months_download_speed_mbps_stddev,
            last_three_months_upload_speed_mbps_stddev,
            last_three_months_rtt_avg,
            last_three_months_retransmit_avg,
            last_six_months_test_count,
            last_six_months_download_speed_mbps_median,
            last_six_months_upload_speed_mbps_median,
            last_six_months_download_speed_mbps_avg,
            last_six_months_upload_speed_mbps_avg,
            last_six_months_download_speed_mbps_min,
            last_six_months_upload_speed_mbps_min,
            last_six_months_download_speed_mbps_max,
            last_six_months_upload_speed_mbps_max,
            last_six_months_download_speed_mbps_stddev,
            last_six_months_upload_speed_mbps_stddev,
            last_six_months_rtt_avg,
            last_six_months_retransmit_avg,
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
            last_year_rtt_avg,
            last_year_retransmit_avg ) , -- ============
-- Type: continent
-- ============

  (SELECT "" as parent_location_key, -- parent key
 REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), "")), " ", "") as child_location_key, -- child key
 "continent" AS type, -- type
 count(*) as test_count,
 all.client_continent as client_continent,
 all.client_continent_code as client_continent_code, -- meta fields we are selecting
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
 last_month.rtt_avg as last_month_rtt_avg,
 last_month.retransmit_avg as last_month_retransmit_avg,
 last_three_months.test_count as last_three_months_test_count,
 last_three_months.download_speed_mbps_median as last_three_months_download_speed_mbps_median,
 last_three_months.upload_speed_mbps_median as last_three_months_upload_speed_mbps_median,
 last_three_months.download_speed_mbps_avg as last_three_months_download_speed_mbps_avg,
 last_three_months.upload_speed_mbps_avg as last_three_months_upload_speed_mbps_avg,
 last_three_months.download_speed_mbps_min as last_three_months_download_speed_mbps_min,
 last_three_months.upload_speed_mbps_min as last_three_months_upload_speed_mbps_min,
 last_three_months.download_speed_mbps_max as last_three_months_download_speed_mbps_max,
 last_three_months.upload_speed_mbps_max as last_three_months_upload_speed_mbps_max,
 last_three_months.download_speed_mbps_stddev as last_three_months_download_speed_mbps_stddev,
 last_three_months.upload_speed_mbps_stddev as last_three_months_upload_speed_mbps_stddev,
 last_three_months.rtt_avg as last_three_months_rtt_avg,
 last_three_months.retransmit_avg as last_three_months_retransmit_avg,
 last_six_months.test_count as last_six_months_test_count,
 last_six_months.download_speed_mbps_median as last_six_months_download_speed_mbps_median,
 last_six_months.upload_speed_mbps_median as last_six_months_upload_speed_mbps_median,
 last_six_months.download_speed_mbps_avg as last_six_months_download_speed_mbps_avg,
 last_six_months.upload_speed_mbps_avg as last_six_months_upload_speed_mbps_avg,
 last_six_months.download_speed_mbps_min as last_six_months_download_speed_mbps_min,
 last_six_months.upload_speed_mbps_min as last_six_months_upload_speed_mbps_min,
 last_six_months.download_speed_mbps_max as last_six_months_download_speed_mbps_max,
 last_six_months.upload_speed_mbps_max as last_six_months_upload_speed_mbps_max,
 last_six_months.download_speed_mbps_stddev as last_six_months_download_speed_mbps_stddev,
 last_six_months.upload_speed_mbps_stddev as last_six_months_upload_speed_mbps_stddev,
 last_six_months.rtt_avg as last_six_months_rtt_avg,
 last_six_months.retransmit_avg as last_six_months_retransmit_avg,
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
 last_year.rtt_avg as last_year_rtt_avg,
 last_year.retransmit_avg as last_year_retransmit_avg -- static fields

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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
        and client_continent is not null
      group by -- group by location fields
 client_continent,
 client_continent_code ) last_three_months on -- join on location fields from the all table.
 all.client_continent = last_three_months.client_continent
   and all.client_continent_code = last_three_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -6, "MONTH")
        and client_continent is not null
      group by -- group by location fields
 client_continent,
 client_continent_code ) last_six_months on -- join on location fields from the all table.
 all.client_continent = last_six_months.client_continent
   and all.client_continent_code = last_six_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR")
        and client_continent is not null
      group by -- group by location fields
 client_continent,
 client_continent_code ) last_year on -- join on location fields from the all table.
 all.client_continent = last_year.client_continent
   and all.client_continent_code = last_year.client_continent_code
   GROUP BY parent_location_key,
            child_location_key,
            client_continent,
            client_continent_code,
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
            last_month_rtt_avg,
            last_month_retransmit_avg,
            last_three_months_test_count,
            last_three_months_download_speed_mbps_median,
            last_three_months_upload_speed_mbps_median,
            last_three_months_download_speed_mbps_avg,
            last_three_months_upload_speed_mbps_avg,
            last_three_months_download_speed_mbps_min,
            last_three_months_upload_speed_mbps_min,
            last_three_months_download_speed_mbps_max,
            last_three_months_upload_speed_mbps_max,
            last_three_months_download_speed_mbps_stddev,
            last_three_months_upload_speed_mbps_stddev,
            last_three_months_rtt_avg,
            last_three_months_retransmit_avg,
            last_six_months_test_count,
            last_six_months_download_speed_mbps_median,
            last_six_months_upload_speed_mbps_median,
            last_six_months_download_speed_mbps_avg,
            last_six_months_upload_speed_mbps_avg,
            last_six_months_download_speed_mbps_min,
            last_six_months_upload_speed_mbps_min,
            last_six_months_download_speed_mbps_max,
            last_six_months_upload_speed_mbps_max,
            last_six_months_download_speed_mbps_stddev,
            last_six_months_upload_speed_mbps_stddev,
            last_six_months_rtt_avg,
            last_six_months_retransmit_avg,
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
            last_year_rtt_avg,
            last_year_retransmit_avg ) , -- ============
-- Type: city
-- ============

  (SELECT "info" as parent_location_key, -- parent key
 REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), "",IFNULL(all.client_country_code, ""), "",IFNULL(all.client_region_code, ""), "",IFNULL(all.client_city, ""), "")), " ", "") as child_location_key, -- child key
 "city" AS type, -- type
 count(*) as test_count,
 all.client_city as client_city,
 all.client_region as client_region,
 all.client_country as client_country,
 all.client_continent as client_continent,
 all.client_region_code as client_region_code,
 all.client_country_code as client_country_code,
 all.client_continent_code as client_continent_code, -- meta fields we are selecting
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
 last_month.rtt_avg as last_month_rtt_avg,
 last_month.retransmit_avg as last_month_retransmit_avg,
 last_three_months.test_count as last_three_months_test_count,
 last_three_months.download_speed_mbps_median as last_three_months_download_speed_mbps_median,
 last_three_months.upload_speed_mbps_median as last_three_months_upload_speed_mbps_median,
 last_three_months.download_speed_mbps_avg as last_three_months_download_speed_mbps_avg,
 last_three_months.upload_speed_mbps_avg as last_three_months_upload_speed_mbps_avg,
 last_three_months.download_speed_mbps_min as last_three_months_download_speed_mbps_min,
 last_three_months.upload_speed_mbps_min as last_three_months_upload_speed_mbps_min,
 last_three_months.download_speed_mbps_max as last_three_months_download_speed_mbps_max,
 last_three_months.upload_speed_mbps_max as last_three_months_upload_speed_mbps_max,
 last_three_months.download_speed_mbps_stddev as last_three_months_download_speed_mbps_stddev,
 last_three_months.upload_speed_mbps_stddev as last_three_months_upload_speed_mbps_stddev,
 last_three_months.rtt_avg as last_three_months_rtt_avg,
 last_three_months.retransmit_avg as last_three_months_retransmit_avg,
 last_six_months.test_count as last_six_months_test_count,
 last_six_months.download_speed_mbps_median as last_six_months_download_speed_mbps_median,
 last_six_months.upload_speed_mbps_median as last_six_months_upload_speed_mbps_median,
 last_six_months.download_speed_mbps_avg as last_six_months_download_speed_mbps_avg,
 last_six_months.upload_speed_mbps_avg as last_six_months_upload_speed_mbps_avg,
 last_six_months.download_speed_mbps_min as last_six_months_download_speed_mbps_min,
 last_six_months.upload_speed_mbps_min as last_six_months_upload_speed_mbps_min,
 last_six_months.download_speed_mbps_max as last_six_months_download_speed_mbps_max,
 last_six_months.upload_speed_mbps_max as last_six_months_upload_speed_mbps_max,
 last_six_months.download_speed_mbps_stddev as last_six_months_download_speed_mbps_stddev,
 last_six_months.upload_speed_mbps_stddev as last_six_months_upload_speed_mbps_stddev,
 last_six_months.rtt_avg as last_six_months_rtt_avg,
 last_six_months.retransmit_avg as last_six_months_retransmit_avg,
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
 last_year.rtt_avg as last_year_rtt_avg,
 last_year.retransmit_avg as last_year_retransmit_avg -- static fields

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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
        and client_city is not null
      group by -- group by location fields
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_three_months on -- join on location fields from the all table.
 all.client_city = last_three_months.client_city
   and all.client_region = last_three_months.client_region
   and all.client_country = last_three_months.client_country
   and all.client_continent = last_three_months.client_continent
   and all.client_region_code = last_three_months.client_region_code
   and all.client_country_code = last_three_months.client_country_code
   and all.client_continent_code = last_three_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -6, "MONTH")
        and client_city is not null
      group by -- group by location fields
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_six_months on -- join on location fields from the all table.
 all.client_city = last_six_months.client_city
   and all.client_region = last_six_months.client_region
   and all.client_country = last_six_months.client_country
   and all.client_continent = last_six_months.client_continent
   and all.client_region_code = last_six_months.client_region_code
   and all.client_country_code = last_six_months.client_country_code
   and all.client_continent_code = last_six_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
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
   WHERE LENGTH(all.client_city) > 0
     AND LENGTH(all.client_region) > 0
     AND LENGTH(all.client_country) > 0
     AND LENGTH(all.client_continent) > 0
     AND LENGTH(all.client_region_code) > 0
     AND LENGTH(all.client_country_code) > 0
     AND LENGTH(all.client_continent_code) > 0
   GROUP BY parent_location_key,
            child_location_key,
            client_city,
            client_region,
            client_country,
            client_continent,
            client_region_code,
            client_country_code,
            client_continent_code,
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
            last_month_rtt_avg,
            last_month_retransmit_avg,
            last_three_months_test_count,
            last_three_months_download_speed_mbps_median,
            last_three_months_upload_speed_mbps_median,
            last_three_months_download_speed_mbps_avg,
            last_three_months_upload_speed_mbps_avg,
            last_three_months_download_speed_mbps_min,
            last_three_months_upload_speed_mbps_min,
            last_three_months_download_speed_mbps_max,
            last_three_months_upload_speed_mbps_max,
            last_three_months_download_speed_mbps_stddev,
            last_three_months_upload_speed_mbps_stddev,
            last_three_months_rtt_avg,
            last_three_months_retransmit_avg,
            last_six_months_test_count,
            last_six_months_download_speed_mbps_median,
            last_six_months_upload_speed_mbps_median,
            last_six_months_download_speed_mbps_avg,
            last_six_months_upload_speed_mbps_avg,
            last_six_months_download_speed_mbps_min,
            last_six_months_upload_speed_mbps_min,
            last_six_months_download_speed_mbps_max,
            last_six_months_upload_speed_mbps_max,
            last_six_months_download_speed_mbps_stddev,
            last_six_months_upload_speed_mbps_stddev,
            last_six_months_rtt_avg,
            last_six_months_retransmit_avg,
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
            last_year_rtt_avg,
            last_year_retransmit_avg ) , -- ============
-- Type: region
-- ============

  (SELECT "info" as parent_location_key, -- parent key
 REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), "",IFNULL(all.client_country_code, ""), "",IFNULL(all.client_region_code, ""), "")), " ", "") as child_location_key, -- child key
 "region" AS type, -- type
 count(*) as test_count,
 all.client_region as client_region,
 all.client_country as client_country,
 all.client_continent as client_continent,
 all.client_region_code as client_region_code,
 all.client_country_code as client_country_code,
 all.client_continent_code as client_continent_code, -- meta fields we are selecting
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
 last_month.rtt_avg as last_month_rtt_avg,
 last_month.retransmit_avg as last_month_retransmit_avg,
 last_three_months.test_count as last_three_months_test_count,
 last_three_months.download_speed_mbps_median as last_three_months_download_speed_mbps_median,
 last_three_months.upload_speed_mbps_median as last_three_months_upload_speed_mbps_median,
 last_three_months.download_speed_mbps_avg as last_three_months_download_speed_mbps_avg,
 last_three_months.upload_speed_mbps_avg as last_three_months_upload_speed_mbps_avg,
 last_three_months.download_speed_mbps_min as last_three_months_download_speed_mbps_min,
 last_three_months.upload_speed_mbps_min as last_three_months_upload_speed_mbps_min,
 last_three_months.download_speed_mbps_max as last_three_months_download_speed_mbps_max,
 last_three_months.upload_speed_mbps_max as last_three_months_upload_speed_mbps_max,
 last_three_months.download_speed_mbps_stddev as last_three_months_download_speed_mbps_stddev,
 last_three_months.upload_speed_mbps_stddev as last_three_months_upload_speed_mbps_stddev,
 last_three_months.rtt_avg as last_three_months_rtt_avg,
 last_three_months.retransmit_avg as last_three_months_retransmit_avg,
 last_six_months.test_count as last_six_months_test_count,
 last_six_months.download_speed_mbps_median as last_six_months_download_speed_mbps_median,
 last_six_months.upload_speed_mbps_median as last_six_months_upload_speed_mbps_median,
 last_six_months.download_speed_mbps_avg as last_six_months_download_speed_mbps_avg,
 last_six_months.upload_speed_mbps_avg as last_six_months_upload_speed_mbps_avg,
 last_six_months.download_speed_mbps_min as last_six_months_download_speed_mbps_min,
 last_six_months.upload_speed_mbps_min as last_six_months_upload_speed_mbps_min,
 last_six_months.download_speed_mbps_max as last_six_months_download_speed_mbps_max,
 last_six_months.upload_speed_mbps_max as last_six_months_upload_speed_mbps_max,
 last_six_months.download_speed_mbps_stddev as last_six_months_download_speed_mbps_stddev,
 last_six_months.upload_speed_mbps_stddev as last_six_months_upload_speed_mbps_stddev,
 last_six_months.rtt_avg as last_six_months_rtt_avg,
 last_six_months.retransmit_avg as last_six_months_retransmit_avg,
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
 last_year.rtt_avg as last_year_rtt_avg,
 last_year.retransmit_avg as last_year_retransmit_avg -- static fields

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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
        and client_region_code is not null
      group by -- group by location fields
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_three_months on -- join on location fields from the all table.
 all.client_region = last_three_months.client_region
   and all.client_country = last_three_months.client_country
   and all.client_continent = last_three_months.client_continent
   and all.client_region_code = last_three_months.client_region_code
   and all.client_country_code = last_three_months.client_country_code
   and all.client_continent_code = last_three_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH")
        and client_region_code is not null
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -6, "MONTH")
        and client_region_code is not null
      group by -- group by location fields
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) last_six_months on -- join on location fields from the all table.
 all.client_region = last_six_months.client_region
   and all.client_country = last_six_months.client_country
   and all.client_continent = last_six_months.client_continent
   and all.client_region_code = last_six_months.client_region_code
   and all.client_country_code = last_six_months.client_country_code
   and all.client_continent_code = last_six_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR")
        and client_region_code is not null
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
   WHERE LENGTH(all.client_region) > 0
     AND LENGTH(all.client_country) > 0
     AND LENGTH(all.client_continent) > 0
     AND LENGTH(all.client_region_code) > 0
     AND LENGTH(all.client_country_code) > 0
     AND LENGTH(all.client_continent_code) > 0
   GROUP BY parent_location_key,
            child_location_key,
            client_region,
            client_country,
            client_continent,
            client_region_code,
            client_country_code,
            client_continent_code,
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
            last_month_rtt_avg,
            last_month_retransmit_avg,
            last_three_months_test_count,
            last_three_months_download_speed_mbps_median,
            last_three_months_upload_speed_mbps_median,
            last_three_months_download_speed_mbps_avg,
            last_three_months_upload_speed_mbps_avg,
            last_three_months_download_speed_mbps_min,
            last_three_months_upload_speed_mbps_min,
            last_three_months_download_speed_mbps_max,
            last_three_months_upload_speed_mbps_max,
            last_three_months_download_speed_mbps_stddev,
            last_three_months_upload_speed_mbps_stddev,
            last_three_months_rtt_avg,
            last_three_months_retransmit_avg,
            last_six_months_test_count,
            last_six_months_download_speed_mbps_median,
            last_six_months_upload_speed_mbps_median,
            last_six_months_download_speed_mbps_avg,
            last_six_months_upload_speed_mbps_avg,
            last_six_months_download_speed_mbps_min,
            last_six_months_upload_speed_mbps_min,
            last_six_months_download_speed_mbps_max,
            last_six_months_upload_speed_mbps_max,
            last_six_months_download_speed_mbps_stddev,
            last_six_months_upload_speed_mbps_stddev,
            last_six_months_rtt_avg,
            last_six_months_retransmit_avg,
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
            last_year_rtt_avg,
            last_year_retransmit_avg ) , -- ============
-- Type: country
-- ============

  (SELECT "info" as parent_location_key, -- parent key
 REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), "",IFNULL(all.client_country_code, ""), "")), " ", "") as child_location_key, -- child key
 "country" AS type, -- type
 count(*) as test_count,
 all.client_country as client_country,
 all.client_continent as client_continent,
 all.client_country_code as client_country_code,
 all.client_continent_code as client_continent_code, -- meta fields we are selecting
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
 last_month.rtt_avg as last_month_rtt_avg,
 last_month.retransmit_avg as last_month_retransmit_avg,
 last_three_months.test_count as last_three_months_test_count,
 last_three_months.download_speed_mbps_median as last_three_months_download_speed_mbps_median,
 last_three_months.upload_speed_mbps_median as last_three_months_upload_speed_mbps_median,
 last_three_months.download_speed_mbps_avg as last_three_months_download_speed_mbps_avg,
 last_three_months.upload_speed_mbps_avg as last_three_months_upload_speed_mbps_avg,
 last_three_months.download_speed_mbps_min as last_three_months_download_speed_mbps_min,
 last_three_months.upload_speed_mbps_min as last_three_months_upload_speed_mbps_min,
 last_three_months.download_speed_mbps_max as last_three_months_download_speed_mbps_max,
 last_three_months.upload_speed_mbps_max as last_three_months_upload_speed_mbps_max,
 last_three_months.download_speed_mbps_stddev as last_three_months_download_speed_mbps_stddev,
 last_three_months.upload_speed_mbps_stddev as last_three_months_upload_speed_mbps_stddev,
 last_three_months.rtt_avg as last_three_months_rtt_avg,
 last_three_months.retransmit_avg as last_three_months_retransmit_avg,
 last_six_months.test_count as last_six_months_test_count,
 last_six_months.download_speed_mbps_median as last_six_months_download_speed_mbps_median,
 last_six_months.upload_speed_mbps_median as last_six_months_upload_speed_mbps_median,
 last_six_months.download_speed_mbps_avg as last_six_months_download_speed_mbps_avg,
 last_six_months.upload_speed_mbps_avg as last_six_months_upload_speed_mbps_avg,
 last_six_months.download_speed_mbps_min as last_six_months_download_speed_mbps_min,
 last_six_months.upload_speed_mbps_min as last_six_months_upload_speed_mbps_min,
 last_six_months.download_speed_mbps_max as last_six_months_download_speed_mbps_max,
 last_six_months.upload_speed_mbps_max as last_six_months_upload_speed_mbps_max,
 last_six_months.download_speed_mbps_stddev as last_six_months_download_speed_mbps_stddev,
 last_six_months.upload_speed_mbps_stddev as last_six_months_upload_speed_mbps_stddev,
 last_six_months.rtt_avg as last_six_months_rtt_avg,
 last_six_months.retransmit_avg as last_six_months_retransmit_avg,
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
 last_year.rtt_avg as last_year_rtt_avg,
 last_year.retransmit_avg as last_year_retransmit_avg -- static fields

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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
        and client_country_code is not null
      group by -- group by location fields
 client_country,
 client_continent,
 client_country_code,
 client_continent_code ) last_three_months on -- join on location fields from the all table.
 all.client_country = last_three_months.client_country
   and all.client_continent = last_three_months.client_continent
   and all.client_country_code = last_three_months.client_country_code
   and all.client_continent_code = last_three_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH")
        and client_country_code is not null
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -6, "MONTH")
        and client_country_code is not null
      group by -- group by location fields
 client_country,
 client_continent,
 client_country_code,
 client_continent_code ) last_six_months on -- join on location fields from the all table.
 all.client_country = last_six_months.client_country
   and all.client_continent = last_six_months.client_continent
   and all.client_country_code = last_six_months.client_country_code
   and all.client_continent_code = last_six_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR")
        and client_country_code is not null
      group by -- group by location fields
 client_country,
 client_continent,
 client_country_code,
 client_continent_code ) last_year on -- join on location fields from the all table.
 all.client_country = last_year.client_country
   and all.client_continent = last_year.client_continent
   and all.client_country_code = last_year.client_country_code
   and all.client_continent_code = last_year.client_continent_code
   WHERE LENGTH(all.client_country) > 0
     AND LENGTH(all.client_continent) > 0
     AND LENGTH(all.client_country_code) > 0
     AND LENGTH(all.client_continent_code) > 0
   GROUP BY parent_location_key,
            child_location_key,
            client_country,
            client_continent,
            client_country_code,
            client_continent_code,
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
            last_month_rtt_avg,
            last_month_retransmit_avg,
            last_three_months_test_count,
            last_three_months_download_speed_mbps_median,
            last_three_months_upload_speed_mbps_median,
            last_three_months_download_speed_mbps_avg,
            last_three_months_upload_speed_mbps_avg,
            last_three_months_download_speed_mbps_min,
            last_three_months_upload_speed_mbps_min,
            last_three_months_download_speed_mbps_max,
            last_three_months_upload_speed_mbps_max,
            last_three_months_download_speed_mbps_stddev,
            last_three_months_upload_speed_mbps_stddev,
            last_three_months_rtt_avg,
            last_three_months_retransmit_avg,
            last_six_months_test_count,
            last_six_months_download_speed_mbps_median,
            last_six_months_upload_speed_mbps_median,
            last_six_months_download_speed_mbps_avg,
            last_six_months_upload_speed_mbps_avg,
            last_six_months_download_speed_mbps_min,
            last_six_months_upload_speed_mbps_min,
            last_six_months_download_speed_mbps_max,
            last_six_months_upload_speed_mbps_max,
            last_six_months_download_speed_mbps_stddev,
            last_six_months_upload_speed_mbps_stddev,
            last_six_months_rtt_avg,
            last_six_months_retransmit_avg,
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
            last_year_rtt_avg,
            last_year_retransmit_avg ) , -- ============
-- Type: continent
-- ============

  (SELECT "info" as parent_location_key, -- parent key
 REPLACE(LOWER(CONCAT(IFNULL(all.client_continent_code, ""), "")), " ", "") as child_location_key, -- child key
 "continent" AS type, -- type
 count(*) as test_count,
 all.client_continent as client_continent,
 all.client_continent_code as client_continent_code, -- meta fields we are selecting
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
 last_month.rtt_avg as last_month_rtt_avg,
 last_month.retransmit_avg as last_month_retransmit_avg,
 last_three_months.test_count as last_three_months_test_count,
 last_three_months.download_speed_mbps_median as last_three_months_download_speed_mbps_median,
 last_three_months.upload_speed_mbps_median as last_three_months_upload_speed_mbps_median,
 last_three_months.download_speed_mbps_avg as last_three_months_download_speed_mbps_avg,
 last_three_months.upload_speed_mbps_avg as last_three_months_upload_speed_mbps_avg,
 last_three_months.download_speed_mbps_min as last_three_months_download_speed_mbps_min,
 last_three_months.upload_speed_mbps_min as last_three_months_upload_speed_mbps_min,
 last_three_months.download_speed_mbps_max as last_three_months_download_speed_mbps_max,
 last_three_months.upload_speed_mbps_max as last_three_months_upload_speed_mbps_max,
 last_three_months.download_speed_mbps_stddev as last_three_months_download_speed_mbps_stddev,
 last_three_months.upload_speed_mbps_stddev as last_three_months_upload_speed_mbps_stddev,
 last_three_months.rtt_avg as last_three_months_rtt_avg,
 last_three_months.retransmit_avg as last_three_months_retransmit_avg,
 last_six_months.test_count as last_six_months_test_count,
 last_six_months.download_speed_mbps_median as last_six_months_download_speed_mbps_median,
 last_six_months.upload_speed_mbps_median as last_six_months_upload_speed_mbps_median,
 last_six_months.download_speed_mbps_avg as last_six_months_download_speed_mbps_avg,
 last_six_months.upload_speed_mbps_avg as last_six_months_upload_speed_mbps_avg,
 last_six_months.download_speed_mbps_min as last_six_months_download_speed_mbps_min,
 last_six_months.upload_speed_mbps_min as last_six_months_upload_speed_mbps_min,
 last_six_months.download_speed_mbps_max as last_six_months_download_speed_mbps_max,
 last_six_months.upload_speed_mbps_max as last_six_months_upload_speed_mbps_max,
 last_six_months.download_speed_mbps_stddev as last_six_months_download_speed_mbps_stddev,
 last_six_months.upload_speed_mbps_stddev as last_six_months_upload_speed_mbps_stddev,
 last_six_months.rtt_avg as last_six_months_rtt_avg,
 last_six_months.retransmit_avg as last_six_months_retransmit_avg,
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
 last_year.rtt_avg as last_year_rtt_avg,
 last_year.retransmit_avg as last_year_retransmit_avg -- static fields

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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
        and client_continent_code is not null
      group by -- group by location fields
 client_continent,
 client_continent_code ) last_three_months on -- join on location fields from the all table.
 all.client_continent = last_three_months.client_continent
   and all.client_continent_code = last_three_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH")
        and client_continent_code is not null
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -6, "MONTH")
        and client_continent_code is not null
      group by -- group by location fields
 client_continent,
 client_continent_code ) last_six_months on -- join on location fields from the all table.
 all.client_continent = last_six_months.client_continent
   and all.client_continent_code = last_six_months.client_continent_code
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
 SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
 AVG(packet_retransmit_rate) AS retransmit_avg,
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR")
        and client_continent_code is not null
      group by -- group by location fields
 client_continent,
 client_continent_code ) last_year on -- join on location fields from the all table.
 all.client_continent = last_year.client_continent
   and all.client_continent_code = last_year.client_continent_code
   WHERE LENGTH(all.client_continent) > 0
     AND LENGTH(all.client_continent_code) > 0
   GROUP BY parent_location_key,
            child_location_key,
            client_continent,
            client_continent_code,
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
            last_month_rtt_avg,
            last_month_retransmit_avg,
            last_three_months_test_count,
            last_three_months_download_speed_mbps_median,
            last_three_months_upload_speed_mbps_median,
            last_three_months_download_speed_mbps_avg,
            last_three_months_upload_speed_mbps_avg,
            last_three_months_download_speed_mbps_min,
            last_three_months_upload_speed_mbps_min,
            last_three_months_download_speed_mbps_max,
            last_three_months_upload_speed_mbps_max,
            last_three_months_download_speed_mbps_stddev,
            last_three_months_upload_speed_mbps_stddev,
            last_three_months_rtt_avg,
            last_three_months_retransmit_avg,
            last_six_months_test_count,
            last_six_months_download_speed_mbps_median,
            last_six_months_upload_speed_mbps_median,
            last_six_months_download_speed_mbps_avg,
            last_six_months_upload_speed_mbps_avg,
            last_six_months_download_speed_mbps_min,
            last_six_months_upload_speed_mbps_min,
            last_six_months_download_speed_mbps_max,
            last_six_months_upload_speed_mbps_max,
            last_six_months_download_speed_mbps_stddev,
            last_six_months_upload_speed_mbps_stddev,
            last_six_months_rtt_avg,
            last_six_months_retransmit_avg,
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
            last_year_rtt_avg,
            last_year_retransmit_avg )