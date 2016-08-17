SELECT
  nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
  nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,

  -- AVG(download_speed_mbps) AS download_speed_mbps_avg,
  -- AVG(upload_speed_mbps) AS upload_speed_mbps_avg,

  -- MIN(download_speed_mbps) AS download_speed_mbps_min,
  -- MAX(download_speed_mbps) AS download_speed_mbps_max,

  -- MIN(upload_speed_mbps) AS upload_speed_mbps_min,
  -- MAX(upload_speed_mbps) AS upload_speed_mbps_max,

  -- STDDEV(download_speed_mbps) AS download_speed_mbps_stddev,
  -- STDDEV(upload_speed_mbps) AS upload_speed_mbps_stddev,

  COUNT(*) AS count,

server_asn_name,
client_continent_code,
client_country_code,
client_region_code,
client_city,
client_continent,
client_country,
client_region,
REPLACE(LOWER(CONCAT(IFNULL(client_continent_code, ""), ""
    ,IFNULL(client_country_code, ""), ""
    ,IFNULL(client_region_code, ""), ""
    ,IFNULL(client_city, ""), ""
  )), " ", "") as client_location_key,
DATE(test_date) AS date

FROM
  {0}
WHERE 

GROUP BY
client_location_key,
server_asn_name,
date,
client_continent_code,
client_country_code,
client_region_code,
client_city,
client_continent,
client_country,
client_region

