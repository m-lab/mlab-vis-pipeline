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

  SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
  AVG(packet_retransmit_rate) AS retransmit_avg,

  COUNT(*) AS count,

client_asn_number,
client_continent_code,
client_country_code,
client_region_code,
client_city,
client_continent,
client_country,
client_region,
client_asn_name,
REPLACE(LOWER(CONCAT(IFNULL(client_continent_code, ""), ""
    ,IFNULL(client_country_code, ""), ""
    ,IFNULL(client_region_code, ""), ""
    ,IFNULL(client_city, ""), ""
  )), " ", "") as client_location_key,
DATE(test_date) AS date

FROM
  {0}
WHERE LENGTH(client_asn_number) > 0 AND LENGTH(client_asn_name) > 0

GROUP BY
client_location_key,
client_asn_number,
date,
client_continent_code,
client_country_code,
client_region_code,
client_city,
client_continent,
client_country,
client_region,
client_asn_name
