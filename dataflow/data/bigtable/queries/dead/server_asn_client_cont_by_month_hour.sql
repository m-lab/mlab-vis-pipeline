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

client_continent_code,
server_asn_name,
client_continent,
STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([test_date]), "%Y-%m") as date,
STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([test_date]), "%H") as hour

FROM
  {0}
WHERE LENGTH(client_continent_code) > 0

GROUP BY
client_continent_code,
server_asn_name,
date,
hour,
client_continent

