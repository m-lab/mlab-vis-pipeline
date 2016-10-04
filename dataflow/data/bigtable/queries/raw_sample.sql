SELECT
  DATE(local_test_date) AS date,
  REPLACE(LOWER(CONCAT(IFNULL(client_continent_code, ""), IFNULL(client_country_code, ""), IFNULL(client_region_code, ""), IFNULL(client_city, ""), "")), " ", "") AS client_location_key,
  client_city,
  client_country,
  client_region,
  client_asn_name,
  server_asn_name,
  client_latitude,
  client_longitude,
  server_latitude,
  server_longitude,
  download_speed_mbps,
  upload_speed_mbps

FROM {0}
WHERE
  test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
  AND download_speed_mbps > 0
  AND upload_speed_mbps > 0
  AND client_latitude > 0
  AND client_longitude > 0
  AND server_latitude > 0
  AND server_longitude > 0
  AND LENGTH(client_city) > 0
  AND LENGTH(client_country) > 0
  AND LENGTH(client_region) > 0
  AND LENGTH(client_asn_name) > 0
  AND LENGTH(server_asn_name) > 0
LIMIT 5000
