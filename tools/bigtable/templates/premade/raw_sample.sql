SELECT
  [date],
  client_location_key,
  client_city,
  client_region,
  client_country,
  client_continent_code,
  client_asn_name,
  server_asn_name,
  client_latitude,
  client_longitude,
  server_latitude,
  server_longitude,
  download_speed_mbps,
  upload_speed_mbps

FROM
(
  SELECT
  DATE(local_test_date) AS date,
  local_test_date,
  REPLACE(LOWER(CONCAT(IFNULL(client_continent_code, ""), IFNULL(client_country_code, ""), IFNULL(client_region_code, ""), IFNULL(client_city, ""), "")), " ", "") AS client_location_key,
  client_city,
  client_region,
  client_country,
  client_continent_code,
  client_asn_name,
  server_asn_name,
  client_latitude,
  client_longitude,
  server_latitude,
  server_longitude,
  download_speed_mbps,
  upload_speed_mbps,
  rand() as random
  FROM {0}
  WHERE client_continent_code = "NA"
  AND local_test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
  AND download_speed_mbps > 0
  AND upload_speed_mbps > 0
  AND client_latitude != 0.0
  AND client_longitude != 0.0
  AND server_latitude != 0.0
  AND server_longitude != 0.0
  AND LENGTH(client_city) > 0
  AND LENGTH(client_country) > 0
  AND LENGTH(client_region) > 0
  AND LENGTH(client_asn_name) > 0
  AND LENGTH(server_asn_name) > 0
  ORDER BY random
  LIMIT 1000
),
(
  SELECT
  DATE(local_test_date) AS date,
  local_test_date,
  REPLACE(LOWER(CONCAT(IFNULL(client_continent_code, ""), IFNULL(client_country_code, ""), IFNULL(client_region_code, ""), IFNULL(client_city, ""), "")), " ", "") AS client_location_key,
  client_city,
  client_region,
  client_country,
  client_continent_code,
  client_asn_name,
  server_asn_name,
  client_latitude,
  client_longitude,
  server_latitude,
  server_longitude,
  download_speed_mbps,
  upload_speed_mbps,
  rand() as random
  FROM {0}
  WHERE client_continent_code = "SA"
  AND local_test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
  AND download_speed_mbps > 0
  AND upload_speed_mbps > 0
  AND client_latitude != 0.0
  AND client_longitude != 0.0
  AND server_latitude != 0.0
  AND server_longitude != 0.0
  AND LENGTH(client_city) > 0
  AND LENGTH(client_country) > 0
  AND LENGTH(client_region) > 0
  AND LENGTH(client_asn_name) > 0
  AND LENGTH(server_asn_name) > 0
  ORDER BY random
  LIMIT 1000
),
(
  SELECT
  DATE(local_test_date) AS date,
  local_test_date,
  REPLACE(LOWER(CONCAT(IFNULL(client_continent_code, ""), IFNULL(client_country_code, ""), IFNULL(client_region_code, ""), IFNULL(client_city, ""), "")), " ", "") AS client_location_key,
  client_city,
  client_region,
  client_country,
  client_continent_code,
  client_asn_name,
  server_asn_name,
  client_latitude,
  client_longitude,
  server_latitude,
  server_longitude,
  download_speed_mbps,
  upload_speed_mbps,
  rand() as random
  FROM {0}
  WHERE client_continent_code = "EU"
  AND local_test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
  AND download_speed_mbps > 0
  AND upload_speed_mbps > 0
  AND client_latitude != 0.0
  AND client_longitude != 0.0
  AND server_latitude != 0.0
  AND server_longitude != 0.0
  AND LENGTH(client_city) > 0
  AND LENGTH(client_country) > 0
  AND LENGTH(client_region) > 0
  AND LENGTH(client_asn_name) > 0
  AND LENGTH(server_asn_name) > 0
  ORDER BY random
  LIMIT 1000
),
(
  SELECT
  DATE(local_test_date) AS date,
  local_test_date,
  REPLACE(LOWER(CONCAT(IFNULL(client_continent_code, ""), IFNULL(client_country_code, ""), IFNULL(client_region_code, ""), IFNULL(client_city, ""), "")), " ", "") AS client_location_key,
  client_city,
  client_region,
  client_country,
  client_continent_code,
  client_asn_name,
  server_asn_name,
  client_latitude,
  client_longitude,
  server_latitude,
  server_longitude,
  download_speed_mbps,
  upload_speed_mbps,
  rand() as random
  FROM {0}
  WHERE client_continent_code = "OC"
  AND local_test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
  AND download_speed_mbps > 0
  AND upload_speed_mbps > 0
  AND client_latitude != 0.0
  AND client_longitude != 0.0
  AND server_latitude != 0.0
  AND server_longitude != 0.0
  AND LENGTH(client_city) > 0
  AND LENGTH(client_country) > 0
  AND LENGTH(client_region) > 0
  AND LENGTH(client_asn_name) > 0
  AND LENGTH(server_asn_name) > 0
  ORDER BY random
  LIMIT 1000
),
(
  SELECT
  DATE(local_test_date) AS date,
  local_test_date,
  REPLACE(LOWER(CONCAT(IFNULL(client_continent_code, ""), IFNULL(client_country_code, ""), IFNULL(client_region_code, ""), IFNULL(client_city, ""), "")), " ", "") AS client_location_key,
  client_city,
  client_region,
  client_country,
  client_continent_code,
  client_asn_name,
  server_asn_name,
  client_latitude,
  client_longitude,
  server_latitude,
  server_longitude,
  download_speed_mbps,
  upload_speed_mbps,
  rand() as random
  FROM {0}
  WHERE client_continent_code = "AS"
  AND local_test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
  AND download_speed_mbps > 0
  AND upload_speed_mbps > 0
  AND client_latitude != 0.0
  AND client_longitude != 0.0
  AND server_latitude != 0.0
  AND server_longitude != 0.0
  AND LENGTH(client_city) > 0
  AND LENGTH(client_country) > 0
  AND LENGTH(client_region) > 0
  AND LENGTH(client_asn_name) > 0
  AND LENGTH(server_asn_name) > 0
  ORDER BY random
  LIMIT 1000
),
(
  SELECT
  DATE(local_test_date) AS date,
  local_test_date,
  REPLACE(LOWER(CONCAT(IFNULL(client_continent_code, ""), IFNULL(client_country_code, ""), IFNULL(client_region_code, ""), IFNULL(client_city, ""), "")), " ", "") AS client_location_key,
  client_city,
  client_region,
  client_country,
  client_continent_code,
  client_asn_name,
  server_asn_name,
  client_latitude,
  client_longitude,
  server_latitude,
  server_longitude,
  download_speed_mbps,
  upload_speed_mbps,
  rand() as random
  FROM {0}
  WHERE client_continent_code = "AF"
  AND local_test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
  AND download_speed_mbps > 0
  AND upload_speed_mbps > 0
  AND client_latitude != 0.0
  AND client_longitude != 0.0
  AND server_latitude != 0.0
  AND server_longitude != 0.0
  AND LENGTH(client_city) > 0
  AND LENGTH(client_country) > 0
  AND LENGTH(client_region) > 0
  AND LENGTH(client_asn_name) > 0
  AND LENGTH(server_asn_name) > 0
  ORDER BY random
  LIMIT 1000
)
