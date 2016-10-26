-- Finds upload median grouped by day & hour and client_ip based on
-- passed in timestamps.
--
-- Note this assumes that IPs are masked with 0xffffffc0
--
-- Includes:
--   * upload median
--
-- {0} = start timestamp (>=)
-- {1} = end timestamp (<)

SELECT

  -- Counts
  count(*) AS upload_test_count,

  -- General Information
  USEC_TO_TIMESTAMP(UTC_USEC_TO_HOUR(web100_log_entry.log_time * INTEGER(POW(10, 6)))) AS test_date,

 -- Client Information
  web100_log_entry.connection_spec.remote_ip AS client_ip,
  TO_BASE64(PARSE_PACKED_IP(web100_log_entry.connection_spec.remote_ip)) as client_ip_base64,
  -- IP Family: 10 = IPv6, 2 = IPv4
  case when PARSE_IP(web100_log_entry.connection_spec.remote_ip) is null then 10 else 2 end as client_ip_family,
  connection_spec.client_geolocation.city AS client_city,
  connection_spec.client_geolocation.region AS client_region_code,
  connection_spec.client_geolocation.continent_code as client_continent_code,
  connection_spec.client_geolocation.country_code AS client_country_code,
  connection_spec.client_geolocation.latitude AS client_latitude,
  connection_spec.client_geolocation.longitude AS client_longitude,

  -- Server Information
  web100_log_entry.connection_spec.local_ip AS server_ip,
  TO_BASE64(PARSE_PACKED_IP(web100_log_entry.connection_spec.local_ip)) as server_ip_base64,
  -- IP Family: 10 = IPv6, 2 = IPv4
  case when PARSE_IP(web100_log_entry.connection_spec.local_ip) is null then 10 else 2 end as server_ip_family,
  connection_spec.server_geolocation.city AS server_city,
  connection_spec.server_geolocation.region AS server_region_code,
  connection_spec.server_geolocation.continent_code as server_continent_code,
  connection_spec.server_geolocation.country_code AS server_country_code,
  connection_spec.server_geolocation.latitude AS server_latitude,
  connection_spec.server_geolocation.longitude AS server_longitude,

  nth(51, quantiles(8 * (web100_log_entry.snap.HCThruOctetsReceived /
         web100_log_entry.snap.Duration), 101)) AS upload_speed_mbps
FROM
  [plx.google:m_lab.ndt.all]
WHERE
  -- Limit to within a time region
  USEC_TO_TIMESTAMP(UTC_USEC_TO_HOUR(web100_log_entry.log_time * INTEGER(POW(10, 6)))) >= "{0}"
  AND USEC_TO_TIMESTAMP(UTC_USEC_TO_HOUR(web100_log_entry.log_time * INTEGER(POW(10, 6)))) < "{1}"

  AND web100_log_entry.snap.Duration IS NOT NULL
  AND connection_spec.data_direction IS NOT NULL
  AND project = 0
  AND blacklist_flags = 0
  AND connection_spec.data_direction = 0
  AND web100_log_entry.snap.HCThruOctetsReceived >= 8192
  AND (web100_log_entry.snap.State == 1
    OR (web100_log_entry.snap.State >= 5
        AND web100_log_entry.snap.State <= 11))
  AND web100_log_entry.snap.Duration >= 9000000
  AND web100_log_entry.snap.Duration < 3600000000

GROUP BY
  test_date,
  -- Client Information
  client_ip,
  client_ip_base64,
  client_ip_family,
  client_city,
  client_region_code,
  client_continent_code,
  client_country_code,
  client_latitude,
  client_longitude,

  -- Server Information
  server_ip,
  server_ip_base64,
  server_ip_family,
  server_city,
  server_region_code,
  server_continent_code,
  server_country_code,
  server_latitude,
  server_longitude
