#standardSQL

-- Finds various metrics grouped by remote_ip (client) based on
-- passed in timestamps.
--
-- Note this assumes that IPs are masked with 0xffffffc0
--
-- Includes:
--   * sample count
--   * download speed
--   * rtt
--   * packet retransmission rate
--
-- {0} = start timestamp (>=)
-- {1} = end timestamp (<)

SELECT

  -- Row index for batching
  -- ROW_NUMBER() OVER() AS row_idx,

  -- Counts
  count(*) AS download_test_count,

  -- General Information
  FORMAT_TIMESTAMP("%F %X", TIMESTAMP_TRUNC(TIMESTAMP_MICROS(web100_log_entry.log_time * 1000000), DAY, "UTC")) as test_date,
  _PARTITIONTIME as partition_date,

 -- Client Information
  web100_log_entry.connection_spec.remote_ip AS client_ip,
  TO_BASE64(NET.IP_FROM_STRING(web100_log_entry.connection_spec.remote_ip)) as client_ip_base64,
  -- IP Family: 1 = IPv6, 0 = IPv4
  (CASE
    WHEN REGEXP_CONTAINS(web100_log_entry.connection_spec.remote_ip, ":") THEN 1
    ELSE 0
  END) as client_ip_family,
  connection_spec.client_geolocation.city AS client_city,
  connection_spec.client_geolocation.region as client_region_code,
  connection_spec.client_geolocation.continent_code as client_continent_code,
  connection_spec.client_geolocation.country_code AS client_country_code,
  connection_spec.client_geolocation.latitude AS client_latitude,
  connection_spec.client_geolocation.longitude AS client_longitude,

  -- Server Information
  web100_log_entry.connection_spec.local_ip AS server_ip,
  TO_BASE64(NET.IP_FROM_STRING(web100_log_entry.connection_spec.local_ip)) as server_ip_base64,
  -- IP Family: 1 = IPv6, 0 = IPv4
  (CASE
    WHEN REGEXP_CONTAINS(web100_log_entry.connection_spec.local_ip, ":") THEN 1
    ELSE 0
  END) as server_ip_family,
  connection_spec.server_geolocation.city AS server_city,
  connection_spec.server_geolocation.region AS server_region_code,
  connection_spec.server_geolocation.continent_code as server_continent_code,
  connection_spec.server_geolocation.country_code AS server_country_code,
  connection_spec.server_geolocation.latitude AS server_latitude,
  connection_spec.server_geolocation.longitude AS server_longitude,

  -- Throughput
  APPROX_QUANTILES(8 * (web100_log_entry.snap.HCThruOctetsAcked /
       (web100_log_entry.snap.SndLimTimeRwin +
        web100_log_entry.snap.SndLimTimeCwnd +
        web100_log_entry.snap.SndLimTimeSnd)), 101)[SAFE_ORDINAL(51)] AS download_speed_mbps,


  -- Round-trip time
  sum(web100_log_entry.snap.SumRTT) AS rtt_sum,
  sum(web100_log_entry.snap.CountRTT) AS rtt_count,
  min(web100_log_entry.snap.MinRTT) AS rtt_min,
  sum(web100_log_entry.snap.SumRTT) / sum(web100_log_entry.snap.CountRTT) AS rtt_avg,
  APPROX_QUANTILES(web100_log_entry.snap.SumRTT / web100_log_entry.snap.CountRTT, 101)[SAFE_ORDINAL(51)] AS rtt_median,

  -- Packet retransmission rate
  sum(web100_log_entry.snap.SegsRetrans) AS segs_retrans,
  sum(web100_log_entry.snap.DataSegsOut) AS segs_out,
  sum(web100_log_entry.snap.SegsRetrans) / sum(web100_log_entry.snap.DataSegsOut) AS packet_retransmit_rate,
  APPROX_QUANTILES(web100_log_entry.snap.SegsRetrans / web100_log_entry.snap.DataSegsOut, 101)[SAFE_ORDINAL(51)] AS packet_retransmit_rate_median,


  -- Network-limited time ratio =
  SUM(web100_log_entry.snap.SndLimTimeCwnd) /
    SUM(web100_log_entry.snap.SndLimTimeRwin +
        web100_log_entry.snap.SndLimTimeCwnd +
        web100_log_entry.snap.SndLimTimeSnd) as network_limited_time_ratio,

  -- Client-limited time ratio =
  SUM(web100_log_entry.snap.SndLimTimeRwin) /
    SUM(web100_log_entry.snap.SndLimTimeRwin +
        web100_log_entry.snap.SndLimTimeCwnd +
        web100_log_entry.snap.SndLimTimeSnd) as client_limited_time_ratio,

  -- limited time sums to keep around for aggregated use
  SUM(web100_log_entry.snap.SndLimTimeRwin) as sum_lim_time_rwin,
  SUM(web100_log_entry.snap.SndLimTimeCwnd) as sum_lim_time_cwnd,
  SUM(web100_log_entry.snap.SndLimTimeSnd) as sum_lim_time_snd
FROM
  {2}
WHERE
  -- Limit to within a time region
  TIMESTAMP_TRUNC(TIMESTAMP_MICROS(web100_log_entry.log_time * 1000000), SECOND, "UTC") >= PARSE_TIMESTAMP("%F %X", "{0}")
  AND TIMESTAMP_TRUNC(TIMESTAMP_MICROS(web100_log_entry.log_time * 1000000), SECOND, "UTC") < PARSE_TIMESTAMP("%F %X", "{1}")
  AND connection_spec.data_direction = 1

  AND web100_log_entry.snap.SndLimTimeSnd IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeCwnd IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeRwin IS NOT NULL
  AND web100_log_entry.snap.CongSignals > 0
  AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
  AND (web100_log_entry.snap.State = 1
   OR (web100_log_entry.snap.State >= 5
       AND web100_log_entry.snap.State <= 11))
  AND (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +
      web100_log_entry.snap.SndLimTimeSnd) >= 9000000
  AND (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +
      web100_log_entry.snap.SndLimTimeSnd) < 600000000
  AND web100_log_entry.snap.CountRTT > 10

 -- Do not include rows with none or null values in client annotation fields:
 --     city, region_code, continent_code, country_code, latitude, longitude
  AND (connection_spec.client_geolocation.city IS NOT NULL OR LENGTH(connection_spec.client_geolocation.city) > 0)
  AND (connection_spec.client_geolocation.region IS NOT NULL OR LENGTH(connection_spec.client_geolocation.region) > 0)
  AND (connection_spec.client_geolocation.continent_code IS NOT NULL OR LENGTH(connection_spec.client_geolocation.continent_code) > 0)
  AND (connection_spec.client_geolocation.country_code IS NOT NULL OR LENGTH(connection_spec.client_geolocation.country_code) > 0)
  AND connection_spec.client_geolocation.latitude IS NOT NULL
  AND connection_spec.client_geolocation.longitude IS NOT NULL
 
GROUP BY
  test_date,
  partition_date,
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
