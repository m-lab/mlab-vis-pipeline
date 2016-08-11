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
  USEC_TO_TIMESTAMP(UTC_USEC_TO_DAY(web100_log_entry.log_time * INTEGER(POW(10, 6)))) AS test_date,

  -- Client Information
  web100_log_entry.connection_spec.remote_ip AS client_ip,
  TO_BASE64(PARSE_PACKED_IP(web100_log_entry.connection_spec.remote_ip)) as client_ip_base64,
  -- IP Family: 10 = IPv6, 2 = IPv4
  case when PARSE_IP(web100_log_entry.connection_spec.remote_ip) is null then 10 else 2 end as client_ip_family,
  connection_spec.client_geolocation.city AS client_city,
  connection_spec.client_geolocation.region as client_region_code,
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


  -- Throughput
  nth(51, quantiles(8 * (web100_log_entry.snap.HCThruOctetsAcked /
       (web100_log_entry.snap.SndLimTimeRwin +
        web100_log_entry.snap.SndLimTimeCwnd +
        web100_log_entry.snap.SndLimTimeSnd)), 101)) AS download_speed_mbps,


  -- Round-trip time
  sum(web100_log_entry.snap.SumRTT) AS rtt_sum,
  sum(web100_log_entry.snap.CountRTT) AS rtt_count,
  min(web100_log_entry.snap.MinRTT) AS rtt_min,
  sum(web100_log_entry.snap.SumRTT) / sum(web100_log_entry.snap.CountRTT) AS rtt_avg,
  nth(51, quantiles(web100_log_entry.snap.SumRTT / web100_log_entry.snap.CountRTT, 101)) AS rtt_median,

  -- Packet retransmission rate
  sum(web100_log_entry.snap.SegsRetrans) AS segs_retrans,
  sum(web100_log_entry.snap.DataSegsOut) AS segs_out,
  sum(web100_log_entry.snap.SegsRetrans) / sum(web100_log_entry.snap.DataSegsOut) AS packet_retransmit_rate,
  nth(51, quantiles(web100_log_entry.snap.SegsRetrans / web100_log_entry.snap.DataSegsOut, 101)) AS packet_retransmit_rate_median,


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
  [plx.google:m_lab.ndt.all]
WHERE
  -- Limit to within a time region
  USEC_TO_TIMESTAMP(UTC_USEC_TO_DAY(web100_log_entry.log_time * INTEGER(POW(10, 6)))) >= "{0}"
  AND USEC_TO_TIMESTAMP(UTC_USEC_TO_DAY(web100_log_entry.log_time * INTEGER(POW(10, 6)))) < "{1}"

  AND web100_log_entry.snap.SndLimTimeSnd IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeCwnd IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeRwin IS NOT NULL
  AND project = 0
  AND web100_log_entry.is_last_entry = True
  AND connection_spec.data_direction = 1
  AND web100_log_entry.snap.CongSignals > 0
  AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
  AND (web100_log_entry.snap.State == 1
   OR (web100_log_entry.snap.State >= 5
       AND web100_log_entry.snap.State <= 11))
  AND (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +
      web100_log_entry.snap.SndLimTimeSnd) >= 9000000
  AND (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +
      web100_log_entry.snap.SndLimTimeSnd) < 3600000000
  AND web100_log_entry.snap.CountRTT > 10
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
