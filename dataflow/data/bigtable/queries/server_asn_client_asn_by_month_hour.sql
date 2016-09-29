SELECT server_asn_number,
       client_asn_number,
       server_asn_name,
       client_asn_name,
       STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([local_test_date]), "%Y-%m") as date,
       STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC([local_test_date]), "%H") as hour,
       SUM(rtt_sum) / SUM(rtt_count) AS rtt_avg,
       AVG(packet_retransmit_rate) AS retransmit_avg,
       nth(51, quantiles(download_speed_mbps, 101)) AS download_speed_mbps_median,
       nth(51, quantiles(upload_speed_mbps, 101)) AS upload_speed_mbps_median,
       COUNT(*) AS count
FROM {0}
WHERE LENGTH(server_asn_name) > 0
 AND LENGTH(client_asn_name) > 0
 AND LENGTH(server_asn_number) > 0
 AND LENGTH(client_asn_number) > 0
GROUP BY server_asn_number,
         client_asn_number,
         server_asn_name,
         client_asn_name,
         [date],
         [hour]