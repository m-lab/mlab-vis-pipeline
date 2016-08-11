left join
(
  SELECT
    count(*) as test_count,

    -- which location fields?
    {0},

    -- measurements:
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

    -- bins:
    {5}

  from {{0}}
  where
    test_date >= {1} and
    {2} is not null
  group by
    -- group by location fields
    {0}

) {4} on
  -- join on location fields from the all table.
  {3}
