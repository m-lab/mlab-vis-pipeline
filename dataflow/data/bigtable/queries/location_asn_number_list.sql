SELECT

  -- key:
  location_id,
  client_asn_number,

  -- metadata:
  client_asn_name,
  type,

  client_city,
  client_region,
  client_region_code,
  client_country,
  client_country_code,
  client_continent,
  client_continent_code,

  -- data: last_week
  last_week_test_count,
  last_week_download_speed_mbps_median,
  last_week_upload_speed_mbps_median,
  last_week_download_speed_mbps_avg,
  last_week_upload_speed_mbps_avg,
  last_week_download_speed_mbps_min,
  last_week_download_speed_mbps_max,
  last_week_upload_speed_mbps_min,
  last_week_upload_speed_mbps_max,
  last_week_download_speed_mbps_stddev,
  last_week_upload_speed_mbps_stddev,

  -- data: last month
  last_month_test_count,
  last_month_download_speed_mbps_median,
  last_month_upload_speed_mbps_median,
  last_month_download_speed_mbps_avg,
  last_month_upload_speed_mbps_avg,
  last_month_download_speed_mbps_min,
  last_month_download_speed_mbps_max,
  last_month_upload_speed_mbps_min,
  last_month_upload_speed_mbps_max,
  last_month_download_speed_mbps_stddev,
  last_month_upload_speed_mbps_stddev,

  -- data: last year
  last_year_test_count,
  last_year_download_speed_mbps_median,
  last_year_upload_speed_mbps_median,
  last_year_download_speed_mbps_avg,
  last_year_upload_speed_mbps_avg,
  last_year_download_speed_mbps_min,
  last_year_download_speed_mbps_max,
  last_year_upload_speed_mbps_min,
  last_year_upload_speed_mbps_max,
  last_year_download_speed_mbps_stddev,
  last_year_upload_speed_mbps_stddev,

  -- download bins
    "0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64,68,72,76,80,84,88,92,96,100" as speed_mbps_bins,

  -- last week bins
  concat(
    last_week_download_speed_mbps_B4_8, ",",
    last_week_download_speed_mbps_B8_12, ",",
    last_week_download_speed_mbps_B12_16, ",",
    last_week_download_speed_mbps_B16_20, ",",
    last_week_download_speed_mbps_B20_24, ",",
    last_week_download_speed_mbps_B24_28, ",",
    last_week_download_speed_mbps_B28_32, ",",
    last_week_download_speed_mbps_B32_36, ",",
    last_week_download_speed_mbps_B36_40, ",",
    last_week_download_speed_mbps_B40_44, ",",
    last_week_download_speed_mbps_B44_48, ",",
    last_week_download_speed_mbps_B48_52, ",",
    last_week_download_speed_mbps_B52_56, ",",
    last_week_download_speed_mbps_B56_60, ",",
    last_week_download_speed_mbps_B60_64, ",",
    last_week_download_speed_mbps_B64_68, ",",
    last_week_download_speed_mbps_B68_72, ",",
    last_week_download_speed_mbps_B72_76, ",",
    last_week_download_speed_mbps_B76_80, ",",
    last_week_download_speed_mbps_B80_84, ",",
    last_week_download_speed_mbps_B84_88, ",",
    last_week_download_speed_mbps_B88_92, ",",
    last_week_download_speed_mbps_B92_96, ",",
    last_week_download_speed_mbps_B96_100, ",",
    last_week_download_speed_mbps_BGT100) as last_week_download_speed_mbps_bin_values,

  concat(
    last_week_upload_speed_mbps_B4_8, ",",
    last_week_upload_speed_mbps_B8_12, ",",
    last_week_upload_speed_mbps_B12_16, ",",
    last_week_upload_speed_mbps_B16_20, ",",
    last_week_upload_speed_mbps_B20_24, ",",
    last_week_upload_speed_mbps_B24_28, ",",
    last_week_upload_speed_mbps_B28_32, ",",
    last_week_upload_speed_mbps_B32_36, ",",
    last_week_upload_speed_mbps_B36_40, ",",
    last_week_upload_speed_mbps_B40_44, ",",
    last_week_upload_speed_mbps_B44_48, ",",
    last_week_upload_speed_mbps_B48_52, ",",
    last_week_upload_speed_mbps_B52_56, ",",
    last_week_upload_speed_mbps_B56_60, ",",
    last_week_upload_speed_mbps_B60_64, ",",
    last_week_upload_speed_mbps_B64_68, ",",
    last_week_upload_speed_mbps_B68_72, ",",
    last_week_upload_speed_mbps_B72_76, ",",
    last_week_upload_speed_mbps_B76_80, ",",
    last_week_upload_speed_mbps_B80_84, ",",
    last_week_upload_speed_mbps_B84_88, ",",
    last_week_upload_speed_mbps_B88_92, ",",
    last_week_upload_speed_mbps_B92_96, ",",
    last_week_upload_speed_mbps_B96_100, ",",
    last_week_upload_speed_mbps_BGT100) as last_week_upload_speed_mbps_bin_values,

  -- last month bins
  concat(
    last_month_download_speed_mbps_B4_8, ",",
    last_month_download_speed_mbps_B8_12, ",",
    last_month_download_speed_mbps_B12_16, ",",
    last_month_download_speed_mbps_B16_20, ",",
    last_month_download_speed_mbps_B20_24, ",",
    last_month_download_speed_mbps_B24_28, ",",
    last_month_download_speed_mbps_B28_32, ",",
    last_month_download_speed_mbps_B32_36, ",",
    last_month_download_speed_mbps_B36_40, ",",
    last_month_download_speed_mbps_B40_44, ",",
    last_month_download_speed_mbps_B44_48, ",",
    last_month_download_speed_mbps_B48_52, ",",
    last_month_download_speed_mbps_B52_56, ",",
    last_month_download_speed_mbps_B56_60, ",",
    last_month_download_speed_mbps_B60_64, ",",
    last_month_download_speed_mbps_B64_68, ",",
    last_month_download_speed_mbps_B68_72, ",",
    last_month_download_speed_mbps_B72_76, ",",
    last_month_download_speed_mbps_B76_80, ",",
    last_month_download_speed_mbps_B80_84, ",",
    last_month_download_speed_mbps_B84_88, ",",
    last_month_download_speed_mbps_B88_92, ",",
    last_month_download_speed_mbps_B92_96, ",",
    last_month_download_speed_mbps_B96_100, ",",
    last_month_download_speed_mbps_BGT100) as last_month_download_speed_mbps_bin_values,

  concat(
    last_month_upload_speed_mbps_B4_8, ",",
    last_month_upload_speed_mbps_B8_12, ",",
    last_month_upload_speed_mbps_B12_16, ",",
    last_month_upload_speed_mbps_B16_20, ",",
    last_month_upload_speed_mbps_B20_24, ",",
    last_month_upload_speed_mbps_B24_28, ",",
    last_month_upload_speed_mbps_B28_32, ",",
    last_month_upload_speed_mbps_B32_36, ",",
    last_month_upload_speed_mbps_B36_40, ",",
    last_month_upload_speed_mbps_B40_44, ",",
    last_month_upload_speed_mbps_B44_48, ",",
    last_month_upload_speed_mbps_B48_52, ",",
    last_month_upload_speed_mbps_B52_56, ",",
    last_month_upload_speed_mbps_B56_60, ",",
    last_month_upload_speed_mbps_B60_64, ",",
    last_month_upload_speed_mbps_B64_68, ",",
    last_month_upload_speed_mbps_B68_72, ",",
    last_month_upload_speed_mbps_B72_76, ",",
    last_month_upload_speed_mbps_B76_80, ",",
    last_month_upload_speed_mbps_B80_84, ",",
    last_month_upload_speed_mbps_B84_88, ",",
    last_month_upload_speed_mbps_B88_92, ",",
    last_month_upload_speed_mbps_B92_96, ",",
    last_month_upload_speed_mbps_B96_100, ",",
    last_month_upload_speed_mbps_BGT100) as last_month_upload_speed_mbps_bin_values,


  -- last year bins
  concat(
    last_year_download_speed_mbps_B4_8, ",",
    last_year_download_speed_mbps_B8_12, ",",
    last_year_download_speed_mbps_B12_16, ",",
    last_year_download_speed_mbps_B16_20, ",",
    last_year_download_speed_mbps_B20_24, ",",
    last_year_download_speed_mbps_B24_28, ",",
    last_year_download_speed_mbps_B28_32, ",",
    last_year_download_speed_mbps_B32_36, ",",
    last_year_download_speed_mbps_B36_40, ",",
    last_year_download_speed_mbps_B40_44, ",",
    last_year_download_speed_mbps_B44_48, ",",
    last_year_download_speed_mbps_B48_52, ",",
    last_year_download_speed_mbps_B52_56, ",",
    last_year_download_speed_mbps_B56_60, ",",
    last_year_download_speed_mbps_B60_64, ",",
    last_year_download_speed_mbps_B64_68, ",",
    last_year_download_speed_mbps_B68_72, ",",
    last_year_download_speed_mbps_B72_76, ",",
    last_year_download_speed_mbps_B76_80, ",",
    last_year_download_speed_mbps_B80_84, ",",
    last_year_download_speed_mbps_B84_88, ",",
    last_year_download_speed_mbps_B88_92, ",",
    last_year_download_speed_mbps_B92_96, ",",
    last_year_download_speed_mbps_B96_100, ",",
    last_year_download_speed_mbps_BGT100) as last_year_download_speed_mbps_bin_values,

  concat(
    last_year_upload_speed_mbps_B4_8, ",",
    last_year_upload_speed_mbps_B8_12, ",",
    last_year_upload_speed_mbps_B12_16, ",",
    last_year_upload_speed_mbps_B16_20, ",",
    last_year_upload_speed_mbps_B20_24, ",",
    last_year_upload_speed_mbps_B24_28, ",",
    last_year_upload_speed_mbps_B28_32, ",",
    last_year_upload_speed_mbps_B32_36, ",",
    last_year_upload_speed_mbps_B36_40, ",",
    last_year_upload_speed_mbps_B40_44, ",",
    last_year_upload_speed_mbps_B44_48, ",",
    last_year_upload_speed_mbps_B48_52, ",",
    last_year_upload_speed_mbps_B52_56, ",",
    last_year_upload_speed_mbps_B56_60, ",",
    last_year_upload_speed_mbps_B60_64, ",",
    last_year_upload_speed_mbps_B64_68, ",",
    last_year_upload_speed_mbps_B68_72, ",",
    last_year_upload_speed_mbps_B72_76, ",",
    last_year_upload_speed_mbps_B76_80, ",",
    last_year_upload_speed_mbps_B80_84, ",",
    last_year_upload_speed_mbps_B84_88, ",",
    last_year_upload_speed_mbps_B88_92, ",",
    last_year_upload_speed_mbps_B92_96, ",",
    last_year_upload_speed_mbps_B96_100, ",",
    last_year_upload_speed_mbps_BGT100) as last_year_upload_speed_mbps_bin_values,

from

-- ============
-- Cities
-- ============
(SELECT

  REPLACE(LOWER(CONCAT(
    IFNULL(all.client_continent_code, ""),
    IFNULL(all.client_country_code, ""), "",
    IFNULL(all.client_region_code, ""), "",
    IFNULL(all.client_city, ""), ""
  )), " ", "") as location_id,

  all.client_asn_number as client_asn_number,
  all.client_asn_name as client_asn_name,

  all.client_city as client_city,
  all.client_region as client_region,
  all.client_country as client_country,
  all.client_continent as client_continent,

  all.client_region_code as client_region_code,
  all.client_country_code as client_country_code,
  all.client_continent_code as client_continent_code,

  "city" as type,

  -- last week measurements
  lastweek.last_week_test_count as last_week_test_count,
  lastweek.download_speed_mbps_median as last_week_download_speed_mbps_median,
  lastweek.upload_speed_mbps_median as last_week_upload_speed_mbps_median,
  lastweek.download_speed_mbps_avg as last_week_download_speed_mbps_avg,
  lastweek.upload_speed_mbps_avg as last_week_upload_speed_mbps_avg,
  lastweek.download_speed_mbps_min as last_week_download_speed_mbps_min,
  lastweek.download_speed_mbps_max as last_week_download_speed_mbps_max,
  lastweek.upload_speed_mbps_min as last_week_upload_speed_mbps_min,
  lastweek.upload_speed_mbps_max as last_week_upload_speed_mbps_max,
  lastweek.download_speed_mbps_stddev as last_week_download_speed_mbps_stddev,
  lastweek.upload_speed_mbps_stddev as last_week_upload_speed_mbps_stddev,

  lastweek.download_speed_mbps_B4_8 as last_week_download_speed_mbps_B4_8,
  lastweek.download_speed_mbps_B8_12 as last_week_download_speed_mbps_B8_12,
  lastweek.download_speed_mbps_B12_16 as last_week_download_speed_mbps_B12_16,
  lastweek.download_speed_mbps_B16_20 as last_week_download_speed_mbps_B16_20,
  lastweek.download_speed_mbps_B20_24 as last_week_download_speed_mbps_B20_24,
  lastweek.download_speed_mbps_B24_28 as last_week_download_speed_mbps_B24_28,
  lastweek.download_speed_mbps_B28_32 as last_week_download_speed_mbps_B28_32,
  lastweek.download_speed_mbps_B32_36 as last_week_download_speed_mbps_B32_36,
  lastweek.download_speed_mbps_B36_40 as last_week_download_speed_mbps_B36_40,
  lastweek.download_speed_mbps_B40_44 as last_week_download_speed_mbps_B40_44,
  lastweek.download_speed_mbps_B44_48 as last_week_download_speed_mbps_B44_48,
  lastweek.download_speed_mbps_B48_52 as last_week_download_speed_mbps_B48_52,
  lastweek.download_speed_mbps_B52_56 as last_week_download_speed_mbps_B52_56,
  lastweek.download_speed_mbps_B56_60 as last_week_download_speed_mbps_B56_60,
  lastweek.download_speed_mbps_B60_64 as last_week_download_speed_mbps_B60_64,
  lastweek.download_speed_mbps_B64_68 as last_week_download_speed_mbps_B64_68,
  lastweek.download_speed_mbps_B68_72 as last_week_download_speed_mbps_B68_72,
  lastweek.download_speed_mbps_B72_76 as last_week_download_speed_mbps_B72_76,
  lastweek.download_speed_mbps_B76_80 as last_week_download_speed_mbps_B76_80,
  lastweek.download_speed_mbps_B80_84 as last_week_download_speed_mbps_B80_84,
  lastweek.download_speed_mbps_B84_88 as last_week_download_speed_mbps_B84_88,
  lastweek.download_speed_mbps_B88_92 as last_week_download_speed_mbps_B88_92,
  lastweek.download_speed_mbps_B92_96 as last_week_download_speed_mbps_B92_96,
  lastweek.download_speed_mbps_B96_100 as last_week_download_speed_mbps_B96_100,
  lastweek.download_speed_mbps_BGT100 as last_week_download_speed_mbps_BGT100,

  lastweek.upload_speed_mbps_B4_8 as last_week_upload_speed_mbps_B4_8,
  lastweek.upload_speed_mbps_B8_12 as last_week_upload_speed_mbps_B8_12,
  lastweek.upload_speed_mbps_B12_16 as last_week_upload_speed_mbps_B12_16,
  lastweek.upload_speed_mbps_B16_20 as last_week_upload_speed_mbps_B16_20,
  lastweek.upload_speed_mbps_B20_24 as last_week_upload_speed_mbps_B20_24,
  lastweek.upload_speed_mbps_B24_28 as last_week_upload_speed_mbps_B24_28,
  lastweek.upload_speed_mbps_B28_32 as last_week_upload_speed_mbps_B28_32,
  lastweek.upload_speed_mbps_B32_36 as last_week_upload_speed_mbps_B32_36,
  lastweek.upload_speed_mbps_B36_40 as last_week_upload_speed_mbps_B36_40,
  lastweek.upload_speed_mbps_B40_44 as last_week_upload_speed_mbps_B40_44,
  lastweek.upload_speed_mbps_B44_48 as last_week_upload_speed_mbps_B44_48,
  lastweek.upload_speed_mbps_B48_52 as last_week_upload_speed_mbps_B48_52,
  lastweek.upload_speed_mbps_B52_56 as last_week_upload_speed_mbps_B52_56,
  lastweek.upload_speed_mbps_B56_60 as last_week_upload_speed_mbps_B56_60,
  lastweek.upload_speed_mbps_B60_64 as last_week_upload_speed_mbps_B60_64,
  lastweek.upload_speed_mbps_B64_68 as last_week_upload_speed_mbps_B64_68,
  lastweek.upload_speed_mbps_B68_72 as last_week_upload_speed_mbps_B68_72,
  lastweek.upload_speed_mbps_B72_76 as last_week_upload_speed_mbps_B72_76,
  lastweek.upload_speed_mbps_B76_80 as last_week_upload_speed_mbps_B76_80,
  lastweek.upload_speed_mbps_B80_84 as last_week_upload_speed_mbps_B80_84,
  lastweek.upload_speed_mbps_B84_88 as last_week_upload_speed_mbps_B84_88,
  lastweek.upload_speed_mbps_B88_92 as last_week_upload_speed_mbps_B88_92,
  lastweek.upload_speed_mbps_B92_96 as last_week_upload_speed_mbps_B92_96,
  lastweek.upload_speed_mbps_B96_100 as last_week_upload_speed_mbps_B96_100,
  lastweek.upload_speed_mbps_BGT100 as last_week_upload_speed_mbps_BGT100,

  -- last month measurements
  lastmonth.last_month_test_count as last_month_test_count,
  lastmonth.download_speed_mbps_median as last_month_download_speed_mbps_median,
  lastmonth.upload_speed_mbps_median as last_month_upload_speed_mbps_median,
  lastmonth.download_speed_mbps_avg as last_month_download_speed_mbps_avg,
  lastmonth.upload_speed_mbps_avg as last_month_upload_speed_mbps_avg,
  lastmonth.download_speed_mbps_min as last_month_download_speed_mbps_min,
  lastmonth.download_speed_mbps_max as last_month_download_speed_mbps_max,
  lastmonth.upload_speed_mbps_min as last_month_upload_speed_mbps_min,
  lastmonth.upload_speed_mbps_max as last_month_upload_speed_mbps_max,
  lastmonth.download_speed_mbps_stddev as last_month_download_speed_mbps_stddev,
  lastmonth.upload_speed_mbps_stddev as last_month_upload_speed_mbps_stddev,

  lastmonth.download_speed_mbps_B4_8 as last_month_download_speed_mbps_B4_8,
  lastmonth.download_speed_mbps_B8_12 as last_month_download_speed_mbps_B8_12,
  lastmonth.download_speed_mbps_B12_16 as last_month_download_speed_mbps_B12_16,
  lastmonth.download_speed_mbps_B16_20 as last_month_download_speed_mbps_B16_20,
  lastmonth.download_speed_mbps_B20_24 as last_month_download_speed_mbps_B20_24,
  lastmonth.download_speed_mbps_B24_28 as last_month_download_speed_mbps_B24_28,
  lastmonth.download_speed_mbps_B28_32 as last_month_download_speed_mbps_B28_32,
  lastmonth.download_speed_mbps_B32_36 as last_month_download_speed_mbps_B32_36,
  lastmonth.download_speed_mbps_B36_40 as last_month_download_speed_mbps_B36_40,
  lastmonth.download_speed_mbps_B40_44 as last_month_download_speed_mbps_B40_44,
  lastmonth.download_speed_mbps_B44_48 as last_month_download_speed_mbps_B44_48,
  lastmonth.download_speed_mbps_B48_52 as last_month_download_speed_mbps_B48_52,
  lastmonth.download_speed_mbps_B52_56 as last_month_download_speed_mbps_B52_56,
  lastmonth.download_speed_mbps_B56_60 as last_month_download_speed_mbps_B56_60,
  lastmonth.download_speed_mbps_B60_64 as last_month_download_speed_mbps_B60_64,
  lastmonth.download_speed_mbps_B64_68 as last_month_download_speed_mbps_B64_68,
  lastmonth.download_speed_mbps_B68_72 as last_month_download_speed_mbps_B68_72,
  lastmonth.download_speed_mbps_B72_76 as last_month_download_speed_mbps_B72_76,
  lastmonth.download_speed_mbps_B76_80 as last_month_download_speed_mbps_B76_80,
  lastmonth.download_speed_mbps_B80_84 as last_month_download_speed_mbps_B80_84,
  lastmonth.download_speed_mbps_B84_88 as last_month_download_speed_mbps_B84_88,
  lastmonth.download_speed_mbps_B88_92 as last_month_download_speed_mbps_B88_92,
  lastmonth.download_speed_mbps_B92_96 as last_month_download_speed_mbps_B92_96,
  lastmonth.download_speed_mbps_B96_100 as last_month_download_speed_mbps_B96_100,
  lastmonth.download_speed_mbps_BGT100 as last_month_download_speed_mbps_BGT100,

  lastmonth.upload_speed_mbps_B4_8 as last_month_upload_speed_mbps_B4_8,
  lastmonth.upload_speed_mbps_B8_12 as last_month_upload_speed_mbps_B8_12,
  lastmonth.upload_speed_mbps_B12_16 as last_month_upload_speed_mbps_B12_16,
  lastmonth.upload_speed_mbps_B16_20 as last_month_upload_speed_mbps_B16_20,
  lastmonth.upload_speed_mbps_B20_24 as last_month_upload_speed_mbps_B20_24,
  lastmonth.upload_speed_mbps_B24_28 as last_month_upload_speed_mbps_B24_28,
  lastmonth.upload_speed_mbps_B28_32 as last_month_upload_speed_mbps_B28_32,
  lastmonth.upload_speed_mbps_B32_36 as last_month_upload_speed_mbps_B32_36,
  lastmonth.upload_speed_mbps_B36_40 as last_month_upload_speed_mbps_B36_40,
  lastmonth.upload_speed_mbps_B40_44 as last_month_upload_speed_mbps_B40_44,
  lastmonth.upload_speed_mbps_B44_48 as last_month_upload_speed_mbps_B44_48,
  lastmonth.upload_speed_mbps_B48_52 as last_month_upload_speed_mbps_B48_52,
  lastmonth.upload_speed_mbps_B52_56 as last_month_upload_speed_mbps_B52_56,
  lastmonth.upload_speed_mbps_B56_60 as last_month_upload_speed_mbps_B56_60,
  lastmonth.upload_speed_mbps_B60_64 as last_month_upload_speed_mbps_B60_64,
  lastmonth.upload_speed_mbps_B64_68 as last_month_upload_speed_mbps_B64_68,
  lastmonth.upload_speed_mbps_B68_72 as last_month_upload_speed_mbps_B68_72,
  lastmonth.upload_speed_mbps_B72_76 as last_month_upload_speed_mbps_B72_76,
  lastmonth.upload_speed_mbps_B76_80 as last_month_upload_speed_mbps_B76_80,
  lastmonth.upload_speed_mbps_B80_84 as last_month_upload_speed_mbps_B80_84,
  lastmonth.upload_speed_mbps_B84_88 as last_month_upload_speed_mbps_B84_88,
  lastmonth.upload_speed_mbps_B88_92 as last_month_upload_speed_mbps_B88_92,
  lastmonth.upload_speed_mbps_B92_96 as last_month_upload_speed_mbps_B92_96,
  lastmonth.upload_speed_mbps_B96_100 as last_month_upload_speed_mbps_B96_100,
  lastmonth.upload_speed_mbps_BGT100 as last_month_upload_speed_mbps_BGT100,

  -- last year measurements
  lastyear.last_year_test_count as last_year_test_count,
  lastyear.download_speed_mbps_median as last_year_download_speed_mbps_median,
  lastyear.upload_speed_mbps_median as last_year_upload_speed_mbps_median,
  lastyear.download_speed_mbps_avg as last_year_download_speed_mbps_avg,
  lastyear.upload_speed_mbps_avg as last_year_upload_speed_mbps_avg,
  lastyear.download_speed_mbps_min as last_year_download_speed_mbps_min,
  lastyear.download_speed_mbps_max as last_year_download_speed_mbps_max,
  lastyear.upload_speed_mbps_min as last_year_upload_speed_mbps_min,
  lastyear.upload_speed_mbps_max as last_year_upload_speed_mbps_max,
  lastyear.download_speed_mbps_stddev as last_year_download_speed_mbps_stddev,
  lastyear.upload_speed_mbps_stddev as last_year_upload_speed_mbps_stddev,

  lastyear.download_speed_mbps_B4_8 as last_year_download_speed_mbps_B4_8,
  lastyear.download_speed_mbps_B8_12 as last_year_download_speed_mbps_B8_12,
  lastyear.download_speed_mbps_B12_16 as last_year_download_speed_mbps_B12_16,
  lastyear.download_speed_mbps_B16_20 as last_year_download_speed_mbps_B16_20,
  lastyear.download_speed_mbps_B20_24 as last_year_download_speed_mbps_B20_24,
  lastyear.download_speed_mbps_B24_28 as last_year_download_speed_mbps_B24_28,
  lastyear.download_speed_mbps_B28_32 as last_year_download_speed_mbps_B28_32,
  lastyear.download_speed_mbps_B32_36 as last_year_download_speed_mbps_B32_36,
  lastyear.download_speed_mbps_B36_40 as last_year_download_speed_mbps_B36_40,
  lastyear.download_speed_mbps_B40_44 as last_year_download_speed_mbps_B40_44,
  lastyear.download_speed_mbps_B44_48 as last_year_download_speed_mbps_B44_48,
  lastyear.download_speed_mbps_B48_52 as last_year_download_speed_mbps_B48_52,
  lastyear.download_speed_mbps_B52_56 as last_year_download_speed_mbps_B52_56,
  lastyear.download_speed_mbps_B56_60 as last_year_download_speed_mbps_B56_60,
  lastyear.download_speed_mbps_B60_64 as last_year_download_speed_mbps_B60_64,
  lastyear.download_speed_mbps_B64_68 as last_year_download_speed_mbps_B64_68,
  lastyear.download_speed_mbps_B68_72 as last_year_download_speed_mbps_B68_72,
  lastyear.download_speed_mbps_B72_76 as last_year_download_speed_mbps_B72_76,
  lastyear.download_speed_mbps_B76_80 as last_year_download_speed_mbps_B76_80,
  lastyear.download_speed_mbps_B80_84 as last_year_download_speed_mbps_B80_84,
  lastyear.download_speed_mbps_B84_88 as last_year_download_speed_mbps_B84_88,
  lastyear.download_speed_mbps_B88_92 as last_year_download_speed_mbps_B88_92,
  lastyear.download_speed_mbps_B92_96 as last_year_download_speed_mbps_B92_96,
  lastyear.download_speed_mbps_B96_100 as last_year_download_speed_mbps_B96_100,
  lastyear.download_speed_mbps_BGT100 as last_year_download_speed_mbps_BGT100,

  lastyear.upload_speed_mbps_B4_8 as last_year_upload_speed_mbps_B4_8,
  lastyear.upload_speed_mbps_B8_12 as last_year_upload_speed_mbps_B8_12,
  lastyear.upload_speed_mbps_B12_16 as last_year_upload_speed_mbps_B12_16,
  lastyear.upload_speed_mbps_B16_20 as last_year_upload_speed_mbps_B16_20,
  lastyear.upload_speed_mbps_B20_24 as last_year_upload_speed_mbps_B20_24,
  lastyear.upload_speed_mbps_B24_28 as last_year_upload_speed_mbps_B24_28,
  lastyear.upload_speed_mbps_B28_32 as last_year_upload_speed_mbps_B28_32,
  lastyear.upload_speed_mbps_B32_36 as last_year_upload_speed_mbps_B32_36,
  lastyear.upload_speed_mbps_B36_40 as last_year_upload_speed_mbps_B36_40,
  lastyear.upload_speed_mbps_B40_44 as last_year_upload_speed_mbps_B40_44,
  lastyear.upload_speed_mbps_B44_48 as last_year_upload_speed_mbps_B44_48,
  lastyear.upload_speed_mbps_B48_52 as last_year_upload_speed_mbps_B48_52,
  lastyear.upload_speed_mbps_B52_56 as last_year_upload_speed_mbps_B52_56,
  lastyear.upload_speed_mbps_B56_60 as last_year_upload_speed_mbps_B56_60,
  lastyear.upload_speed_mbps_B60_64 as last_year_upload_speed_mbps_B60_64,
  lastyear.upload_speed_mbps_B64_68 as last_year_upload_speed_mbps_B64_68,
  lastyear.upload_speed_mbps_B68_72 as last_year_upload_speed_mbps_B68_72,
  lastyear.upload_speed_mbps_B72_76 as last_year_upload_speed_mbps_B72_76,
  lastyear.upload_speed_mbps_B76_80 as last_year_upload_speed_mbps_B76_80,
  lastyear.upload_speed_mbps_B80_84 as last_year_upload_speed_mbps_B80_84,
  lastyear.upload_speed_mbps_B84_88 as last_year_upload_speed_mbps_B84_88,
  lastyear.upload_speed_mbps_B88_92 as last_year_upload_speed_mbps_B88_92,
  lastyear.upload_speed_mbps_B92_96 as last_year_upload_speed_mbps_B92_96,
  lastyear.upload_speed_mbps_B96_100 as last_year_upload_speed_mbps_B96_100,
  lastyear.upload_speed_mbps_BGT100 as last_year_upload_speed_mbps_BGT100,

  FROM [mlab-oti:bocoup_prod.all_ip_by_day] all,

  left join
  (
    SELECT
      count(*) as last_week_test_count,

      client_city,
      client_region,
      client_country,
      client_continent,

      client_asn_number,
      client_asn_name,

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

      -- download bins
      SUM(IF(download_speed_mbps < 4 ,1,0)) AS download_speed_mbps_B0_4,
      SUM(IF(download_speed_mbps >= 4 AND download_speed_mbps < 8, 1,0)) AS download_speed_mbps_B4_8,
      SUM(IF(download_speed_mbps >= 8 AND download_speed_mbps < 12, 1,0)) AS download_speed_mbps_B8_12,
      SUM(IF(download_speed_mbps >= 12 AND download_speed_mbps < 16, 1,0)) AS download_speed_mbps_B12_16,
      SUM(IF(download_speed_mbps >= 16 AND download_speed_mbps < 20, 1,0)) AS download_speed_mbps_B16_20,
      SUM(IF(download_speed_mbps >= 20 AND download_speed_mbps < 24, 1,0)) AS download_speed_mbps_B20_24,
      SUM(IF(download_speed_mbps >= 24 AND download_speed_mbps < 28, 1,0)) AS download_speed_mbps_B24_28,
      SUM(IF(download_speed_mbps >= 28 AND download_speed_mbps < 32, 1,0)) AS download_speed_mbps_B28_32,
      SUM(IF(download_speed_mbps >= 32 AND download_speed_mbps < 36, 1,0)) AS download_speed_mbps_B32_36,
      SUM(IF(download_speed_mbps >= 36 AND download_speed_mbps < 40, 1,0)) AS download_speed_mbps_B36_40,
      SUM(IF(download_speed_mbps >= 40 AND download_speed_mbps < 44, 1,0)) AS download_speed_mbps_B40_44,
      SUM(IF(download_speed_mbps >= 44 AND download_speed_mbps < 48, 1,0)) AS download_speed_mbps_B44_48,
      SUM(IF(download_speed_mbps >= 48 AND download_speed_mbps < 52, 1,0)) AS download_speed_mbps_B48_52,
      SUM(IF(download_speed_mbps >= 52 AND download_speed_mbps < 56, 1,0)) AS download_speed_mbps_B52_56,
      SUM(IF(download_speed_mbps >= 56 AND download_speed_mbps < 60, 1,0)) AS download_speed_mbps_B56_60,
      SUM(IF(download_speed_mbps >= 60 AND download_speed_mbps < 64, 1,0)) AS download_speed_mbps_B60_64,
      SUM(IF(download_speed_mbps >= 64 AND download_speed_mbps < 68, 1,0)) AS download_speed_mbps_B64_68,
      SUM(IF(download_speed_mbps >= 68 AND download_speed_mbps < 72, 1,0)) AS download_speed_mbps_B68_72,
      SUM(IF(download_speed_mbps >= 72 AND download_speed_mbps < 76, 1,0)) AS download_speed_mbps_B72_76,
      SUM(IF(download_speed_mbps >= 76 AND download_speed_mbps < 80, 1,0)) AS download_speed_mbps_B76_80,
      SUM(IF(download_speed_mbps >= 80 AND download_speed_mbps < 84, 1,0)) AS download_speed_mbps_B80_84,
      SUM(IF(download_speed_mbps >= 84 AND download_speed_mbps < 88, 1,0)) AS download_speed_mbps_B84_88,
      SUM(IF(download_speed_mbps >= 88 AND download_speed_mbps < 92, 1,0)) AS download_speed_mbps_B88_92,
      SUM(IF(download_speed_mbps >= 92 AND download_speed_mbps < 96, 1,0)) AS download_speed_mbps_B92_96,
      SUM(IF(download_speed_mbps >= 96 AND download_speed_mbps < 100, 1,0)) AS download_speed_mbps_B96_100,
      SUM(IF(download_speed_mbps >= 100, 1,0)) AS download_speed_mbps_BGT100,

      -- upload bins
      SUM(IF(upload_speed_mbps < 4 ,1,0)) AS upload_speed_mbps_B0_4,
      SUM(IF(upload_speed_mbps >= 4 AND upload_speed_mbps < 8, 1,0)) AS upload_speed_mbps_B4_8,
      SUM(IF(upload_speed_mbps >= 8 AND upload_speed_mbps < 12, 1,0)) AS upload_speed_mbps_B8_12,
      SUM(IF(upload_speed_mbps >= 12 AND upload_speed_mbps < 16, 1,0)) AS upload_speed_mbps_B12_16,
      SUM(IF(upload_speed_mbps >= 16 AND upload_speed_mbps < 20, 1,0)) AS upload_speed_mbps_B16_20,
      SUM(IF(upload_speed_mbps >= 20 AND upload_speed_mbps < 24, 1,0)) AS upload_speed_mbps_B20_24,
      SUM(IF(upload_speed_mbps >= 24 AND upload_speed_mbps < 28, 1,0)) AS upload_speed_mbps_B24_28,
      SUM(IF(upload_speed_mbps >= 28 AND upload_speed_mbps < 32, 1,0)) AS upload_speed_mbps_B28_32,
      SUM(IF(upload_speed_mbps >= 32 AND upload_speed_mbps < 36, 1,0)) AS upload_speed_mbps_B32_36,
      SUM(IF(upload_speed_mbps >= 36 AND upload_speed_mbps < 40, 1,0)) AS upload_speed_mbps_B36_40,
      SUM(IF(upload_speed_mbps >= 40 AND upload_speed_mbps < 44, 1,0)) AS upload_speed_mbps_B40_44,
      SUM(IF(upload_speed_mbps >= 44 AND upload_speed_mbps < 48, 1,0)) AS upload_speed_mbps_B44_48,
      SUM(IF(upload_speed_mbps >= 48 AND upload_speed_mbps < 52, 1,0)) AS upload_speed_mbps_B48_52,
      SUM(IF(upload_speed_mbps >= 52 AND upload_speed_mbps < 56, 1,0)) AS upload_speed_mbps_B52_56,
      SUM(IF(upload_speed_mbps >= 56 AND upload_speed_mbps < 60, 1,0)) AS upload_speed_mbps_B56_60,
      SUM(IF(upload_speed_mbps >= 60 AND upload_speed_mbps < 64, 1,0)) AS upload_speed_mbps_B60_64,
      SUM(IF(upload_speed_mbps >= 64 AND upload_speed_mbps < 68, 1,0)) AS upload_speed_mbps_B64_68,
      SUM(IF(upload_speed_mbps >= 68 AND upload_speed_mbps < 72, 1,0)) AS upload_speed_mbps_B68_72,
      SUM(IF(upload_speed_mbps >= 72 AND upload_speed_mbps < 76, 1,0)) AS upload_speed_mbps_B72_76,
      SUM(IF(upload_speed_mbps >= 76 AND upload_speed_mbps < 80, 1,0)) AS upload_speed_mbps_B76_80,
      SUM(IF(upload_speed_mbps >= 80 AND upload_speed_mbps < 84, 1,0)) AS upload_speed_mbps_B80_84,
      SUM(IF(upload_speed_mbps >= 84 AND upload_speed_mbps < 88, 1,0)) AS upload_speed_mbps_B84_88,
      SUM(IF(upload_speed_mbps >= 88 AND upload_speed_mbps < 92, 1,0)) AS upload_speed_mbps_B88_92,
      SUM(IF(upload_speed_mbps >= 92 AND upload_speed_mbps < 96, 1,0)) AS upload_speed_mbps_B92_96,
      SUM(IF(upload_speed_mbps >= 96 AND upload_speed_mbps < 100, 1,0)) AS upload_speed_mbps_B96_100,
      SUM(IF(upload_speed_mbps >= 100, 1,0)) AS upload_speed_mbps_BGT100,

    from [mlab-oti:bocoup_prod.all_ip_by_day]
    where
      test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, "DAY") and
      client_city is not null and
      client_asn_number is not null and
      client_asn_number is not null

    group by
      client_asn_number,
      client_asn_name,
      client_city,
      client_region,
      client_country,
      client_continent

  ) lastweek on
    all.client_city = lastweek.client_city and
    all.client_region = lastweek.client_region and
    all.client_country = lastweek.client_country and
    all.client_continent = lastweek.client_continent and
    all.client_asn_number = lastweek.client_asn_number and
    all.client_asn_name = lastweek.client_asn_name

  -- Compute metrics for the current month
  left join
  (
    SELECT
      count(*) as last_month_test_count,

      client_city,
      client_region,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name,

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

      -- download bins
      SUM(IF(download_speed_mbps < 4 ,1,0)) AS download_speed_mbps_B0_4,
      SUM(IF(download_speed_mbps >= 4 AND download_speed_mbps < 8, 1,0)) AS download_speed_mbps_B4_8,
      SUM(IF(download_speed_mbps >= 8 AND download_speed_mbps < 12, 1,0)) AS download_speed_mbps_B8_12,
      SUM(IF(download_speed_mbps >= 12 AND download_speed_mbps < 16, 1,0)) AS download_speed_mbps_B12_16,
      SUM(IF(download_speed_mbps >= 16 AND download_speed_mbps < 20, 1,0)) AS download_speed_mbps_B16_20,
      SUM(IF(download_speed_mbps >= 20 AND download_speed_mbps < 24, 1,0)) AS download_speed_mbps_B20_24,
      SUM(IF(download_speed_mbps >= 24 AND download_speed_mbps < 28, 1,0)) AS download_speed_mbps_B24_28,
      SUM(IF(download_speed_mbps >= 28 AND download_speed_mbps < 32, 1,0)) AS download_speed_mbps_B28_32,
      SUM(IF(download_speed_mbps >= 32 AND download_speed_mbps < 36, 1,0)) AS download_speed_mbps_B32_36,
      SUM(IF(download_speed_mbps >= 36 AND download_speed_mbps < 40, 1,0)) AS download_speed_mbps_B36_40,
      SUM(IF(download_speed_mbps >= 40 AND download_speed_mbps < 44, 1,0)) AS download_speed_mbps_B40_44,
      SUM(IF(download_speed_mbps >= 44 AND download_speed_mbps < 48, 1,0)) AS download_speed_mbps_B44_48,
      SUM(IF(download_speed_mbps >= 48 AND download_speed_mbps < 52, 1,0)) AS download_speed_mbps_B48_52,
      SUM(IF(download_speed_mbps >= 52 AND download_speed_mbps < 56, 1,0)) AS download_speed_mbps_B52_56,
      SUM(IF(download_speed_mbps >= 56 AND download_speed_mbps < 60, 1,0)) AS download_speed_mbps_B56_60,
      SUM(IF(download_speed_mbps >= 60 AND download_speed_mbps < 64, 1,0)) AS download_speed_mbps_B60_64,
      SUM(IF(download_speed_mbps >= 64 AND download_speed_mbps < 68, 1,0)) AS download_speed_mbps_B64_68,
      SUM(IF(download_speed_mbps >= 68 AND download_speed_mbps < 72, 1,0)) AS download_speed_mbps_B68_72,
      SUM(IF(download_speed_mbps >= 72 AND download_speed_mbps < 76, 1,0)) AS download_speed_mbps_B72_76,
      SUM(IF(download_speed_mbps >= 76 AND download_speed_mbps < 80, 1,0)) AS download_speed_mbps_B76_80,
      SUM(IF(download_speed_mbps >= 80 AND download_speed_mbps < 84, 1,0)) AS download_speed_mbps_B80_84,
      SUM(IF(download_speed_mbps >= 84 AND download_speed_mbps < 88, 1,0)) AS download_speed_mbps_B84_88,
      SUM(IF(download_speed_mbps >= 88 AND download_speed_mbps < 92, 1,0)) AS download_speed_mbps_B88_92,
      SUM(IF(download_speed_mbps >= 92 AND download_speed_mbps < 96, 1,0)) AS download_speed_mbps_B92_96,
      SUM(IF(download_speed_mbps >= 96 AND download_speed_mbps < 100, 1,0)) AS download_speed_mbps_B96_100,
      SUM(IF(download_speed_mbps >= 100, 1,0)) AS download_speed_mbps_BGT100,

      -- upload bins
      SUM(IF(upload_speed_mbps < 4 ,1,0)) AS upload_speed_mbps_B0_4,
      SUM(IF(upload_speed_mbps >= 4 AND upload_speed_mbps < 8, 1,0)) AS upload_speed_mbps_B4_8,
      SUM(IF(upload_speed_mbps >= 8 AND upload_speed_mbps < 12, 1,0)) AS upload_speed_mbps_B8_12,
      SUM(IF(upload_speed_mbps >= 12 AND upload_speed_mbps < 16, 1,0)) AS upload_speed_mbps_B12_16,
      SUM(IF(upload_speed_mbps >= 16 AND upload_speed_mbps < 20, 1,0)) AS upload_speed_mbps_B16_20,
      SUM(IF(upload_speed_mbps >= 20 AND upload_speed_mbps < 24, 1,0)) AS upload_speed_mbps_B20_24,
      SUM(IF(upload_speed_mbps >= 24 AND upload_speed_mbps < 28, 1,0)) AS upload_speed_mbps_B24_28,
      SUM(IF(upload_speed_mbps >= 28 AND upload_speed_mbps < 32, 1,0)) AS upload_speed_mbps_B28_32,
      SUM(IF(upload_speed_mbps >= 32 AND upload_speed_mbps < 36, 1,0)) AS upload_speed_mbps_B32_36,
      SUM(IF(upload_speed_mbps >= 36 AND upload_speed_mbps < 40, 1,0)) AS upload_speed_mbps_B36_40,
      SUM(IF(upload_speed_mbps >= 40 AND upload_speed_mbps < 44, 1,0)) AS upload_speed_mbps_B40_44,
      SUM(IF(upload_speed_mbps >= 44 AND upload_speed_mbps < 48, 1,0)) AS upload_speed_mbps_B44_48,
      SUM(IF(upload_speed_mbps >= 48 AND upload_speed_mbps < 52, 1,0)) AS upload_speed_mbps_B48_52,
      SUM(IF(upload_speed_mbps >= 52 AND upload_speed_mbps < 56, 1,0)) AS upload_speed_mbps_B52_56,
      SUM(IF(upload_speed_mbps >= 56 AND upload_speed_mbps < 60, 1,0)) AS upload_speed_mbps_B56_60,
      SUM(IF(upload_speed_mbps >= 60 AND upload_speed_mbps < 64, 1,0)) AS upload_speed_mbps_B60_64,
      SUM(IF(upload_speed_mbps >= 64 AND upload_speed_mbps < 68, 1,0)) AS upload_speed_mbps_B64_68,
      SUM(IF(upload_speed_mbps >= 68 AND upload_speed_mbps < 72, 1,0)) AS upload_speed_mbps_B68_72,
      SUM(IF(upload_speed_mbps >= 72 AND upload_speed_mbps < 76, 1,0)) AS upload_speed_mbps_B72_76,
      SUM(IF(upload_speed_mbps >= 76 AND upload_speed_mbps < 80, 1,0)) AS upload_speed_mbps_B76_80,
      SUM(IF(upload_speed_mbps >= 80 AND upload_speed_mbps < 84, 1,0)) AS upload_speed_mbps_B80_84,
      SUM(IF(upload_speed_mbps >= 84 AND upload_speed_mbps < 88, 1,0)) AS upload_speed_mbps_B84_88,
      SUM(IF(upload_speed_mbps >= 88 AND upload_speed_mbps < 92, 1,0)) AS upload_speed_mbps_B88_92,
      SUM(IF(upload_speed_mbps >= 92 AND upload_speed_mbps < 96, 1,0)) AS upload_speed_mbps_B92_96,
      SUM(IF(upload_speed_mbps >= 96 AND upload_speed_mbps < 100, 1,0)) AS upload_speed_mbps_B96_100,
      SUM(IF(upload_speed_mbps >= 100, 1,0)) AS upload_speed_mbps_BGT100,

    from [mlab-oti:bocoup_prod.all_ip_by_day]
    -- current month:
    where
      test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH") and
      client_city is not null and
      client_asn_number is not null and
      client_asn_number is not null

    group by
      client_city,
      client_region,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name
  ) lastmonth on
    all.client_city = lastmonth.client_city and
    all.client_region = lastmonth.client_region and
    all.client_country = lastmonth.client_country and
    all.client_continent = lastmonth.client_continent and
    all.client_asn_number = lastmonth.client_asn_number and
    all.client_asn_name = lastmonth.client_asn_name

  -- Compute metrics for the current year
  left join
  (
    SELECT
      count(*) as last_year_test_count,
      client_city,
      client_region,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name,

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

      -- download bins
      SUM(IF(download_speed_mbps < 4 ,1,0)) AS download_speed_mbps_B0_4,
      SUM(IF(download_speed_mbps >= 4 AND download_speed_mbps < 8, 1,0)) AS download_speed_mbps_B4_8,
      SUM(IF(download_speed_mbps >= 8 AND download_speed_mbps < 12, 1,0)) AS download_speed_mbps_B8_12,
      SUM(IF(download_speed_mbps >= 12 AND download_speed_mbps < 16, 1,0)) AS download_speed_mbps_B12_16,
      SUM(IF(download_speed_mbps >= 16 AND download_speed_mbps < 20, 1,0)) AS download_speed_mbps_B16_20,
      SUM(IF(download_speed_mbps >= 20 AND download_speed_mbps < 24, 1,0)) AS download_speed_mbps_B20_24,
      SUM(IF(download_speed_mbps >= 24 AND download_speed_mbps < 28, 1,0)) AS download_speed_mbps_B24_28,
      SUM(IF(download_speed_mbps >= 28 AND download_speed_mbps < 32, 1,0)) AS download_speed_mbps_B28_32,
      SUM(IF(download_speed_mbps >= 32 AND download_speed_mbps < 36, 1,0)) AS download_speed_mbps_B32_36,
      SUM(IF(download_speed_mbps >= 36 AND download_speed_mbps < 40, 1,0)) AS download_speed_mbps_B36_40,
      SUM(IF(download_speed_mbps >= 40 AND download_speed_mbps < 44, 1,0)) AS download_speed_mbps_B40_44,
      SUM(IF(download_speed_mbps >= 44 AND download_speed_mbps < 48, 1,0)) AS download_speed_mbps_B44_48,
      SUM(IF(download_speed_mbps >= 48 AND download_speed_mbps < 52, 1,0)) AS download_speed_mbps_B48_52,
      SUM(IF(download_speed_mbps >= 52 AND download_speed_mbps < 56, 1,0)) AS download_speed_mbps_B52_56,
      SUM(IF(download_speed_mbps >= 56 AND download_speed_mbps < 60, 1,0)) AS download_speed_mbps_B56_60,
      SUM(IF(download_speed_mbps >= 60 AND download_speed_mbps < 64, 1,0)) AS download_speed_mbps_B60_64,
      SUM(IF(download_speed_mbps >= 64 AND download_speed_mbps < 68, 1,0)) AS download_speed_mbps_B64_68,
      SUM(IF(download_speed_mbps >= 68 AND download_speed_mbps < 72, 1,0)) AS download_speed_mbps_B68_72,
      SUM(IF(download_speed_mbps >= 72 AND download_speed_mbps < 76, 1,0)) AS download_speed_mbps_B72_76,
      SUM(IF(download_speed_mbps >= 76 AND download_speed_mbps < 80, 1,0)) AS download_speed_mbps_B76_80,
      SUM(IF(download_speed_mbps >= 80 AND download_speed_mbps < 84, 1,0)) AS download_speed_mbps_B80_84,
      SUM(IF(download_speed_mbps >= 84 AND download_speed_mbps < 88, 1,0)) AS download_speed_mbps_B84_88,
      SUM(IF(download_speed_mbps >= 88 AND download_speed_mbps < 92, 1,0)) AS download_speed_mbps_B88_92,
      SUM(IF(download_speed_mbps >= 92 AND download_speed_mbps < 96, 1,0)) AS download_speed_mbps_B92_96,
      SUM(IF(download_speed_mbps >= 96 AND download_speed_mbps < 100, 1,0)) AS download_speed_mbps_B96_100,
      SUM(IF(download_speed_mbps >= 100, 1,0)) AS download_speed_mbps_BGT100,

      -- upload bins
      SUM(IF(upload_speed_mbps < 4 ,1,0)) AS upload_speed_mbps_B0_4,
      SUM(IF(upload_speed_mbps >= 4 AND upload_speed_mbps < 8, 1,0)) AS upload_speed_mbps_B4_8,
      SUM(IF(upload_speed_mbps >= 8 AND upload_speed_mbps < 12, 1,0)) AS upload_speed_mbps_B8_12,
      SUM(IF(upload_speed_mbps >= 12 AND upload_speed_mbps < 16, 1,0)) AS upload_speed_mbps_B12_16,
      SUM(IF(upload_speed_mbps >= 16 AND upload_speed_mbps < 20, 1,0)) AS upload_speed_mbps_B16_20,
      SUM(IF(upload_speed_mbps >= 20 AND upload_speed_mbps < 24, 1,0)) AS upload_speed_mbps_B20_24,
      SUM(IF(upload_speed_mbps >= 24 AND upload_speed_mbps < 28, 1,0)) AS upload_speed_mbps_B24_28,
      SUM(IF(upload_speed_mbps >= 28 AND upload_speed_mbps < 32, 1,0)) AS upload_speed_mbps_B28_32,
      SUM(IF(upload_speed_mbps >= 32 AND upload_speed_mbps < 36, 1,0)) AS upload_speed_mbps_B32_36,
      SUM(IF(upload_speed_mbps >= 36 AND upload_speed_mbps < 40, 1,0)) AS upload_speed_mbps_B36_40,
      SUM(IF(upload_speed_mbps >= 40 AND upload_speed_mbps < 44, 1,0)) AS upload_speed_mbps_B40_44,
      SUM(IF(upload_speed_mbps >= 44 AND upload_speed_mbps < 48, 1,0)) AS upload_speed_mbps_B44_48,
      SUM(IF(upload_speed_mbps >= 48 AND upload_speed_mbps < 52, 1,0)) AS upload_speed_mbps_B48_52,
      SUM(IF(upload_speed_mbps >= 52 AND upload_speed_mbps < 56, 1,0)) AS upload_speed_mbps_B52_56,
      SUM(IF(upload_speed_mbps >= 56 AND upload_speed_mbps < 60, 1,0)) AS upload_speed_mbps_B56_60,
      SUM(IF(upload_speed_mbps >= 60 AND upload_speed_mbps < 64, 1,0)) AS upload_speed_mbps_B60_64,
      SUM(IF(upload_speed_mbps >= 64 AND upload_speed_mbps < 68, 1,0)) AS upload_speed_mbps_B64_68,
      SUM(IF(upload_speed_mbps >= 68 AND upload_speed_mbps < 72, 1,0)) AS upload_speed_mbps_B68_72,
      SUM(IF(upload_speed_mbps >= 72 AND upload_speed_mbps < 76, 1,0)) AS upload_speed_mbps_B72_76,
      SUM(IF(upload_speed_mbps >= 76 AND upload_speed_mbps < 80, 1,0)) AS upload_speed_mbps_B76_80,
      SUM(IF(upload_speed_mbps >= 80 AND upload_speed_mbps < 84, 1,0)) AS upload_speed_mbps_B80_84,
      SUM(IF(upload_speed_mbps >= 84 AND upload_speed_mbps < 88, 1,0)) AS upload_speed_mbps_B84_88,
      SUM(IF(upload_speed_mbps >= 88 AND upload_speed_mbps < 92, 1,0)) AS upload_speed_mbps_B88_92,
      SUM(IF(upload_speed_mbps >= 92 AND upload_speed_mbps < 96, 1,0)) AS upload_speed_mbps_B92_96,
      SUM(IF(upload_speed_mbps >= 96 AND upload_speed_mbps < 100, 1,0)) AS upload_speed_mbps_B96_100,
      SUM(IF(upload_speed_mbps >= 100, 1,0)) AS upload_speed_mbps_BGT100,

    from [mlab-oti:bocoup_prod.all_ip_by_day]
    -- current year:
    where
      test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR") and
      client_city is not null and
      client_asn_number is not null and
      client_asn_number is not null
    group by
      client_city,
      client_region,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name

  ) lastyear on
    all.client_city = lastyear.client_city and
    all.client_region = lastyear.client_region and
    all.client_country = lastyear.client_country and
    all.client_continent = lastyear.client_continent and
    all.client_asn_number = lastyear.client_asn_number and
    all.client_asn_name = lastyear.client_asn_name

  GROUP BY
    -- key fields:
    location_id,
    client_asn_number,

    -- metadata:
    client_asn_name,
    type,

    client_city,
    client_region,
    client_region_code,
    client_country,
    client_country_code,
    client_continent,
    client_continent_code,

    -- last week:
    last_week_test_count,
    last_week_download_speed_mbps_median,
    last_week_upload_speed_mbps_median,
    last_week_download_speed_mbps_avg,
    last_week_upload_speed_mbps_avg,
    last_week_download_speed_mbps_min,
    last_week_download_speed_mbps_max,
    last_week_upload_speed_mbps_min,
    last_week_upload_speed_mbps_max,
    last_week_download_speed_mbps_stddev,
    last_week_upload_speed_mbps_stddev,

    last_week_download_speed_mbps_B4_8,
    last_week_download_speed_mbps_B8_12,
    last_week_download_speed_mbps_B12_16,
    last_week_download_speed_mbps_B16_20,
    last_week_download_speed_mbps_B20_24,
    last_week_download_speed_mbps_B24_28,
    last_week_download_speed_mbps_B28_32,
    last_week_download_speed_mbps_B32_36,
    last_week_download_speed_mbps_B36_40,
    last_week_download_speed_mbps_B40_44,
    last_week_download_speed_mbps_B44_48,
    last_week_download_speed_mbps_B48_52,
    last_week_download_speed_mbps_B52_56,
    last_week_download_speed_mbps_B56_60,
    last_week_download_speed_mbps_B60_64,
    last_week_download_speed_mbps_B64_68,
    last_week_download_speed_mbps_B68_72,
    last_week_download_speed_mbps_B72_76,
    last_week_download_speed_mbps_B76_80,
    last_week_download_speed_mbps_B80_84,
    last_week_download_speed_mbps_B84_88,
    last_week_download_speed_mbps_B88_92,
    last_week_download_speed_mbps_B92_96,
    last_week_download_speed_mbps_B96_100,
    last_week_download_speed_mbps_BGT100,

    last_week_upload_speed_mbps_B4_8,
    last_week_upload_speed_mbps_B8_12,
    last_week_upload_speed_mbps_B12_16,
    last_week_upload_speed_mbps_B16_20,
    last_week_upload_speed_mbps_B20_24,
    last_week_upload_speed_mbps_B24_28,
    last_week_upload_speed_mbps_B28_32,
    last_week_upload_speed_mbps_B32_36,
    last_week_upload_speed_mbps_B36_40,
    last_week_upload_speed_mbps_B40_44,
    last_week_upload_speed_mbps_B44_48,
    last_week_upload_speed_mbps_B48_52,
    last_week_upload_speed_mbps_B52_56,
    last_week_upload_speed_mbps_B56_60,
    last_week_upload_speed_mbps_B60_64,
    last_week_upload_speed_mbps_B64_68,
    last_week_upload_speed_mbps_B68_72,
    last_week_upload_speed_mbps_B72_76,
    last_week_upload_speed_mbps_B76_80,
    last_week_upload_speed_mbps_B80_84,
    last_week_upload_speed_mbps_B84_88,
    last_week_upload_speed_mbps_B88_92,
    last_week_upload_speed_mbps_B92_96,
    last_week_upload_speed_mbps_B96_100,
    last_week_upload_speed_mbps_BGT100,

    -- last month:
    last_month_test_count,
    last_month_download_speed_mbps_median,
    last_month_upload_speed_mbps_median,
    last_month_download_speed_mbps_avg,
    last_month_upload_speed_mbps_avg,
    last_month_download_speed_mbps_min,
    last_month_download_speed_mbps_max,
    last_month_upload_speed_mbps_min,
    last_month_upload_speed_mbps_max,
    last_month_download_speed_mbps_stddev,
    last_month_upload_speed_mbps_stddev,

    last_month_download_speed_mbps_B4_8,
    last_month_download_speed_mbps_B8_12,
    last_month_download_speed_mbps_B12_16,
    last_month_download_speed_mbps_B16_20,
    last_month_download_speed_mbps_B20_24,
    last_month_download_speed_mbps_B24_28,
    last_month_download_speed_mbps_B28_32,
    last_month_download_speed_mbps_B32_36,
    last_month_download_speed_mbps_B36_40,
    last_month_download_speed_mbps_B40_44,
    last_month_download_speed_mbps_B44_48,
    last_month_download_speed_mbps_B48_52,
    last_month_download_speed_mbps_B52_56,
    last_month_download_speed_mbps_B56_60,
    last_month_download_speed_mbps_B60_64,
    last_month_download_speed_mbps_B64_68,
    last_month_download_speed_mbps_B68_72,
    last_month_download_speed_mbps_B72_76,
    last_month_download_speed_mbps_B76_80,
    last_month_download_speed_mbps_B80_84,
    last_month_download_speed_mbps_B84_88,
    last_month_download_speed_mbps_B88_92,
    last_month_download_speed_mbps_B92_96,
    last_month_download_speed_mbps_B96_100,
    last_month_download_speed_mbps_BGT100,
    last_month_upload_speed_mbps_B4_8,
    last_month_upload_speed_mbps_B8_12,
    last_month_upload_speed_mbps_B12_16,
    last_month_upload_speed_mbps_B16_20,
    last_month_upload_speed_mbps_B20_24,
    last_month_upload_speed_mbps_B24_28,
    last_month_upload_speed_mbps_B28_32,
    last_month_upload_speed_mbps_B32_36,
    last_month_upload_speed_mbps_B36_40,
    last_month_upload_speed_mbps_B40_44,
    last_month_upload_speed_mbps_B44_48,
    last_month_upload_speed_mbps_B48_52,
    last_month_upload_speed_mbps_B52_56,
    last_month_upload_speed_mbps_B56_60,
    last_month_upload_speed_mbps_B60_64,
    last_month_upload_speed_mbps_B64_68,
    last_month_upload_speed_mbps_B68_72,
    last_month_upload_speed_mbps_B72_76,
    last_month_upload_speed_mbps_B76_80,
    last_month_upload_speed_mbps_B80_84,
    last_month_upload_speed_mbps_B84_88,
    last_month_upload_speed_mbps_B88_92,
    last_month_upload_speed_mbps_B92_96,
    last_month_upload_speed_mbps_B96_100,
    last_month_upload_speed_mbps_BGT100,

    -- last year:
    last_year_test_count,
    last_year_download_speed_mbps_median,
    last_year_upload_speed_mbps_median,
    last_year_download_speed_mbps_avg,
    last_year_upload_speed_mbps_avg,
    last_year_download_speed_mbps_min,
    last_year_download_speed_mbps_max,
    last_year_upload_speed_mbps_min,
    last_year_upload_speed_mbps_max,
    last_year_download_speed_mbps_stddev,
    last_year_upload_speed_mbps_stddev,

    last_year_download_speed_mbps_B4_8,
    last_year_download_speed_mbps_B8_12,
    last_year_download_speed_mbps_B12_16,
    last_year_download_speed_mbps_B16_20,
    last_year_download_speed_mbps_B20_24,
    last_year_download_speed_mbps_B24_28,
    last_year_download_speed_mbps_B28_32,
    last_year_download_speed_mbps_B32_36,
    last_year_download_speed_mbps_B36_40,
    last_year_download_speed_mbps_B40_44,
    last_year_download_speed_mbps_B44_48,
    last_year_download_speed_mbps_B48_52,
    last_year_download_speed_mbps_B52_56,
    last_year_download_speed_mbps_B56_60,
    last_year_download_speed_mbps_B60_64,
    last_year_download_speed_mbps_B64_68,
    last_year_download_speed_mbps_B68_72,
    last_year_download_speed_mbps_B72_76,
    last_year_download_speed_mbps_B76_80,
    last_year_download_speed_mbps_B80_84,
    last_year_download_speed_mbps_B84_88,
    last_year_download_speed_mbps_B88_92,
    last_year_download_speed_mbps_B92_96,
    last_year_download_speed_mbps_B96_100,
    last_year_download_speed_mbps_BGT100,
    last_year_upload_speed_mbps_B4_8,
    last_year_upload_speed_mbps_B8_12,
    last_year_upload_speed_mbps_B12_16,
    last_year_upload_speed_mbps_B16_20,
    last_year_upload_speed_mbps_B20_24,
    last_year_upload_speed_mbps_B24_28,
    last_year_upload_speed_mbps_B28_32,
    last_year_upload_speed_mbps_B32_36,
    last_year_upload_speed_mbps_B36_40,
    last_year_upload_speed_mbps_B40_44,
    last_year_upload_speed_mbps_B44_48,
    last_year_upload_speed_mbps_B48_52,
    last_year_upload_speed_mbps_B52_56,
    last_year_upload_speed_mbps_B56_60,
    last_year_upload_speed_mbps_B60_64,
    last_year_upload_speed_mbps_B64_68,
    last_year_upload_speed_mbps_B68_72,
    last_year_upload_speed_mbps_B72_76,
    last_year_upload_speed_mbps_B76_80,
    last_year_upload_speed_mbps_B80_84,
    last_year_upload_speed_mbps_B84_88,
    last_year_upload_speed_mbps_B88_92,
    last_year_upload_speed_mbps_B92_96,
    last_year_upload_speed_mbps_B96_100,
    last_year_upload_speed_mbps_BGT100
),

-- ============
-- Regions
-- ============
(SELECT

  REPLACE(LOWER(CONCAT(
    IFNULL(all.client_continent_code, ""),
    IFNULL(all.client_country_code, ""), "",
    IFNULL(all.client_region_code, ""), ""
  )), " ", "") as location_id,

  all.client_asn_number as client_asn_number,
  all.client_asn_name as client_asn_name,

  all.client_region as client_region,
  all.client_country as client_country,
  all.client_continent as client_continent,

  all.client_region_code as client_region_code,
  all.client_country_code as client_country_code,
  all.client_continent_code as client_continent_code,

  "region" as type,

  -- last week measurements
  lastweek.last_week_test_count as last_week_test_count,
  lastweek.download_speed_mbps_median as last_week_download_speed_mbps_median,
  lastweek.upload_speed_mbps_median as last_week_upload_speed_mbps_median,
  lastweek.download_speed_mbps_avg as last_week_download_speed_mbps_avg,
  lastweek.upload_speed_mbps_avg as last_week_upload_speed_mbps_avg,
  lastweek.download_speed_mbps_min as last_week_download_speed_mbps_min,
  lastweek.download_speed_mbps_max as last_week_download_speed_mbps_max,
  lastweek.upload_speed_mbps_min as last_week_upload_speed_mbps_min,
  lastweek.upload_speed_mbps_max as last_week_upload_speed_mbps_max,
  lastweek.download_speed_mbps_stddev as last_week_download_speed_mbps_stddev,
  lastweek.upload_speed_mbps_stddev as last_week_upload_speed_mbps_stddev,

  lastweek.download_speed_mbps_B4_8 as last_week_download_speed_mbps_B4_8,
  lastweek.download_speed_mbps_B8_12 as last_week_download_speed_mbps_B8_12,
  lastweek.download_speed_mbps_B12_16 as last_week_download_speed_mbps_B12_16,
  lastweek.download_speed_mbps_B16_20 as last_week_download_speed_mbps_B16_20,
  lastweek.download_speed_mbps_B20_24 as last_week_download_speed_mbps_B20_24,
  lastweek.download_speed_mbps_B24_28 as last_week_download_speed_mbps_B24_28,
  lastweek.download_speed_mbps_B28_32 as last_week_download_speed_mbps_B28_32,
  lastweek.download_speed_mbps_B32_36 as last_week_download_speed_mbps_B32_36,
  lastweek.download_speed_mbps_B36_40 as last_week_download_speed_mbps_B36_40,
  lastweek.download_speed_mbps_B40_44 as last_week_download_speed_mbps_B40_44,
  lastweek.download_speed_mbps_B44_48 as last_week_download_speed_mbps_B44_48,
  lastweek.download_speed_mbps_B48_52 as last_week_download_speed_mbps_B48_52,
  lastweek.download_speed_mbps_B52_56 as last_week_download_speed_mbps_B52_56,
  lastweek.download_speed_mbps_B56_60 as last_week_download_speed_mbps_B56_60,
  lastweek.download_speed_mbps_B60_64 as last_week_download_speed_mbps_B60_64,
  lastweek.download_speed_mbps_B64_68 as last_week_download_speed_mbps_B64_68,
  lastweek.download_speed_mbps_B68_72 as last_week_download_speed_mbps_B68_72,
  lastweek.download_speed_mbps_B72_76 as last_week_download_speed_mbps_B72_76,
  lastweek.download_speed_mbps_B76_80 as last_week_download_speed_mbps_B76_80,
  lastweek.download_speed_mbps_B80_84 as last_week_download_speed_mbps_B80_84,
  lastweek.download_speed_mbps_B84_88 as last_week_download_speed_mbps_B84_88,
  lastweek.download_speed_mbps_B88_92 as last_week_download_speed_mbps_B88_92,
  lastweek.download_speed_mbps_B92_96 as last_week_download_speed_mbps_B92_96,
  lastweek.download_speed_mbps_B96_100 as last_week_download_speed_mbps_B96_100,
  lastweek.download_speed_mbps_BGT100 as last_week_download_speed_mbps_BGT100,

  lastweek.upload_speed_mbps_B4_8 as last_week_upload_speed_mbps_B4_8,
  lastweek.upload_speed_mbps_B8_12 as last_week_upload_speed_mbps_B8_12,
  lastweek.upload_speed_mbps_B12_16 as last_week_upload_speed_mbps_B12_16,
  lastweek.upload_speed_mbps_B16_20 as last_week_upload_speed_mbps_B16_20,
  lastweek.upload_speed_mbps_B20_24 as last_week_upload_speed_mbps_B20_24,
  lastweek.upload_speed_mbps_B24_28 as last_week_upload_speed_mbps_B24_28,
  lastweek.upload_speed_mbps_B28_32 as last_week_upload_speed_mbps_B28_32,
  lastweek.upload_speed_mbps_B32_36 as last_week_upload_speed_mbps_B32_36,
  lastweek.upload_speed_mbps_B36_40 as last_week_upload_speed_mbps_B36_40,
  lastweek.upload_speed_mbps_B40_44 as last_week_upload_speed_mbps_B40_44,
  lastweek.upload_speed_mbps_B44_48 as last_week_upload_speed_mbps_B44_48,
  lastweek.upload_speed_mbps_B48_52 as last_week_upload_speed_mbps_B48_52,
  lastweek.upload_speed_mbps_B52_56 as last_week_upload_speed_mbps_B52_56,
  lastweek.upload_speed_mbps_B56_60 as last_week_upload_speed_mbps_B56_60,
  lastweek.upload_speed_mbps_B60_64 as last_week_upload_speed_mbps_B60_64,
  lastweek.upload_speed_mbps_B64_68 as last_week_upload_speed_mbps_B64_68,
  lastweek.upload_speed_mbps_B68_72 as last_week_upload_speed_mbps_B68_72,
  lastweek.upload_speed_mbps_B72_76 as last_week_upload_speed_mbps_B72_76,
  lastweek.upload_speed_mbps_B76_80 as last_week_upload_speed_mbps_B76_80,
  lastweek.upload_speed_mbps_B80_84 as last_week_upload_speed_mbps_B80_84,
  lastweek.upload_speed_mbps_B84_88 as last_week_upload_speed_mbps_B84_88,
  lastweek.upload_speed_mbps_B88_92 as last_week_upload_speed_mbps_B88_92,
  lastweek.upload_speed_mbps_B92_96 as last_week_upload_speed_mbps_B92_96,
  lastweek.upload_speed_mbps_B96_100 as last_week_upload_speed_mbps_B96_100,
  lastweek.upload_speed_mbps_BGT100 as last_week_upload_speed_mbps_BGT100,

  -- last month measurements
  lastmonth.last_month_test_count as last_month_test_count,
  lastmonth.download_speed_mbps_median as last_month_download_speed_mbps_median,
  lastmonth.upload_speed_mbps_median as last_month_upload_speed_mbps_median,
  lastmonth.download_speed_mbps_avg as last_month_download_speed_mbps_avg,
  lastmonth.upload_speed_mbps_avg as last_month_upload_speed_mbps_avg,
  lastmonth.download_speed_mbps_min as last_month_download_speed_mbps_min,
  lastmonth.download_speed_mbps_max as last_month_download_speed_mbps_max,
  lastmonth.upload_speed_mbps_min as last_month_upload_speed_mbps_min,
  lastmonth.upload_speed_mbps_max as last_month_upload_speed_mbps_max,
  lastmonth.download_speed_mbps_stddev as last_month_download_speed_mbps_stddev,
  lastmonth.upload_speed_mbps_stddev as last_month_upload_speed_mbps_stddev,

  lastmonth.download_speed_mbps_B4_8 as last_month_download_speed_mbps_B4_8,
  lastmonth.download_speed_mbps_B8_12 as last_month_download_speed_mbps_B8_12,
  lastmonth.download_speed_mbps_B12_16 as last_month_download_speed_mbps_B12_16,
  lastmonth.download_speed_mbps_B16_20 as last_month_download_speed_mbps_B16_20,
  lastmonth.download_speed_mbps_B20_24 as last_month_download_speed_mbps_B20_24,
  lastmonth.download_speed_mbps_B24_28 as last_month_download_speed_mbps_B24_28,
  lastmonth.download_speed_mbps_B28_32 as last_month_download_speed_mbps_B28_32,
  lastmonth.download_speed_mbps_B32_36 as last_month_download_speed_mbps_B32_36,
  lastmonth.download_speed_mbps_B36_40 as last_month_download_speed_mbps_B36_40,
  lastmonth.download_speed_mbps_B40_44 as last_month_download_speed_mbps_B40_44,
  lastmonth.download_speed_mbps_B44_48 as last_month_download_speed_mbps_B44_48,
  lastmonth.download_speed_mbps_B48_52 as last_month_download_speed_mbps_B48_52,
  lastmonth.download_speed_mbps_B52_56 as last_month_download_speed_mbps_B52_56,
  lastmonth.download_speed_mbps_B56_60 as last_month_download_speed_mbps_B56_60,
  lastmonth.download_speed_mbps_B60_64 as last_month_download_speed_mbps_B60_64,
  lastmonth.download_speed_mbps_B64_68 as last_month_download_speed_mbps_B64_68,
  lastmonth.download_speed_mbps_B68_72 as last_month_download_speed_mbps_B68_72,
  lastmonth.download_speed_mbps_B72_76 as last_month_download_speed_mbps_B72_76,
  lastmonth.download_speed_mbps_B76_80 as last_month_download_speed_mbps_B76_80,
  lastmonth.download_speed_mbps_B80_84 as last_month_download_speed_mbps_B80_84,
  lastmonth.download_speed_mbps_B84_88 as last_month_download_speed_mbps_B84_88,
  lastmonth.download_speed_mbps_B88_92 as last_month_download_speed_mbps_B88_92,
  lastmonth.download_speed_mbps_B92_96 as last_month_download_speed_mbps_B92_96,
  lastmonth.download_speed_mbps_B96_100 as last_month_download_speed_mbps_B96_100,
  lastmonth.download_speed_mbps_BGT100 as last_month_download_speed_mbps_BGT100,

  lastmonth.upload_speed_mbps_B4_8 as last_month_upload_speed_mbps_B4_8,
  lastmonth.upload_speed_mbps_B8_12 as last_month_upload_speed_mbps_B8_12,
  lastmonth.upload_speed_mbps_B12_16 as last_month_upload_speed_mbps_B12_16,
  lastmonth.upload_speed_mbps_B16_20 as last_month_upload_speed_mbps_B16_20,
  lastmonth.upload_speed_mbps_B20_24 as last_month_upload_speed_mbps_B20_24,
  lastmonth.upload_speed_mbps_B24_28 as last_month_upload_speed_mbps_B24_28,
  lastmonth.upload_speed_mbps_B28_32 as last_month_upload_speed_mbps_B28_32,
  lastmonth.upload_speed_mbps_B32_36 as last_month_upload_speed_mbps_B32_36,
  lastmonth.upload_speed_mbps_B36_40 as last_month_upload_speed_mbps_B36_40,
  lastmonth.upload_speed_mbps_B40_44 as last_month_upload_speed_mbps_B40_44,
  lastmonth.upload_speed_mbps_B44_48 as last_month_upload_speed_mbps_B44_48,
  lastmonth.upload_speed_mbps_B48_52 as last_month_upload_speed_mbps_B48_52,
  lastmonth.upload_speed_mbps_B52_56 as last_month_upload_speed_mbps_B52_56,
  lastmonth.upload_speed_mbps_B56_60 as last_month_upload_speed_mbps_B56_60,
  lastmonth.upload_speed_mbps_B60_64 as last_month_upload_speed_mbps_B60_64,
  lastmonth.upload_speed_mbps_B64_68 as last_month_upload_speed_mbps_B64_68,
  lastmonth.upload_speed_mbps_B68_72 as last_month_upload_speed_mbps_B68_72,
  lastmonth.upload_speed_mbps_B72_76 as last_month_upload_speed_mbps_B72_76,
  lastmonth.upload_speed_mbps_B76_80 as last_month_upload_speed_mbps_B76_80,
  lastmonth.upload_speed_mbps_B80_84 as last_month_upload_speed_mbps_B80_84,
  lastmonth.upload_speed_mbps_B84_88 as last_month_upload_speed_mbps_B84_88,
  lastmonth.upload_speed_mbps_B88_92 as last_month_upload_speed_mbps_B88_92,
  lastmonth.upload_speed_mbps_B92_96 as last_month_upload_speed_mbps_B92_96,
  lastmonth.upload_speed_mbps_B96_100 as last_month_upload_speed_mbps_B96_100,
  lastmonth.upload_speed_mbps_BGT100 as last_month_upload_speed_mbps_BGT100,

  -- last year measurements
  lastyear.last_year_test_count as last_year_test_count,
  lastyear.download_speed_mbps_median as last_year_download_speed_mbps_median,
  lastyear.upload_speed_mbps_median as last_year_upload_speed_mbps_median,
  lastyear.download_speed_mbps_avg as last_year_download_speed_mbps_avg,
  lastyear.upload_speed_mbps_avg as last_year_upload_speed_mbps_avg,
  lastyear.download_speed_mbps_min as last_year_download_speed_mbps_min,
  lastyear.download_speed_mbps_max as last_year_download_speed_mbps_max,
  lastyear.upload_speed_mbps_min as last_year_upload_speed_mbps_min,
  lastyear.upload_speed_mbps_max as last_year_upload_speed_mbps_max,
  lastyear.download_speed_mbps_stddev as last_year_download_speed_mbps_stddev,
  lastyear.upload_speed_mbps_stddev as last_year_upload_speed_mbps_stddev,

  lastyear.download_speed_mbps_B4_8 as last_year_download_speed_mbps_B4_8,
  lastyear.download_speed_mbps_B8_12 as last_year_download_speed_mbps_B8_12,
  lastyear.download_speed_mbps_B12_16 as last_year_download_speed_mbps_B12_16,
  lastyear.download_speed_mbps_B16_20 as last_year_download_speed_mbps_B16_20,
  lastyear.download_speed_mbps_B20_24 as last_year_download_speed_mbps_B20_24,
  lastyear.download_speed_mbps_B24_28 as last_year_download_speed_mbps_B24_28,
  lastyear.download_speed_mbps_B28_32 as last_year_download_speed_mbps_B28_32,
  lastyear.download_speed_mbps_B32_36 as last_year_download_speed_mbps_B32_36,
  lastyear.download_speed_mbps_B36_40 as last_year_download_speed_mbps_B36_40,
  lastyear.download_speed_mbps_B40_44 as last_year_download_speed_mbps_B40_44,
  lastyear.download_speed_mbps_B44_48 as last_year_download_speed_mbps_B44_48,
  lastyear.download_speed_mbps_B48_52 as last_year_download_speed_mbps_B48_52,
  lastyear.download_speed_mbps_B52_56 as last_year_download_speed_mbps_B52_56,
  lastyear.download_speed_mbps_B56_60 as last_year_download_speed_mbps_B56_60,
  lastyear.download_speed_mbps_B60_64 as last_year_download_speed_mbps_B60_64,
  lastyear.download_speed_mbps_B64_68 as last_year_download_speed_mbps_B64_68,
  lastyear.download_speed_mbps_B68_72 as last_year_download_speed_mbps_B68_72,
  lastyear.download_speed_mbps_B72_76 as last_year_download_speed_mbps_B72_76,
  lastyear.download_speed_mbps_B76_80 as last_year_download_speed_mbps_B76_80,
  lastyear.download_speed_mbps_B80_84 as last_year_download_speed_mbps_B80_84,
  lastyear.download_speed_mbps_B84_88 as last_year_download_speed_mbps_B84_88,
  lastyear.download_speed_mbps_B88_92 as last_year_download_speed_mbps_B88_92,
  lastyear.download_speed_mbps_B92_96 as last_year_download_speed_mbps_B92_96,
  lastyear.download_speed_mbps_B96_100 as last_year_download_speed_mbps_B96_100,
  lastyear.download_speed_mbps_BGT100 as last_year_download_speed_mbps_BGT100,
  lastyear.upload_speed_mbps_B4_8 as last_year_upload_speed_mbps_B4_8,
  lastyear.upload_speed_mbps_B8_12 as last_year_upload_speed_mbps_B8_12,
  lastyear.upload_speed_mbps_B12_16 as last_year_upload_speed_mbps_B12_16,
  lastyear.upload_speed_mbps_B16_20 as last_year_upload_speed_mbps_B16_20,
  lastyear.upload_speed_mbps_B20_24 as last_year_upload_speed_mbps_B20_24,
  lastyear.upload_speed_mbps_B24_28 as last_year_upload_speed_mbps_B24_28,
  lastyear.upload_speed_mbps_B28_32 as last_year_upload_speed_mbps_B28_32,
  lastyear.upload_speed_mbps_B32_36 as last_year_upload_speed_mbps_B32_36,
  lastyear.upload_speed_mbps_B36_40 as last_year_upload_speed_mbps_B36_40,
  lastyear.upload_speed_mbps_B40_44 as last_year_upload_speed_mbps_B40_44,
  lastyear.upload_speed_mbps_B44_48 as last_year_upload_speed_mbps_B44_48,
  lastyear.upload_speed_mbps_B48_52 as last_year_upload_speed_mbps_B48_52,
  lastyear.upload_speed_mbps_B52_56 as last_year_upload_speed_mbps_B52_56,
  lastyear.upload_speed_mbps_B56_60 as last_year_upload_speed_mbps_B56_60,
  lastyear.upload_speed_mbps_B60_64 as last_year_upload_speed_mbps_B60_64,
  lastyear.upload_speed_mbps_B64_68 as last_year_upload_speed_mbps_B64_68,
  lastyear.upload_speed_mbps_B68_72 as last_year_upload_speed_mbps_B68_72,
  lastyear.upload_speed_mbps_B72_76 as last_year_upload_speed_mbps_B72_76,
  lastyear.upload_speed_mbps_B76_80 as last_year_upload_speed_mbps_B76_80,
  lastyear.upload_speed_mbps_B80_84 as last_year_upload_speed_mbps_B80_84,
  lastyear.upload_speed_mbps_B84_88 as last_year_upload_speed_mbps_B84_88,
  lastyear.upload_speed_mbps_B88_92 as last_year_upload_speed_mbps_B88_92,
  lastyear.upload_speed_mbps_B92_96 as last_year_upload_speed_mbps_B92_96,
  lastyear.upload_speed_mbps_B96_100 as last_year_upload_speed_mbps_B96_100,
  lastyear.upload_speed_mbps_BGT100 as last_year_upload_speed_mbps_BGT100,

  FROM [mlab-oti:bocoup_prod.all_ip_by_day] all,

  left join
  (
    SELECT
      count(*) as last_week_test_count,

      client_region,
      client_country,
      client_continent,

      client_asn_number,
      client_asn_name,

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

      -- download bins
      SUM(IF(download_speed_mbps < 4 ,1,0)) AS download_speed_mbps_B0_4,
      SUM(IF(download_speed_mbps >= 4 AND download_speed_mbps < 8, 1,0)) AS download_speed_mbps_B4_8,
      SUM(IF(download_speed_mbps >= 8 AND download_speed_mbps < 12, 1,0)) AS download_speed_mbps_B8_12,
      SUM(IF(download_speed_mbps >= 12 AND download_speed_mbps < 16, 1,0)) AS download_speed_mbps_B12_16,
      SUM(IF(download_speed_mbps >= 16 AND download_speed_mbps < 20, 1,0)) AS download_speed_mbps_B16_20,
      SUM(IF(download_speed_mbps >= 20 AND download_speed_mbps < 24, 1,0)) AS download_speed_mbps_B20_24,
      SUM(IF(download_speed_mbps >= 24 AND download_speed_mbps < 28, 1,0)) AS download_speed_mbps_B24_28,
      SUM(IF(download_speed_mbps >= 28 AND download_speed_mbps < 32, 1,0)) AS download_speed_mbps_B28_32,
      SUM(IF(download_speed_mbps >= 32 AND download_speed_mbps < 36, 1,0)) AS download_speed_mbps_B32_36,
      SUM(IF(download_speed_mbps >= 36 AND download_speed_mbps < 40, 1,0)) AS download_speed_mbps_B36_40,
      SUM(IF(download_speed_mbps >= 40 AND download_speed_mbps < 44, 1,0)) AS download_speed_mbps_B40_44,
      SUM(IF(download_speed_mbps >= 44 AND download_speed_mbps < 48, 1,0)) AS download_speed_mbps_B44_48,
      SUM(IF(download_speed_mbps >= 48 AND download_speed_mbps < 52, 1,0)) AS download_speed_mbps_B48_52,
      SUM(IF(download_speed_mbps >= 52 AND download_speed_mbps < 56, 1,0)) AS download_speed_mbps_B52_56,
      SUM(IF(download_speed_mbps >= 56 AND download_speed_mbps < 60, 1,0)) AS download_speed_mbps_B56_60,
      SUM(IF(download_speed_mbps >= 60 AND download_speed_mbps < 64, 1,0)) AS download_speed_mbps_B60_64,
      SUM(IF(download_speed_mbps >= 64 AND download_speed_mbps < 68, 1,0)) AS download_speed_mbps_B64_68,
      SUM(IF(download_speed_mbps >= 68 AND download_speed_mbps < 72, 1,0)) AS download_speed_mbps_B68_72,
      SUM(IF(download_speed_mbps >= 72 AND download_speed_mbps < 76, 1,0)) AS download_speed_mbps_B72_76,
      SUM(IF(download_speed_mbps >= 76 AND download_speed_mbps < 80, 1,0)) AS download_speed_mbps_B76_80,
      SUM(IF(download_speed_mbps >= 80 AND download_speed_mbps < 84, 1,0)) AS download_speed_mbps_B80_84,
      SUM(IF(download_speed_mbps >= 84 AND download_speed_mbps < 88, 1,0)) AS download_speed_mbps_B84_88,
      SUM(IF(download_speed_mbps >= 88 AND download_speed_mbps < 92, 1,0)) AS download_speed_mbps_B88_92,
      SUM(IF(download_speed_mbps >= 92 AND download_speed_mbps < 96, 1,0)) AS download_speed_mbps_B92_96,
      SUM(IF(download_speed_mbps >= 96 AND download_speed_mbps < 100, 1,0)) AS download_speed_mbps_B96_100,
      SUM(IF(download_speed_mbps >= 100, 1,0)) AS download_speed_mbps_BGT100,

      -- upload bins
      SUM(IF(upload_speed_mbps < 4 ,1,0)) AS upload_speed_mbps_B0_4,
      SUM(IF(upload_speed_mbps >= 4 AND upload_speed_mbps < 8, 1,0)) AS upload_speed_mbps_B4_8,
      SUM(IF(upload_speed_mbps >= 8 AND upload_speed_mbps < 12, 1,0)) AS upload_speed_mbps_B8_12,
      SUM(IF(upload_speed_mbps >= 12 AND upload_speed_mbps < 16, 1,0)) AS upload_speed_mbps_B12_16,
      SUM(IF(upload_speed_mbps >= 16 AND upload_speed_mbps < 20, 1,0)) AS upload_speed_mbps_B16_20,
      SUM(IF(upload_speed_mbps >= 20 AND upload_speed_mbps < 24, 1,0)) AS upload_speed_mbps_B20_24,
      SUM(IF(upload_speed_mbps >= 24 AND upload_speed_mbps < 28, 1,0)) AS upload_speed_mbps_B24_28,
      SUM(IF(upload_speed_mbps >= 28 AND upload_speed_mbps < 32, 1,0)) AS upload_speed_mbps_B28_32,
      SUM(IF(upload_speed_mbps >= 32 AND upload_speed_mbps < 36, 1,0)) AS upload_speed_mbps_B32_36,
      SUM(IF(upload_speed_mbps >= 36 AND upload_speed_mbps < 40, 1,0)) AS upload_speed_mbps_B36_40,
      SUM(IF(upload_speed_mbps >= 40 AND upload_speed_mbps < 44, 1,0)) AS upload_speed_mbps_B40_44,
      SUM(IF(upload_speed_mbps >= 44 AND upload_speed_mbps < 48, 1,0)) AS upload_speed_mbps_B44_48,
      SUM(IF(upload_speed_mbps >= 48 AND upload_speed_mbps < 52, 1,0)) AS upload_speed_mbps_B48_52,
      SUM(IF(upload_speed_mbps >= 52 AND upload_speed_mbps < 56, 1,0)) AS upload_speed_mbps_B52_56,
      SUM(IF(upload_speed_mbps >= 56 AND upload_speed_mbps < 60, 1,0)) AS upload_speed_mbps_B56_60,
      SUM(IF(upload_speed_mbps >= 60 AND upload_speed_mbps < 64, 1,0)) AS upload_speed_mbps_B60_64,
      SUM(IF(upload_speed_mbps >= 64 AND upload_speed_mbps < 68, 1,0)) AS upload_speed_mbps_B64_68,
      SUM(IF(upload_speed_mbps >= 68 AND upload_speed_mbps < 72, 1,0)) AS upload_speed_mbps_B68_72,
      SUM(IF(upload_speed_mbps >= 72 AND upload_speed_mbps < 76, 1,0)) AS upload_speed_mbps_B72_76,
      SUM(IF(upload_speed_mbps >= 76 AND upload_speed_mbps < 80, 1,0)) AS upload_speed_mbps_B76_80,
      SUM(IF(upload_speed_mbps >= 80 AND upload_speed_mbps < 84, 1,0)) AS upload_speed_mbps_B80_84,
      SUM(IF(upload_speed_mbps >= 84 AND upload_speed_mbps < 88, 1,0)) AS upload_speed_mbps_B84_88,
      SUM(IF(upload_speed_mbps >= 88 AND upload_speed_mbps < 92, 1,0)) AS upload_speed_mbps_B88_92,
      SUM(IF(upload_speed_mbps >= 92 AND upload_speed_mbps < 96, 1,0)) AS upload_speed_mbps_B92_96,
      SUM(IF(upload_speed_mbps >= 96 AND upload_speed_mbps < 100, 1,0)) AS upload_speed_mbps_B96_100,
      SUM(IF(upload_speed_mbps >= 100, 1,0)) AS upload_speed_mbps_BGT100,

    from [mlab-oti:bocoup_prod.all_ip_by_day]
    where
      test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, "DAY") and
      client_asn_number is not null and
      client_asn_number is not null

    group by
      client_asn_number,
      client_asn_name,
      client_region,
      client_country,
      client_continent

  ) lastweek on
    all.client_region = lastweek.client_region and
    all.client_country = lastweek.client_country and
    all.client_continent = lastweek.client_continent and
    all.client_asn_number = lastweek.client_asn_number and
    all.client_asn_name = lastweek.client_asn_name

  -- Compute metrics for the current month
  left join
  (
    SELECT
      count(*) as last_month_test_count,
      client_region,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name,

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

      -- download bins
      SUM(IF(download_speed_mbps < 4 ,1,0)) AS download_speed_mbps_B0_4,
      SUM(IF(download_speed_mbps >= 4 AND download_speed_mbps < 8, 1,0)) AS download_speed_mbps_B4_8,
      SUM(IF(download_speed_mbps >= 8 AND download_speed_mbps < 12, 1,0)) AS download_speed_mbps_B8_12,
      SUM(IF(download_speed_mbps >= 12 AND download_speed_mbps < 16, 1,0)) AS download_speed_mbps_B12_16,
      SUM(IF(download_speed_mbps >= 16 AND download_speed_mbps < 20, 1,0)) AS download_speed_mbps_B16_20,
      SUM(IF(download_speed_mbps >= 20 AND download_speed_mbps < 24, 1,0)) AS download_speed_mbps_B20_24,
      SUM(IF(download_speed_mbps >= 24 AND download_speed_mbps < 28, 1,0)) AS download_speed_mbps_B24_28,
      SUM(IF(download_speed_mbps >= 28 AND download_speed_mbps < 32, 1,0)) AS download_speed_mbps_B28_32,
      SUM(IF(download_speed_mbps >= 32 AND download_speed_mbps < 36, 1,0)) AS download_speed_mbps_B32_36,
      SUM(IF(download_speed_mbps >= 36 AND download_speed_mbps < 40, 1,0)) AS download_speed_mbps_B36_40,
      SUM(IF(download_speed_mbps >= 40 AND download_speed_mbps < 44, 1,0)) AS download_speed_mbps_B40_44,
      SUM(IF(download_speed_mbps >= 44 AND download_speed_mbps < 48, 1,0)) AS download_speed_mbps_B44_48,
      SUM(IF(download_speed_mbps >= 48 AND download_speed_mbps < 52, 1,0)) AS download_speed_mbps_B48_52,
      SUM(IF(download_speed_mbps >= 52 AND download_speed_mbps < 56, 1,0)) AS download_speed_mbps_B52_56,
      SUM(IF(download_speed_mbps >= 56 AND download_speed_mbps < 60, 1,0)) AS download_speed_mbps_B56_60,
      SUM(IF(download_speed_mbps >= 60 AND download_speed_mbps < 64, 1,0)) AS download_speed_mbps_B60_64,
      SUM(IF(download_speed_mbps >= 64 AND download_speed_mbps < 68, 1,0)) AS download_speed_mbps_B64_68,
      SUM(IF(download_speed_mbps >= 68 AND download_speed_mbps < 72, 1,0)) AS download_speed_mbps_B68_72,
      SUM(IF(download_speed_mbps >= 72 AND download_speed_mbps < 76, 1,0)) AS download_speed_mbps_B72_76,
      SUM(IF(download_speed_mbps >= 76 AND download_speed_mbps < 80, 1,0)) AS download_speed_mbps_B76_80,
      SUM(IF(download_speed_mbps >= 80 AND download_speed_mbps < 84, 1,0)) AS download_speed_mbps_B80_84,
      SUM(IF(download_speed_mbps >= 84 AND download_speed_mbps < 88, 1,0)) AS download_speed_mbps_B84_88,
      SUM(IF(download_speed_mbps >= 88 AND download_speed_mbps < 92, 1,0)) AS download_speed_mbps_B88_92,
      SUM(IF(download_speed_mbps >= 92 AND download_speed_mbps < 96, 1,0)) AS download_speed_mbps_B92_96,
      SUM(IF(download_speed_mbps >= 96 AND download_speed_mbps < 100, 1,0)) AS download_speed_mbps_B96_100,
      SUM(IF(download_speed_mbps >= 100, 1,0)) AS download_speed_mbps_BGT100,

      -- upload bins
      SUM(IF(upload_speed_mbps < 4 ,1,0)) AS upload_speed_mbps_B0_4,
      SUM(IF(upload_speed_mbps >= 4 AND upload_speed_mbps < 8, 1,0)) AS upload_speed_mbps_B4_8,
      SUM(IF(upload_speed_mbps >= 8 AND upload_speed_mbps < 12, 1,0)) AS upload_speed_mbps_B8_12,
      SUM(IF(upload_speed_mbps >= 12 AND upload_speed_mbps < 16, 1,0)) AS upload_speed_mbps_B12_16,
      SUM(IF(upload_speed_mbps >= 16 AND upload_speed_mbps < 20, 1,0)) AS upload_speed_mbps_B16_20,
      SUM(IF(upload_speed_mbps >= 20 AND upload_speed_mbps < 24, 1,0)) AS upload_speed_mbps_B20_24,
      SUM(IF(upload_speed_mbps >= 24 AND upload_speed_mbps < 28, 1,0)) AS upload_speed_mbps_B24_28,
      SUM(IF(upload_speed_mbps >= 28 AND upload_speed_mbps < 32, 1,0)) AS upload_speed_mbps_B28_32,
      SUM(IF(upload_speed_mbps >= 32 AND upload_speed_mbps < 36, 1,0)) AS upload_speed_mbps_B32_36,
      SUM(IF(upload_speed_mbps >= 36 AND upload_speed_mbps < 40, 1,0)) AS upload_speed_mbps_B36_40,
      SUM(IF(upload_speed_mbps >= 40 AND upload_speed_mbps < 44, 1,0)) AS upload_speed_mbps_B40_44,
      SUM(IF(upload_speed_mbps >= 44 AND upload_speed_mbps < 48, 1,0)) AS upload_speed_mbps_B44_48,
      SUM(IF(upload_speed_mbps >= 48 AND upload_speed_mbps < 52, 1,0)) AS upload_speed_mbps_B48_52,
      SUM(IF(upload_speed_mbps >= 52 AND upload_speed_mbps < 56, 1,0)) AS upload_speed_mbps_B52_56,
      SUM(IF(upload_speed_mbps >= 56 AND upload_speed_mbps < 60, 1,0)) AS upload_speed_mbps_B56_60,
      SUM(IF(upload_speed_mbps >= 60 AND upload_speed_mbps < 64, 1,0)) AS upload_speed_mbps_B60_64,
      SUM(IF(upload_speed_mbps >= 64 AND upload_speed_mbps < 68, 1,0)) AS upload_speed_mbps_B64_68,
      SUM(IF(upload_speed_mbps >= 68 AND upload_speed_mbps < 72, 1,0)) AS upload_speed_mbps_B68_72,
      SUM(IF(upload_speed_mbps >= 72 AND upload_speed_mbps < 76, 1,0)) AS upload_speed_mbps_B72_76,
      SUM(IF(upload_speed_mbps >= 76 AND upload_speed_mbps < 80, 1,0)) AS upload_speed_mbps_B76_80,
      SUM(IF(upload_speed_mbps >= 80 AND upload_speed_mbps < 84, 1,0)) AS upload_speed_mbps_B80_84,
      SUM(IF(upload_speed_mbps >= 84 AND upload_speed_mbps < 88, 1,0)) AS upload_speed_mbps_B84_88,
      SUM(IF(upload_speed_mbps >= 88 AND upload_speed_mbps < 92, 1,0)) AS upload_speed_mbps_B88_92,
      SUM(IF(upload_speed_mbps >= 92 AND upload_speed_mbps < 96, 1,0)) AS upload_speed_mbps_B92_96,
      SUM(IF(upload_speed_mbps >= 96 AND upload_speed_mbps < 100, 1,0)) AS upload_speed_mbps_B96_100,
      SUM(IF(upload_speed_mbps >= 100, 1,0)) AS upload_speed_mbps_BGT100,

    from [mlab-oti:bocoup_prod.all_ip_by_day]
    -- current month:
    where
      test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH") and
      client_asn_number is not null and
      client_asn_number is not null

    group by
      client_region,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name
  ) lastmonth on
    all.client_region = lastmonth.client_region and
    all.client_country = lastmonth.client_country and
    all.client_continent = lastmonth.client_continent and
    all.client_asn_number = lastmonth.client_asn_number and
    all.client_asn_name = lastmonth.client_asn_name

  -- Compute metrics for the current year
  left join
  (
    SELECT
      count(*) as last_year_test_count,
      client_region,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name,

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

      -- download bins
      SUM(IF(download_speed_mbps < 4 ,1,0)) AS download_speed_mbps_B0_4,
      SUM(IF(download_speed_mbps >= 4 AND download_speed_mbps < 8, 1,0)) AS download_speed_mbps_B4_8,
      SUM(IF(download_speed_mbps >= 8 AND download_speed_mbps < 12, 1,0)) AS download_speed_mbps_B8_12,
      SUM(IF(download_speed_mbps >= 12 AND download_speed_mbps < 16, 1,0)) AS download_speed_mbps_B12_16,
      SUM(IF(download_speed_mbps >= 16 AND download_speed_mbps < 20, 1,0)) AS download_speed_mbps_B16_20,
      SUM(IF(download_speed_mbps >= 20 AND download_speed_mbps < 24, 1,0)) AS download_speed_mbps_B20_24,
      SUM(IF(download_speed_mbps >= 24 AND download_speed_mbps < 28, 1,0)) AS download_speed_mbps_B24_28,
      SUM(IF(download_speed_mbps >= 28 AND download_speed_mbps < 32, 1,0)) AS download_speed_mbps_B28_32,
      SUM(IF(download_speed_mbps >= 32 AND download_speed_mbps < 36, 1,0)) AS download_speed_mbps_B32_36,
      SUM(IF(download_speed_mbps >= 36 AND download_speed_mbps < 40, 1,0)) AS download_speed_mbps_B36_40,
      SUM(IF(download_speed_mbps >= 40 AND download_speed_mbps < 44, 1,0)) AS download_speed_mbps_B40_44,
      SUM(IF(download_speed_mbps >= 44 AND download_speed_mbps < 48, 1,0)) AS download_speed_mbps_B44_48,
      SUM(IF(download_speed_mbps >= 48 AND download_speed_mbps < 52, 1,0)) AS download_speed_mbps_B48_52,
      SUM(IF(download_speed_mbps >= 52 AND download_speed_mbps < 56, 1,0)) AS download_speed_mbps_B52_56,
      SUM(IF(download_speed_mbps >= 56 AND download_speed_mbps < 60, 1,0)) AS download_speed_mbps_B56_60,
      SUM(IF(download_speed_mbps >= 60 AND download_speed_mbps < 64, 1,0)) AS download_speed_mbps_B60_64,
      SUM(IF(download_speed_mbps >= 64 AND download_speed_mbps < 68, 1,0)) AS download_speed_mbps_B64_68,
      SUM(IF(download_speed_mbps >= 68 AND download_speed_mbps < 72, 1,0)) AS download_speed_mbps_B68_72,
      SUM(IF(download_speed_mbps >= 72 AND download_speed_mbps < 76, 1,0)) AS download_speed_mbps_B72_76,
      SUM(IF(download_speed_mbps >= 76 AND download_speed_mbps < 80, 1,0)) AS download_speed_mbps_B76_80,
      SUM(IF(download_speed_mbps >= 80 AND download_speed_mbps < 84, 1,0)) AS download_speed_mbps_B80_84,
      SUM(IF(download_speed_mbps >= 84 AND download_speed_mbps < 88, 1,0)) AS download_speed_mbps_B84_88,
      SUM(IF(download_speed_mbps >= 88 AND download_speed_mbps < 92, 1,0)) AS download_speed_mbps_B88_92,
      SUM(IF(download_speed_mbps >= 92 AND download_speed_mbps < 96, 1,0)) AS download_speed_mbps_B92_96,
      SUM(IF(download_speed_mbps >= 96 AND download_speed_mbps < 100, 1,0)) AS download_speed_mbps_B96_100,
      SUM(IF(download_speed_mbps >= 100, 1,0)) AS download_speed_mbps_BGT100,

      -- upload bins
      SUM(IF(upload_speed_mbps < 4 ,1,0)) AS upload_speed_mbps_B0_4,
      SUM(IF(upload_speed_mbps >= 4 AND upload_speed_mbps < 8, 1,0)) AS upload_speed_mbps_B4_8,
      SUM(IF(upload_speed_mbps >= 8 AND upload_speed_mbps < 12, 1,0)) AS upload_speed_mbps_B8_12,
      SUM(IF(upload_speed_mbps >= 12 AND upload_speed_mbps < 16, 1,0)) AS upload_speed_mbps_B12_16,
      SUM(IF(upload_speed_mbps >= 16 AND upload_speed_mbps < 20, 1,0)) AS upload_speed_mbps_B16_20,
      SUM(IF(upload_speed_mbps >= 20 AND upload_speed_mbps < 24, 1,0)) AS upload_speed_mbps_B20_24,
      SUM(IF(upload_speed_mbps >= 24 AND upload_speed_mbps < 28, 1,0)) AS upload_speed_mbps_B24_28,
      SUM(IF(upload_speed_mbps >= 28 AND upload_speed_mbps < 32, 1,0)) AS upload_speed_mbps_B28_32,
      SUM(IF(upload_speed_mbps >= 32 AND upload_speed_mbps < 36, 1,0)) AS upload_speed_mbps_B32_36,
      SUM(IF(upload_speed_mbps >= 36 AND upload_speed_mbps < 40, 1,0)) AS upload_speed_mbps_B36_40,
      SUM(IF(upload_speed_mbps >= 40 AND upload_speed_mbps < 44, 1,0)) AS upload_speed_mbps_B40_44,
      SUM(IF(upload_speed_mbps >= 44 AND upload_speed_mbps < 48, 1,0)) AS upload_speed_mbps_B44_48,
      SUM(IF(upload_speed_mbps >= 48 AND upload_speed_mbps < 52, 1,0)) AS upload_speed_mbps_B48_52,
      SUM(IF(upload_speed_mbps >= 52 AND upload_speed_mbps < 56, 1,0)) AS upload_speed_mbps_B52_56,
      SUM(IF(upload_speed_mbps >= 56 AND upload_speed_mbps < 60, 1,0)) AS upload_speed_mbps_B56_60,
      SUM(IF(upload_speed_mbps >= 60 AND upload_speed_mbps < 64, 1,0)) AS upload_speed_mbps_B60_64,
      SUM(IF(upload_speed_mbps >= 64 AND upload_speed_mbps < 68, 1,0)) AS upload_speed_mbps_B64_68,
      SUM(IF(upload_speed_mbps >= 68 AND upload_speed_mbps < 72, 1,0)) AS upload_speed_mbps_B68_72,
      SUM(IF(upload_speed_mbps >= 72 AND upload_speed_mbps < 76, 1,0)) AS upload_speed_mbps_B72_76,
      SUM(IF(upload_speed_mbps >= 76 AND upload_speed_mbps < 80, 1,0)) AS upload_speed_mbps_B76_80,
      SUM(IF(upload_speed_mbps >= 80 AND upload_speed_mbps < 84, 1,0)) AS upload_speed_mbps_B80_84,
      SUM(IF(upload_speed_mbps >= 84 AND upload_speed_mbps < 88, 1,0)) AS upload_speed_mbps_B84_88,
      SUM(IF(upload_speed_mbps >= 88 AND upload_speed_mbps < 92, 1,0)) AS upload_speed_mbps_B88_92,
      SUM(IF(upload_speed_mbps >= 92 AND upload_speed_mbps < 96, 1,0)) AS upload_speed_mbps_B92_96,
      SUM(IF(upload_speed_mbps >= 96 AND upload_speed_mbps < 100, 1,0)) AS upload_speed_mbps_B96_100,
      SUM(IF(upload_speed_mbps >= 100, 1,0)) AS upload_speed_mbps_BGT100,

    from [mlab-oti:bocoup_prod.all_ip_by_day]
    -- current year:
    where
      test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR") and
      client_asn_number is not null and
      client_asn_number is not null
    group by
      client_region,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name

  ) lastyear on
    all.client_region = lastyear.client_region and
    all.client_country = lastyear.client_country and
    all.client_continent = lastyear.client_continent and
    all.client_asn_number = lastyear.client_asn_number and
    all.client_asn_name = lastyear.client_asn_name

  GROUP BY
    -- key fields:
    location_id,
    client_asn_number,

    -- metadata:
    client_asn_name,
    type,

    client_region,
    client_region_code,
    client_country,
    client_country_code,
    client_continent,
    client_continent_code,

    -- last week:
    last_week_test_count,
    last_week_download_speed_mbps_median,
    last_week_upload_speed_mbps_median,
    last_week_download_speed_mbps_avg,
    last_week_upload_speed_mbps_avg,
    last_week_download_speed_mbps_min,
    last_week_download_speed_mbps_max,
    last_week_upload_speed_mbps_min,
    last_week_upload_speed_mbps_max,
    last_week_download_speed_mbps_stddev,
    last_week_upload_speed_mbps_stddev,

    -- last month:
    last_month_test_count,
    last_month_download_speed_mbps_median,
    last_month_upload_speed_mbps_median,
    last_month_download_speed_mbps_avg,
    last_month_upload_speed_mbps_avg,
    last_month_download_speed_mbps_min,
    last_month_download_speed_mbps_max,
    last_month_upload_speed_mbps_min,
    last_month_upload_speed_mbps_max,
    last_month_download_speed_mbps_stddev,
    last_month_upload_speed_mbps_stddev,

    -- last year:
    last_year_test_count,
    last_year_download_speed_mbps_median,
    last_year_upload_speed_mbps_median,
    last_year_download_speed_mbps_avg,
    last_year_upload_speed_mbps_avg,
    last_year_download_speed_mbps_min,
    last_year_download_speed_mbps_max,
    last_year_upload_speed_mbps_min,
    last_year_upload_speed_mbps_max,
    last_year_download_speed_mbps_stddev,
    last_year_upload_speed_mbps_stddev
),

-- ============
-- Regions
-- ============
(SELECT

  REPLACE(LOWER(CONCAT(
    IFNULL(all.client_continent_code, ""),
    IFNULL(all.client_country_code, ""), "",
    IFNULL(all.client_region_code, ""), ""
  )), " ", "") as location_id,

  all.client_asn_number as client_asn_number,
  all.client_asn_name as client_asn_name,

  all.client_region as client_region,
  all.client_country as client_country,
  all.client_continent as client_continent,

  all.client_region_code as client_region_code,
  all.client_country_code as client_country_code,
  all.client_continent_code as client_continent_code,

  "region" as type,

  -- last week measurements
  lastweek.last_week_test_count as last_week_test_count,
  lastweek.download_speed_mbps_median as last_week_download_speed_mbps_median,
  lastweek.upload_speed_mbps_median as last_week_upload_speed_mbps_median,
  lastweek.download_speed_mbps_avg as last_week_download_speed_mbps_avg,
  lastweek.upload_speed_mbps_avg as last_week_upload_speed_mbps_avg,
  lastweek.download_speed_mbps_min as last_week_download_speed_mbps_min,
  lastweek.download_speed_mbps_max as last_week_download_speed_mbps_max,
  lastweek.upload_speed_mbps_min as last_week_upload_speed_mbps_min,
  lastweek.upload_speed_mbps_max as last_week_upload_speed_mbps_max,
  lastweek.download_speed_mbps_stddev as last_week_download_speed_mbps_stddev,
  lastweek.upload_speed_mbps_stddev as last_week_upload_speed_mbps_stddev,

  lastweek.download_speed_mbps_B4_8 as last_year_download_speed_mbps_B4_8,
  lastweek.download_speed_mbps_B8_12 as last_year_download_speed_mbps_B8_12,
  lastweek.download_speed_mbps_B12_16 as last_year_download_speed_mbps_B12_16,
  lastweek.download_speed_mbps_B16_20 as last_year_download_speed_mbps_B16_20,
  lastweek.download_speed_mbps_B20_24 as last_year_download_speed_mbps_B20_24,
  lastweek.download_speed_mbps_B24_28 as last_year_download_speed_mbps_B24_28,
  lastweek.download_speed_mbps_B28_32 as last_year_download_speed_mbps_B28_32,
  lastweek.download_speed_mbps_B32_36 as last_year_download_speed_mbps_B32_36,
  lastweek.download_speed_mbps_B36_40 as last_year_download_speed_mbps_B36_40,
  lastweek.download_speed_mbps_B40_44 as last_year_download_speed_mbps_B40_44,
  lastweek.download_speed_mbps_B44_48 as last_year_download_speed_mbps_B44_48,
  lastweek.download_speed_mbps_B48_52 as last_year_download_speed_mbps_B48_52,
  lastweek.download_speed_mbps_B52_56 as last_year_download_speed_mbps_B52_56,
  lastweek.download_speed_mbps_B56_60 as last_year_download_speed_mbps_B56_60,
  lastweek.download_speed_mbps_B60_64 as last_year_download_speed_mbps_B60_64,
  lastweek.download_speed_mbps_B64_68 as last_year_download_speed_mbps_B64_68,
  lastweek.download_speed_mbps_B68_72 as last_year_download_speed_mbps_B68_72,
  lastweek.download_speed_mbps_B72_76 as last_year_download_speed_mbps_B72_76,
  lastweek.download_speed_mbps_B76_80 as last_year_download_speed_mbps_B76_80,
  lastweek.download_speed_mbps_B80_84 as last_year_download_speed_mbps_B80_84,
  lastweek.download_speed_mbps_B84_88 as last_year_download_speed_mbps_B84_88,
  lastweek.download_speed_mbps_B88_92 as last_year_download_speed_mbps_B88_92,
  lastweek.download_speed_mbps_B92_96 as last_year_download_speed_mbps_B92_96,
  lastweek.download_speed_mbps_B96_100 as last_year_download_speed_mbps_B96_100,
  lastweek.download_speed_mbps_BGT100 as last_year_download_speed_mbps_BGT100,
  lastweek.upload_speed_mbps_B4_8 as last_year_upload_speed_mbps_B4_8,
  lastweek.upload_speed_mbps_B8_12 as last_year_upload_speed_mbps_B8_12,
  lastweek.upload_speed_mbps_B12_16 as last_year_upload_speed_mbps_B12_16,
  lastweek.upload_speed_mbps_B16_20 as last_year_upload_speed_mbps_B16_20,
  lastweek.upload_speed_mbps_B20_24 as last_year_upload_speed_mbps_B20_24,
  lastweek.upload_speed_mbps_B24_28 as last_year_upload_speed_mbps_B24_28,
  lastweek.upload_speed_mbps_B28_32 as last_year_upload_speed_mbps_B28_32,
  lastweek.upload_speed_mbps_B32_36 as last_year_upload_speed_mbps_B32_36,
  lastweek.upload_speed_mbps_B36_40 as last_year_upload_speed_mbps_B36_40,
  lastweek.upload_speed_mbps_B40_44 as last_year_upload_speed_mbps_B40_44,
  lastweek.upload_speed_mbps_B44_48 as last_year_upload_speed_mbps_B44_48,
  lastweek.upload_speed_mbps_B48_52 as last_year_upload_speed_mbps_B48_52,
  lastweek.upload_speed_mbps_B52_56 as last_year_upload_speed_mbps_B52_56,
  lastweek.upload_speed_mbps_B56_60 as last_year_upload_speed_mbps_B56_60,
  lastweek.upload_speed_mbps_B60_64 as last_year_upload_speed_mbps_B60_64,
  lastweek.upload_speed_mbps_B64_68 as last_year_upload_speed_mbps_B64_68,
  lastweek.upload_speed_mbps_B68_72 as last_year_upload_speed_mbps_B68_72,
  lastweek.upload_speed_mbps_B72_76 as last_year_upload_speed_mbps_B72_76,
  lastweek.upload_speed_mbps_B76_80 as last_year_upload_speed_mbps_B76_80,
  lastweek.upload_speed_mbps_B80_84 as last_year_upload_speed_mbps_B80_84,
  lastweek.upload_speed_mbps_B84_88 as last_year_upload_speed_mbps_B84_88,
  lastweek.upload_speed_mbps_B88_92 as last_year_upload_speed_mbps_B88_92,
  lastweek.upload_speed_mbps_B92_96 as last_year_upload_speed_mbps_B92_96,
  lastweek.upload_speed_mbps_B96_100 as last_year_upload_speed_mbps_B96_100,
  lastweek.upload_speed_mbps_BGT100 as last_year_upload_speed_mbps_BGT100,

  -- last month measurements
  lastmonth.last_month_test_count as last_month_test_count,
  lastmonth.download_speed_mbps_median as last_month_download_speed_mbps_median,
  lastmonth.upload_speed_mbps_median as last_month_upload_speed_mbps_median,
  lastmonth.download_speed_mbps_avg as last_month_download_speed_mbps_avg,
  lastmonth.upload_speed_mbps_avg as last_month_upload_speed_mbps_avg,
  lastmonth.download_speed_mbps_min as last_month_download_speed_mbps_min,
  lastmonth.download_speed_mbps_max as last_month_download_speed_mbps_max,
  lastmonth.upload_speed_mbps_min as last_month_upload_speed_mbps_min,
  lastmonth.upload_speed_mbps_max as last_month_upload_speed_mbps_max,
  lastmonth.download_speed_mbps_stddev as last_month_download_speed_mbps_stddev,
  lastmonth.upload_speed_mbps_stddev as last_month_upload_speed_mbps_stddev,

  lastmonth.download_speed_mbps_B4_8 as last_month_download_speed_mbps_B4_8,
  lastmonth.download_speed_mbps_B8_12 as last_month_download_speed_mbps_B8_12,
  lastmonth.download_speed_mbps_B12_16 as last_month_download_speed_mbps_B12_16,
  lastmonth.download_speed_mbps_B16_20 as last_month_download_speed_mbps_B16_20,
  lastmonth.download_speed_mbps_B20_24 as last_month_download_speed_mbps_B20_24,
  lastmonth.download_speed_mbps_B24_28 as last_month_download_speed_mbps_B24_28,
  lastmonth.download_speed_mbps_B28_32 as last_month_download_speed_mbps_B28_32,
  lastmonth.download_speed_mbps_B32_36 as last_month_download_speed_mbps_B32_36,
  lastmonth.download_speed_mbps_B36_40 as last_month_download_speed_mbps_B36_40,
  lastmonth.download_speed_mbps_B40_44 as last_month_download_speed_mbps_B40_44,
  lastmonth.download_speed_mbps_B44_48 as last_month_download_speed_mbps_B44_48,
  lastmonth.download_speed_mbps_B48_52 as last_month_download_speed_mbps_B48_52,
  lastmonth.download_speed_mbps_B52_56 as last_month_download_speed_mbps_B52_56,
  lastmonth.download_speed_mbps_B56_60 as last_month_download_speed_mbps_B56_60,
  lastmonth.download_speed_mbps_B60_64 as last_month_download_speed_mbps_B60_64,
  lastmonth.download_speed_mbps_B64_68 as last_month_download_speed_mbps_B64_68,
  lastmonth.download_speed_mbps_B68_72 as last_month_download_speed_mbps_B68_72,
  lastmonth.download_speed_mbps_B72_76 as last_month_download_speed_mbps_B72_76,
  lastmonth.download_speed_mbps_B76_80 as last_month_download_speed_mbps_B76_80,
  lastmonth.download_speed_mbps_B80_84 as last_month_download_speed_mbps_B80_84,
  lastmonth.download_speed_mbps_B84_88 as last_month_download_speed_mbps_B84_88,
  lastmonth.download_speed_mbps_B88_92 as last_month_download_speed_mbps_B88_92,
  lastmonth.download_speed_mbps_B92_96 as last_month_download_speed_mbps_B92_96,
  lastmonth.download_speed_mbps_B96_100 as last_month_download_speed_mbps_B96_100,
  lastmonth.download_speed_mbps_BGT100 as last_month_download_speed_mbps_BGT100,
  lastmonth.upload_speed_mbps_B4_8 as last_month_upload_speed_mbps_B4_8,
  lastmonth.upload_speed_mbps_B8_12 as last_month_upload_speed_mbps_B8_12,
  lastmonth.upload_speed_mbps_B12_16 as last_month_upload_speed_mbps_B12_16,
  lastmonth.upload_speed_mbps_B16_20 as last_month_upload_speed_mbps_B16_20,
  lastmonth.upload_speed_mbps_B20_24 as last_month_upload_speed_mbps_B20_24,
  lastmonth.upload_speed_mbps_B24_28 as last_month_upload_speed_mbps_B24_28,
  lastmonth.upload_speed_mbps_B28_32 as last_month_upload_speed_mbps_B28_32,
  lastmonth.upload_speed_mbps_B32_36 as last_month_upload_speed_mbps_B32_36,
  lastmonth.upload_speed_mbps_B36_40 as last_month_upload_speed_mbps_B36_40,
  lastmonth.upload_speed_mbps_B40_44 as last_month_upload_speed_mbps_B40_44,
  lastmonth.upload_speed_mbps_B44_48 as last_month_upload_speed_mbps_B44_48,
  lastmonth.upload_speed_mbps_B48_52 as last_month_upload_speed_mbps_B48_52,
  lastmonth.upload_speed_mbps_B52_56 as last_month_upload_speed_mbps_B52_56,
  lastmonth.upload_speed_mbps_B56_60 as last_month_upload_speed_mbps_B56_60,
  lastmonth.upload_speed_mbps_B60_64 as last_month_upload_speed_mbps_B60_64,
  lastmonth.upload_speed_mbps_B64_68 as last_month_upload_speed_mbps_B64_68,
  lastmonth.upload_speed_mbps_B68_72 as last_month_upload_speed_mbps_B68_72,
  lastmonth.upload_speed_mbps_B72_76 as last_month_upload_speed_mbps_B72_76,
  lastmonth.upload_speed_mbps_B76_80 as last_month_upload_speed_mbps_B76_80,
  lastmonth.upload_speed_mbps_B80_84 as last_month_upload_speed_mbps_B80_84,
  lastmonth.upload_speed_mbps_B84_88 as last_month_upload_speed_mbps_B84_88,
  lastmonth.upload_speed_mbps_B88_92 as last_month_upload_speed_mbps_B88_92,
  lastmonth.upload_speed_mbps_B92_96 as last_month_upload_speed_mbps_B92_96,
  lastmonth.upload_speed_mbps_B96_100 as last_month_upload_speed_mbps_B96_100,
  lastmonth.upload_speed_mbps_BGT100 as last_month_upload_speed_mbps_BGT100,

  -- last year measurements
  lastyear.last_year_test_count as last_year_test_count,
  lastyear.download_speed_mbps_median as last_year_download_speed_mbps_median,
  lastyear.upload_speed_mbps_median as last_year_upload_speed_mbps_median,
  lastyear.download_speed_mbps_avg as last_year_download_speed_mbps_avg,
  lastyear.upload_speed_mbps_avg as last_year_upload_speed_mbps_avg,
  lastyear.download_speed_mbps_min as last_year_download_speed_mbps_min,
  lastyear.download_speed_mbps_max as last_year_download_speed_mbps_max,
  lastyear.upload_speed_mbps_min as last_year_upload_speed_mbps_min,
  lastyear.upload_speed_mbps_max as last_year_upload_speed_mbps_max,
  lastyear.download_speed_mbps_stddev as last_year_download_speed_mbps_stddev,
  lastyear.upload_speed_mbps_stddev as last_year_upload_speed_mbps_stddev,

  lastyear.download_speed_mbps_B4_8 as last_year_download_speed_mbps_B4_8,
  lastyear.download_speed_mbps_B8_12 as last_year_download_speed_mbps_B8_12,
  lastyear.download_speed_mbps_B12_16 as last_year_download_speed_mbps_B12_16,
  lastyear.download_speed_mbps_B16_20 as last_year_download_speed_mbps_B16_20,
  lastyear.download_speed_mbps_B20_24 as last_year_download_speed_mbps_B20_24,
  lastyear.download_speed_mbps_B24_28 as last_year_download_speed_mbps_B24_28,
  lastyear.download_speed_mbps_B28_32 as last_year_download_speed_mbps_B28_32,
  lastyear.download_speed_mbps_B32_36 as last_year_download_speed_mbps_B32_36,
  lastyear.download_speed_mbps_B36_40 as last_year_download_speed_mbps_B36_40,
  lastyear.download_speed_mbps_B40_44 as last_year_download_speed_mbps_B40_44,
  lastyear.download_speed_mbps_B44_48 as last_year_download_speed_mbps_B44_48,
  lastyear.download_speed_mbps_B48_52 as last_year_download_speed_mbps_B48_52,
  lastyear.download_speed_mbps_B52_56 as last_year_download_speed_mbps_B52_56,
  lastyear.download_speed_mbps_B56_60 as last_year_download_speed_mbps_B56_60,
  lastyear.download_speed_mbps_B60_64 as last_year_download_speed_mbps_B60_64,
  lastyear.download_speed_mbps_B64_68 as last_year_download_speed_mbps_B64_68,
  lastyear.download_speed_mbps_B68_72 as last_year_download_speed_mbps_B68_72,
  lastyear.download_speed_mbps_B72_76 as last_year_download_speed_mbps_B72_76,
  lastyear.download_speed_mbps_B76_80 as last_year_download_speed_mbps_B76_80,
  lastyear.download_speed_mbps_B80_84 as last_year_download_speed_mbps_B80_84,
  lastyear.download_speed_mbps_B84_88 as last_year_download_speed_mbps_B84_88,
  lastyear.download_speed_mbps_B88_92 as last_year_download_speed_mbps_B88_92,
  lastyear.download_speed_mbps_B92_96 as last_year_download_speed_mbps_B92_96,
  lastyear.download_speed_mbps_B96_100 as last_year_download_speed_mbps_B96_100,
  lastyear.download_speed_mbps_BGT100 as last_year_download_speed_mbps_BGT100,
  lastyear.upload_speed_mbps_B4_8 as last_year_upload_speed_mbps_B4_8,
  lastyear.upload_speed_mbps_B8_12 as last_year_upload_speed_mbps_B8_12,
  lastyear.upload_speed_mbps_B12_16 as last_year_upload_speed_mbps_B12_16,
  lastyear.upload_speed_mbps_B16_20 as last_year_upload_speed_mbps_B16_20,
  lastyear.upload_speed_mbps_B20_24 as last_year_upload_speed_mbps_B20_24,
  lastyear.upload_speed_mbps_B24_28 as last_year_upload_speed_mbps_B24_28,
  lastyear.upload_speed_mbps_B28_32 as last_year_upload_speed_mbps_B28_32,
  lastyear.upload_speed_mbps_B32_36 as last_year_upload_speed_mbps_B32_36,
  lastyear.upload_speed_mbps_B36_40 as last_year_upload_speed_mbps_B36_40,
  lastyear.upload_speed_mbps_B40_44 as last_year_upload_speed_mbps_B40_44,
  lastyear.upload_speed_mbps_B44_48 as last_year_upload_speed_mbps_B44_48,
  lastyear.upload_speed_mbps_B48_52 as last_year_upload_speed_mbps_B48_52,
  lastyear.upload_speed_mbps_B52_56 as last_year_upload_speed_mbps_B52_56,
  lastyear.upload_speed_mbps_B56_60 as last_year_upload_speed_mbps_B56_60,
  lastyear.upload_speed_mbps_B60_64 as last_year_upload_speed_mbps_B60_64,
  lastyear.upload_speed_mbps_B64_68 as last_year_upload_speed_mbps_B64_68,
  lastyear.upload_speed_mbps_B68_72 as last_year_upload_speed_mbps_B68_72,
  lastyear.upload_speed_mbps_B72_76 as last_year_upload_speed_mbps_B72_76,
  lastyear.upload_speed_mbps_B76_80 as last_year_upload_speed_mbps_B76_80,
  lastyear.upload_speed_mbps_B80_84 as last_year_upload_speed_mbps_B80_84,
  lastyear.upload_speed_mbps_B84_88 as last_year_upload_speed_mbps_B84_88,
  lastyear.upload_speed_mbps_B88_92 as last_year_upload_speed_mbps_B88_92,
  lastyear.upload_speed_mbps_B92_96 as last_year_upload_speed_mbps_B92_96,
  lastyear.upload_speed_mbps_B96_100 as last_year_upload_speed_mbps_B96_100,
  lastyear.upload_speed_mbps_BGT100 as last_year_upload_speed_mbps_BGT100,

  FROM [mlab-oti:bocoup_prod.all_ip_by_day] all,

  left join
  (
    SELECT
      count(*) as last_week_test_count,

      client_region,
      client_country,
      client_continent,

      client_asn_number,
      client_asn_name,

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

      -- download bins
      SUM(IF(download_speed_mbps < 4 ,1,0)) AS download_speed_mbps_B0_4,
      SUM(IF(download_speed_mbps >= 4 AND download_speed_mbps < 8, 1,0)) AS download_speed_mbps_B4_8,
      SUM(IF(download_speed_mbps >= 8 AND download_speed_mbps < 12, 1,0)) AS download_speed_mbps_B8_12,
      SUM(IF(download_speed_mbps >= 12 AND download_speed_mbps < 16, 1,0)) AS download_speed_mbps_B12_16,
      SUM(IF(download_speed_mbps >= 16 AND download_speed_mbps < 20, 1,0)) AS download_speed_mbps_B16_20,
      SUM(IF(download_speed_mbps >= 20 AND download_speed_mbps < 24, 1,0)) AS download_speed_mbps_B20_24,
      SUM(IF(download_speed_mbps >= 24 AND download_speed_mbps < 28, 1,0)) AS download_speed_mbps_B24_28,
      SUM(IF(download_speed_mbps >= 28 AND download_speed_mbps < 32, 1,0)) AS download_speed_mbps_B28_32,
      SUM(IF(download_speed_mbps >= 32 AND download_speed_mbps < 36, 1,0)) AS download_speed_mbps_B32_36,
      SUM(IF(download_speed_mbps >= 36 AND download_speed_mbps < 40, 1,0)) AS download_speed_mbps_B36_40,
      SUM(IF(download_speed_mbps >= 40 AND download_speed_mbps < 44, 1,0)) AS download_speed_mbps_B40_44,
      SUM(IF(download_speed_mbps >= 44 AND download_speed_mbps < 48, 1,0)) AS download_speed_mbps_B44_48,
      SUM(IF(download_speed_mbps >= 48 AND download_speed_mbps < 52, 1,0)) AS download_speed_mbps_B48_52,
      SUM(IF(download_speed_mbps >= 52 AND download_speed_mbps < 56, 1,0)) AS download_speed_mbps_B52_56,
      SUM(IF(download_speed_mbps >= 56 AND download_speed_mbps < 60, 1,0)) AS download_speed_mbps_B56_60,
      SUM(IF(download_speed_mbps >= 60 AND download_speed_mbps < 64, 1,0)) AS download_speed_mbps_B60_64,
      SUM(IF(download_speed_mbps >= 64 AND download_speed_mbps < 68, 1,0)) AS download_speed_mbps_B64_68,
      SUM(IF(download_speed_mbps >= 68 AND download_speed_mbps < 72, 1,0)) AS download_speed_mbps_B68_72,
      SUM(IF(download_speed_mbps >= 72 AND download_speed_mbps < 76, 1,0)) AS download_speed_mbps_B72_76,
      SUM(IF(download_speed_mbps >= 76 AND download_speed_mbps < 80, 1,0)) AS download_speed_mbps_B76_80,
      SUM(IF(download_speed_mbps >= 80 AND download_speed_mbps < 84, 1,0)) AS download_speed_mbps_B80_84,
      SUM(IF(download_speed_mbps >= 84 AND download_speed_mbps < 88, 1,0)) AS download_speed_mbps_B84_88,
      SUM(IF(download_speed_mbps >= 88 AND download_speed_mbps < 92, 1,0)) AS download_speed_mbps_B88_92,
      SUM(IF(download_speed_mbps >= 92 AND download_speed_mbps < 96, 1,0)) AS download_speed_mbps_B92_96,
      SUM(IF(download_speed_mbps >= 96 AND download_speed_mbps < 100, 1,0)) AS download_speed_mbps_B96_100,
      SUM(IF(download_speed_mbps >= 100, 1,0)) AS download_speed_mbps_BGT100,

      -- upload bins
      SUM(IF(upload_speed_mbps < 4 ,1,0)) AS upload_speed_mbps_B0_4,
      SUM(IF(upload_speed_mbps >= 4 AND upload_speed_mbps < 8, 1,0)) AS upload_speed_mbps_B4_8,
      SUM(IF(upload_speed_mbps >= 8 AND upload_speed_mbps < 12, 1,0)) AS upload_speed_mbps_B8_12,
      SUM(IF(upload_speed_mbps >= 12 AND upload_speed_mbps < 16, 1,0)) AS upload_speed_mbps_B12_16,
      SUM(IF(upload_speed_mbps >= 16 AND upload_speed_mbps < 20, 1,0)) AS upload_speed_mbps_B16_20,
      SUM(IF(upload_speed_mbps >= 20 AND upload_speed_mbps < 24, 1,0)) AS upload_speed_mbps_B20_24,
      SUM(IF(upload_speed_mbps >= 24 AND upload_speed_mbps < 28, 1,0)) AS upload_speed_mbps_B24_28,
      SUM(IF(upload_speed_mbps >= 28 AND upload_speed_mbps < 32, 1,0)) AS upload_speed_mbps_B28_32,
      SUM(IF(upload_speed_mbps >= 32 AND upload_speed_mbps < 36, 1,0)) AS upload_speed_mbps_B32_36,
      SUM(IF(upload_speed_mbps >= 36 AND upload_speed_mbps < 40, 1,0)) AS upload_speed_mbps_B36_40,
      SUM(IF(upload_speed_mbps >= 40 AND upload_speed_mbps < 44, 1,0)) AS upload_speed_mbps_B40_44,
      SUM(IF(upload_speed_mbps >= 44 AND upload_speed_mbps < 48, 1,0)) AS upload_speed_mbps_B44_48,
      SUM(IF(upload_speed_mbps >= 48 AND upload_speed_mbps < 52, 1,0)) AS upload_speed_mbps_B48_52,
      SUM(IF(upload_speed_mbps >= 52 AND upload_speed_mbps < 56, 1,0)) AS upload_speed_mbps_B52_56,
      SUM(IF(upload_speed_mbps >= 56 AND upload_speed_mbps < 60, 1,0)) AS upload_speed_mbps_B56_60,
      SUM(IF(upload_speed_mbps >= 60 AND upload_speed_mbps < 64, 1,0)) AS upload_speed_mbps_B60_64,
      SUM(IF(upload_speed_mbps >= 64 AND upload_speed_mbps < 68, 1,0)) AS upload_speed_mbps_B64_68,
      SUM(IF(upload_speed_mbps >= 68 AND upload_speed_mbps < 72, 1,0)) AS upload_speed_mbps_B68_72,
      SUM(IF(upload_speed_mbps >= 72 AND upload_speed_mbps < 76, 1,0)) AS upload_speed_mbps_B72_76,
      SUM(IF(upload_speed_mbps >= 76 AND upload_speed_mbps < 80, 1,0)) AS upload_speed_mbps_B76_80,
      SUM(IF(upload_speed_mbps >= 80 AND upload_speed_mbps < 84, 1,0)) AS upload_speed_mbps_B80_84,
      SUM(IF(upload_speed_mbps >= 84 AND upload_speed_mbps < 88, 1,0)) AS upload_speed_mbps_B84_88,
      SUM(IF(upload_speed_mbps >= 88 AND upload_speed_mbps < 92, 1,0)) AS upload_speed_mbps_B88_92,
      SUM(IF(upload_speed_mbps >= 92 AND upload_speed_mbps < 96, 1,0)) AS upload_speed_mbps_B92_96,
      SUM(IF(upload_speed_mbps >= 96 AND upload_speed_mbps < 100, 1,0)) AS upload_speed_mbps_B96_100,
      SUM(IF(upload_speed_mbps >= 100, 1,0)) AS upload_speed_mbps_BGT100,

    from [mlab-oti:bocoup_prod.all_ip_by_day]
    where
      test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, "DAY") and
      client_asn_number is not null and
      client_asn_number is not null

    group by
      client_asn_number,
      client_asn_name,
      client_region,
      client_country,
      client_continent

  ) lastweek on
    all.client_region = lastweek.client_region and
    all.client_country = lastweek.client_country and
    all.client_continent = lastweek.client_continent and
    all.client_asn_number = lastweek.client_asn_number and
    all.client_asn_name = lastweek.client_asn_name

  -- Compute metrics for the current month
  left join
  (
    SELECT
      count(*) as last_month_test_count,
      client_region,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name,

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

      -- download bins
      SUM(IF(download_speed_mbps < 4 ,1,0)) AS download_speed_mbps_B0_4,
      SUM(IF(download_speed_mbps >= 4 AND download_speed_mbps < 8, 1,0)) AS download_speed_mbps_B4_8,
      SUM(IF(download_speed_mbps >= 8 AND download_speed_mbps < 12, 1,0)) AS download_speed_mbps_B8_12,
      SUM(IF(download_speed_mbps >= 12 AND download_speed_mbps < 16, 1,0)) AS download_speed_mbps_B12_16,
      SUM(IF(download_speed_mbps >= 16 AND download_speed_mbps < 20, 1,0)) AS download_speed_mbps_B16_20,
      SUM(IF(download_speed_mbps >= 20 AND download_speed_mbps < 24, 1,0)) AS download_speed_mbps_B20_24,
      SUM(IF(download_speed_mbps >= 24 AND download_speed_mbps < 28, 1,0)) AS download_speed_mbps_B24_28,
      SUM(IF(download_speed_mbps >= 28 AND download_speed_mbps < 32, 1,0)) AS download_speed_mbps_B28_32,
      SUM(IF(download_speed_mbps >= 32 AND download_speed_mbps < 36, 1,0)) AS download_speed_mbps_B32_36,
      SUM(IF(download_speed_mbps >= 36 AND download_speed_mbps < 40, 1,0)) AS download_speed_mbps_B36_40,
      SUM(IF(download_speed_mbps >= 40 AND download_speed_mbps < 44, 1,0)) AS download_speed_mbps_B40_44,
      SUM(IF(download_speed_mbps >= 44 AND download_speed_mbps < 48, 1,0)) AS download_speed_mbps_B44_48,
      SUM(IF(download_speed_mbps >= 48 AND download_speed_mbps < 52, 1,0)) AS download_speed_mbps_B48_52,
      SUM(IF(download_speed_mbps >= 52 AND download_speed_mbps < 56, 1,0)) AS download_speed_mbps_B52_56,
      SUM(IF(download_speed_mbps >= 56 AND download_speed_mbps < 60, 1,0)) AS download_speed_mbps_B56_60,
      SUM(IF(download_speed_mbps >= 60 AND download_speed_mbps < 64, 1,0)) AS download_speed_mbps_B60_64,
      SUM(IF(download_speed_mbps >= 64 AND download_speed_mbps < 68, 1,0)) AS download_speed_mbps_B64_68,
      SUM(IF(download_speed_mbps >= 68 AND download_speed_mbps < 72, 1,0)) AS download_speed_mbps_B68_72,
      SUM(IF(download_speed_mbps >= 72 AND download_speed_mbps < 76, 1,0)) AS download_speed_mbps_B72_76,
      SUM(IF(download_speed_mbps >= 76 AND download_speed_mbps < 80, 1,0)) AS download_speed_mbps_B76_80,
      SUM(IF(download_speed_mbps >= 80 AND download_speed_mbps < 84, 1,0)) AS download_speed_mbps_B80_84,
      SUM(IF(download_speed_mbps >= 84 AND download_speed_mbps < 88, 1,0)) AS download_speed_mbps_B84_88,
      SUM(IF(download_speed_mbps >= 88 AND download_speed_mbps < 92, 1,0)) AS download_speed_mbps_B88_92,
      SUM(IF(download_speed_mbps >= 92 AND download_speed_mbps < 96, 1,0)) AS download_speed_mbps_B92_96,
      SUM(IF(download_speed_mbps >= 96 AND download_speed_mbps < 100, 1,0)) AS download_speed_mbps_B96_100,
      SUM(IF(download_speed_mbps >= 100, 1,0)) AS download_speed_mbps_BGT100,

      -- upload bins
      SUM(IF(upload_speed_mbps < 4 ,1,0)) AS upload_speed_mbps_B0_4,
      SUM(IF(upload_speed_mbps >= 4 AND upload_speed_mbps < 8, 1,0)) AS upload_speed_mbps_B4_8,
      SUM(IF(upload_speed_mbps >= 8 AND upload_speed_mbps < 12, 1,0)) AS upload_speed_mbps_B8_12,
      SUM(IF(upload_speed_mbps >= 12 AND upload_speed_mbps < 16, 1,0)) AS upload_speed_mbps_B12_16,
      SUM(IF(upload_speed_mbps >= 16 AND upload_speed_mbps < 20, 1,0)) AS upload_speed_mbps_B16_20,
      SUM(IF(upload_speed_mbps >= 20 AND upload_speed_mbps < 24, 1,0)) AS upload_speed_mbps_B20_24,
      SUM(IF(upload_speed_mbps >= 24 AND upload_speed_mbps < 28, 1,0)) AS upload_speed_mbps_B24_28,
      SUM(IF(upload_speed_mbps >= 28 AND upload_speed_mbps < 32, 1,0)) AS upload_speed_mbps_B28_32,
      SUM(IF(upload_speed_mbps >= 32 AND upload_speed_mbps < 36, 1,0)) AS upload_speed_mbps_B32_36,
      SUM(IF(upload_speed_mbps >= 36 AND upload_speed_mbps < 40, 1,0)) AS upload_speed_mbps_B36_40,
      SUM(IF(upload_speed_mbps >= 40 AND upload_speed_mbps < 44, 1,0)) AS upload_speed_mbps_B40_44,
      SUM(IF(upload_speed_mbps >= 44 AND upload_speed_mbps < 48, 1,0)) AS upload_speed_mbps_B44_48,
      SUM(IF(upload_speed_mbps >= 48 AND upload_speed_mbps < 52, 1,0)) AS upload_speed_mbps_B48_52,
      SUM(IF(upload_speed_mbps >= 52 AND upload_speed_mbps < 56, 1,0)) AS upload_speed_mbps_B52_56,
      SUM(IF(upload_speed_mbps >= 56 AND upload_speed_mbps < 60, 1,0)) AS upload_speed_mbps_B56_60,
      SUM(IF(upload_speed_mbps >= 60 AND upload_speed_mbps < 64, 1,0)) AS upload_speed_mbps_B60_64,
      SUM(IF(upload_speed_mbps >= 64 AND upload_speed_mbps < 68, 1,0)) AS upload_speed_mbps_B64_68,
      SUM(IF(upload_speed_mbps >= 68 AND upload_speed_mbps < 72, 1,0)) AS upload_speed_mbps_B68_72,
      SUM(IF(upload_speed_mbps >= 72 AND upload_speed_mbps < 76, 1,0)) AS upload_speed_mbps_B72_76,
      SUM(IF(upload_speed_mbps >= 76 AND upload_speed_mbps < 80, 1,0)) AS upload_speed_mbps_B76_80,
      SUM(IF(upload_speed_mbps >= 80 AND upload_speed_mbps < 84, 1,0)) AS upload_speed_mbps_B80_84,
      SUM(IF(upload_speed_mbps >= 84 AND upload_speed_mbps < 88, 1,0)) AS upload_speed_mbps_B84_88,
      SUM(IF(upload_speed_mbps >= 88 AND upload_speed_mbps < 92, 1,0)) AS upload_speed_mbps_B88_92,
      SUM(IF(upload_speed_mbps >= 92 AND upload_speed_mbps < 96, 1,0)) AS upload_speed_mbps_B92_96,
      SUM(IF(upload_speed_mbps >= 96 AND upload_speed_mbps < 100, 1,0)) AS upload_speed_mbps_B96_100,
      SUM(IF(upload_speed_mbps >= 100, 1,0)) AS upload_speed_mbps_BGT100,

    from [mlab-oti:bocoup_prod.all_ip_by_day]
    -- current month:
    where
      test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH") and
      client_asn_number is not null and
      client_asn_number is not null

    group by
      client_region,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name
  ) lastmonth on
    all.client_region = lastmonth.client_region and
    all.client_country = lastmonth.client_country and
    all.client_continent = lastmonth.client_continent and
    all.client_asn_number = lastmonth.client_asn_number and
    all.client_asn_name = lastmonth.client_asn_name

  -- Compute metrics for the current year
  left join
  (
    SELECT
      count(*) as last_year_test_count,
      client_region,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name,

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

      -- download bins
      SUM(IF(download_speed_mbps < 4 ,1,0)) AS download_speed_mbps_B0_4,
      SUM(IF(download_speed_mbps >= 4 AND download_speed_mbps < 8, 1,0)) AS download_speed_mbps_B4_8,
      SUM(IF(download_speed_mbps >= 8 AND download_speed_mbps < 12, 1,0)) AS download_speed_mbps_B8_12,
      SUM(IF(download_speed_mbps >= 12 AND download_speed_mbps < 16, 1,0)) AS download_speed_mbps_B12_16,
      SUM(IF(download_speed_mbps >= 16 AND download_speed_mbps < 20, 1,0)) AS download_speed_mbps_B16_20,
      SUM(IF(download_speed_mbps >= 20 AND download_speed_mbps < 24, 1,0)) AS download_speed_mbps_B20_24,
      SUM(IF(download_speed_mbps >= 24 AND download_speed_mbps < 28, 1,0)) AS download_speed_mbps_B24_28,
      SUM(IF(download_speed_mbps >= 28 AND download_speed_mbps < 32, 1,0)) AS download_speed_mbps_B28_32,
      SUM(IF(download_speed_mbps >= 32 AND download_speed_mbps < 36, 1,0)) AS download_speed_mbps_B32_36,
      SUM(IF(download_speed_mbps >= 36 AND download_speed_mbps < 40, 1,0)) AS download_speed_mbps_B36_40,
      SUM(IF(download_speed_mbps >= 40 AND download_speed_mbps < 44, 1,0)) AS download_speed_mbps_B40_44,
      SUM(IF(download_speed_mbps >= 44 AND download_speed_mbps < 48, 1,0)) AS download_speed_mbps_B44_48,
      SUM(IF(download_speed_mbps >= 48 AND download_speed_mbps < 52, 1,0)) AS download_speed_mbps_B48_52,
      SUM(IF(download_speed_mbps >= 52 AND download_speed_mbps < 56, 1,0)) AS download_speed_mbps_B52_56,
      SUM(IF(download_speed_mbps >= 56 AND download_speed_mbps < 60, 1,0)) AS download_speed_mbps_B56_60,
      SUM(IF(download_speed_mbps >= 60 AND download_speed_mbps < 64, 1,0)) AS download_speed_mbps_B60_64,
      SUM(IF(download_speed_mbps >= 64 AND download_speed_mbps < 68, 1,0)) AS download_speed_mbps_B64_68,
      SUM(IF(download_speed_mbps >= 68 AND download_speed_mbps < 72, 1,0)) AS download_speed_mbps_B68_72,
      SUM(IF(download_speed_mbps >= 72 AND download_speed_mbps < 76, 1,0)) AS download_speed_mbps_B72_76,
      SUM(IF(download_speed_mbps >= 76 AND download_speed_mbps < 80, 1,0)) AS download_speed_mbps_B76_80,
      SUM(IF(download_speed_mbps >= 80 AND download_speed_mbps < 84, 1,0)) AS download_speed_mbps_B80_84,
      SUM(IF(download_speed_mbps >= 84 AND download_speed_mbps < 88, 1,0)) AS download_speed_mbps_B84_88,
      SUM(IF(download_speed_mbps >= 88 AND download_speed_mbps < 92, 1,0)) AS download_speed_mbps_B88_92,
      SUM(IF(download_speed_mbps >= 92 AND download_speed_mbps < 96, 1,0)) AS download_speed_mbps_B92_96,
      SUM(IF(download_speed_mbps >= 96 AND download_speed_mbps < 100, 1,0)) AS download_speed_mbps_B96_100,
      SUM(IF(download_speed_mbps >= 100, 1,0)) AS download_speed_mbps_BGT100,

      -- upload bins
      SUM(IF(upload_speed_mbps < 4 ,1,0)) AS upload_speed_mbps_B0_4,
      SUM(IF(upload_speed_mbps >= 4 AND upload_speed_mbps < 8, 1,0)) AS upload_speed_mbps_B4_8,
      SUM(IF(upload_speed_mbps >= 8 AND upload_speed_mbps < 12, 1,0)) AS upload_speed_mbps_B8_12,
      SUM(IF(upload_speed_mbps >= 12 AND upload_speed_mbps < 16, 1,0)) AS upload_speed_mbps_B12_16,
      SUM(IF(upload_speed_mbps >= 16 AND upload_speed_mbps < 20, 1,0)) AS upload_speed_mbps_B16_20,
      SUM(IF(upload_speed_mbps >= 20 AND upload_speed_mbps < 24, 1,0)) AS upload_speed_mbps_B20_24,
      SUM(IF(upload_speed_mbps >= 24 AND upload_speed_mbps < 28, 1,0)) AS upload_speed_mbps_B24_28,
      SUM(IF(upload_speed_mbps >= 28 AND upload_speed_mbps < 32, 1,0)) AS upload_speed_mbps_B28_32,
      SUM(IF(upload_speed_mbps >= 32 AND upload_speed_mbps < 36, 1,0)) AS upload_speed_mbps_B32_36,
      SUM(IF(upload_speed_mbps >= 36 AND upload_speed_mbps < 40, 1,0)) AS upload_speed_mbps_B36_40,
      SUM(IF(upload_speed_mbps >= 40 AND upload_speed_mbps < 44, 1,0)) AS upload_speed_mbps_B40_44,
      SUM(IF(upload_speed_mbps >= 44 AND upload_speed_mbps < 48, 1,0)) AS upload_speed_mbps_B44_48,
      SUM(IF(upload_speed_mbps >= 48 AND upload_speed_mbps < 52, 1,0)) AS upload_speed_mbps_B48_52,
      SUM(IF(upload_speed_mbps >= 52 AND upload_speed_mbps < 56, 1,0)) AS upload_speed_mbps_B52_56,
      SUM(IF(upload_speed_mbps >= 56 AND upload_speed_mbps < 60, 1,0)) AS upload_speed_mbps_B56_60,
      SUM(IF(upload_speed_mbps >= 60 AND upload_speed_mbps < 64, 1,0)) AS upload_speed_mbps_B60_64,
      SUM(IF(upload_speed_mbps >= 64 AND upload_speed_mbps < 68, 1,0)) AS upload_speed_mbps_B64_68,
      SUM(IF(upload_speed_mbps >= 68 AND upload_speed_mbps < 72, 1,0)) AS upload_speed_mbps_B68_72,
      SUM(IF(upload_speed_mbps >= 72 AND upload_speed_mbps < 76, 1,0)) AS upload_speed_mbps_B72_76,
      SUM(IF(upload_speed_mbps >= 76 AND upload_speed_mbps < 80, 1,0)) AS upload_speed_mbps_B76_80,
      SUM(IF(upload_speed_mbps >= 80 AND upload_speed_mbps < 84, 1,0)) AS upload_speed_mbps_B80_84,
      SUM(IF(upload_speed_mbps >= 84 AND upload_speed_mbps < 88, 1,0)) AS upload_speed_mbps_B84_88,
      SUM(IF(upload_speed_mbps >= 88 AND upload_speed_mbps < 92, 1,0)) AS upload_speed_mbps_B88_92,
      SUM(IF(upload_speed_mbps >= 92 AND upload_speed_mbps < 96, 1,0)) AS upload_speed_mbps_B92_96,
      SUM(IF(upload_speed_mbps >= 96 AND upload_speed_mbps < 100, 1,0)) AS upload_speed_mbps_B96_100,
      SUM(IF(upload_speed_mbps >= 100, 1,0)) AS upload_speed_mbps_BGT100,

    from [mlab-oti:bocoup_prod.all_ip_by_day]
    -- current year:
    where
      test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR") and
      client_asn_number is not null and
      client_asn_number is not null
    group by
      client_region,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name

  ) lastyear on
    all.client_region = lastyear.client_region and
    all.client_country = lastyear.client_country and
    all.client_continent = lastyear.client_continent and
    all.client_asn_number = lastyear.client_asn_number and
    all.client_asn_name = lastyear.client_asn_name

  GROUP BY
    -- key fields:
    location_id,
    client_asn_number,

    -- metadata:
    client_asn_name,
    type,

    client_region,
    client_region_code,
    client_country,
    client_country_code,
    client_continent,
    client_continent_code,

    -- last week:
    last_week_test_count,
    last_week_download_speed_mbps_median,
    last_week_upload_speed_mbps_median,
    last_week_download_speed_mbps_avg,
    last_week_upload_speed_mbps_avg,
    last_week_download_speed_mbps_min,
    last_week_download_speed_mbps_max,
    last_week_upload_speed_mbps_min,
    last_week_upload_speed_mbps_max,
    last_week_download_speed_mbps_stddev,
    last_week_upload_speed_mbps_stddev,

    -- last month:
    last_month_test_count,
    last_month_download_speed_mbps_median,
    last_month_upload_speed_mbps_median,
    last_month_download_speed_mbps_avg,
    last_month_upload_speed_mbps_avg,
    last_month_download_speed_mbps_min,
    last_month_download_speed_mbps_max,
    last_month_upload_speed_mbps_min,
    last_month_upload_speed_mbps_max,
    last_month_download_speed_mbps_stddev,
    last_month_upload_speed_mbps_stddev,

    -- last year:
    last_year_test_count,
    last_year_download_speed_mbps_median,
    last_year_upload_speed_mbps_median,
    last_year_download_speed_mbps_avg,
    last_year_upload_speed_mbps_avg,
    last_year_download_speed_mbps_min,
    last_year_download_speed_mbps_max,
    last_year_upload_speed_mbps_min,
    last_year_upload_speed_mbps_max,
    last_year_download_speed_mbps_stddev,
    last_year_upload_speed_mbps_stddev
),

-- ============
-- Country
-- ============
(SELECT

  REPLACE(LOWER(CONCAT(
    IFNULL(all.client_continent_code, ""),
    IFNULL(all.client_country_code, ""), ""
  )), " ", "") as location_id,

  all.client_asn_number as client_asn_number,
  all.client_asn_name as client_asn_name,

  all.client_country as client_country,
  all.client_continent as client_continent,

  all.client_country_code as client_country_code,
  all.client_continent_code as client_continent_code,

  "country" as type,

  -- last week measurements
  lastweek.last_week_test_count as last_week_test_count,
  lastweek.download_speed_mbps_median as last_week_download_speed_mbps_median,
  lastweek.upload_speed_mbps_median as last_week_upload_speed_mbps_median,
  lastweek.download_speed_mbps_avg as last_week_download_speed_mbps_avg,
  lastweek.upload_speed_mbps_avg as last_week_upload_speed_mbps_avg,
  lastweek.download_speed_mbps_min as last_week_download_speed_mbps_min,
  lastweek.download_speed_mbps_max as last_week_download_speed_mbps_max,
  lastweek.upload_speed_mbps_min as last_week_upload_speed_mbps_min,
  lastweek.upload_speed_mbps_max as last_week_upload_speed_mbps_max,
  lastweek.download_speed_mbps_stddev as last_week_download_speed_mbps_stddev,
  lastweek.upload_speed_mbps_stddev as last_week_upload_speed_mbps_stddev,

   lastweek.download_speed_mbps_B4_8 as last_week_download_speed_mbps_B4_8,
  lastweek.download_speed_mbps_B8_12 as last_week_download_speed_mbps_B8_12,
  lastweek.download_speed_mbps_B12_16 as last_week_download_speed_mbps_B12_16,
  lastweek.download_speed_mbps_B16_20 as last_week_download_speed_mbps_B16_20,
  lastweek.download_speed_mbps_B20_24 as last_week_download_speed_mbps_B20_24,
  lastweek.download_speed_mbps_B24_28 as last_week_download_speed_mbps_B24_28,
  lastweek.download_speed_mbps_B28_32 as last_week_download_speed_mbps_B28_32,
  lastweek.download_speed_mbps_B32_36 as last_week_download_speed_mbps_B32_36,
  lastweek.download_speed_mbps_B36_40 as last_week_download_speed_mbps_B36_40,
  lastweek.download_speed_mbps_B40_44 as last_week_download_speed_mbps_B40_44,
  lastweek.download_speed_mbps_B44_48 as last_week_download_speed_mbps_B44_48,
  lastweek.download_speed_mbps_B48_52 as last_week_download_speed_mbps_B48_52,
  lastweek.download_speed_mbps_B52_56 as last_week_download_speed_mbps_B52_56,
  lastweek.download_speed_mbps_B56_60 as last_week_download_speed_mbps_B56_60,
  lastweek.download_speed_mbps_B60_64 as last_week_download_speed_mbps_B60_64,
  lastweek.download_speed_mbps_B64_68 as last_week_download_speed_mbps_B64_68,
  lastweek.download_speed_mbps_B68_72 as last_week_download_speed_mbps_B68_72,
  lastweek.download_speed_mbps_B72_76 as last_week_download_speed_mbps_B72_76,
  lastweek.download_speed_mbps_B76_80 as last_week_download_speed_mbps_B76_80,
  lastweek.download_speed_mbps_B80_84 as last_week_download_speed_mbps_B80_84,
  lastweek.download_speed_mbps_B84_88 as last_week_download_speed_mbps_B84_88,
  lastweek.download_speed_mbps_B88_92 as last_week_download_speed_mbps_B88_92,
  lastweek.download_speed_mbps_B92_96 as last_week_download_speed_mbps_B92_96,
  lastweek.download_speed_mbps_B96_100 as last_week_download_speed_mbps_B96_100,
  lastweek.download_speed_mbps_BGT100 as last_week_download_speed_mbps_BGT100,

  lastweek.upload_speed_mbps_B4_8 as last_week_upload_speed_mbps_B4_8,
  lastweek.upload_speed_mbps_B8_12 as last_week_upload_speed_mbps_B8_12,
  lastweek.upload_speed_mbps_B12_16 as last_week_upload_speed_mbps_B12_16,
  lastweek.upload_speed_mbps_B16_20 as last_week_upload_speed_mbps_B16_20,
  lastweek.upload_speed_mbps_B20_24 as last_week_upload_speed_mbps_B20_24,
  lastweek.upload_speed_mbps_B24_28 as last_week_upload_speed_mbps_B24_28,
  lastweek.upload_speed_mbps_B28_32 as last_week_upload_speed_mbps_B28_32,
  lastweek.upload_speed_mbps_B32_36 as last_week_upload_speed_mbps_B32_36,
  lastweek.upload_speed_mbps_B36_40 as last_week_upload_speed_mbps_B36_40,
  lastweek.upload_speed_mbps_B40_44 as last_week_upload_speed_mbps_B40_44,
  lastweek.upload_speed_mbps_B44_48 as last_week_upload_speed_mbps_B44_48,
  lastweek.upload_speed_mbps_B48_52 as last_week_upload_speed_mbps_B48_52,
  lastweek.upload_speed_mbps_B52_56 as last_week_upload_speed_mbps_B52_56,
  lastweek.upload_speed_mbps_B56_60 as last_week_upload_speed_mbps_B56_60,
  lastweek.upload_speed_mbps_B60_64 as last_week_upload_speed_mbps_B60_64,
  lastweek.upload_speed_mbps_B64_68 as last_week_upload_speed_mbps_B64_68,
  lastweek.upload_speed_mbps_B68_72 as last_week_upload_speed_mbps_B68_72,
  lastweek.upload_speed_mbps_B72_76 as last_week_upload_speed_mbps_B72_76,
  lastweek.upload_speed_mbps_B76_80 as last_week_upload_speed_mbps_B76_80,
  lastweek.upload_speed_mbps_B80_84 as last_week_upload_speed_mbps_B80_84,
  lastweek.upload_speed_mbps_B84_88 as last_week_upload_speed_mbps_B84_88,
  lastweek.upload_speed_mbps_B88_92 as last_week_upload_speed_mbps_B88_92,
  lastweek.upload_speed_mbps_B92_96 as last_week_upload_speed_mbps_B92_96,
  lastweek.upload_speed_mbps_B96_100 as last_week_upload_speed_mbps_B96_100,
  lastweek.upload_speed_mbps_BGT100 as last_week_upload_speed_mbps_BGT100,

  -- last month measurements
  lastmonth.last_month_test_count as last_month_test_count,
  lastmonth.download_speed_mbps_median as last_month_download_speed_mbps_median,
  lastmonth.upload_speed_mbps_median as last_month_upload_speed_mbps_median,
  lastmonth.download_speed_mbps_avg as last_month_download_speed_mbps_avg,
  lastmonth.upload_speed_mbps_avg as last_month_upload_speed_mbps_avg,
  lastmonth.download_speed_mbps_min as last_month_download_speed_mbps_min,
  lastmonth.download_speed_mbps_max as last_month_download_speed_mbps_max,
  lastmonth.upload_speed_mbps_min as last_month_upload_speed_mbps_min,
  lastmonth.upload_speed_mbps_max as last_month_upload_speed_mbps_max,
  lastmonth.download_speed_mbps_stddev as last_month_download_speed_mbps_stddev,
  lastmonth.upload_speed_mbps_stddev as last_month_upload_speed_mbps_stddev,

  lastmonth.download_speed_mbps_B4_8 as last_month_download_speed_mbps_B4_8,
  lastmonth.download_speed_mbps_B8_12 as last_month_download_speed_mbps_B8_12,
  lastmonth.download_speed_mbps_B12_16 as last_month_download_speed_mbps_B12_16,
  lastmonth.download_speed_mbps_B16_20 as last_month_download_speed_mbps_B16_20,
  lastmonth.download_speed_mbps_B20_24 as last_month_download_speed_mbps_B20_24,
  lastmonth.download_speed_mbps_B24_28 as last_month_download_speed_mbps_B24_28,
  lastmonth.download_speed_mbps_B28_32 as last_month_download_speed_mbps_B28_32,
  lastmonth.download_speed_mbps_B32_36 as last_month_download_speed_mbps_B32_36,
  lastmonth.download_speed_mbps_B36_40 as last_month_download_speed_mbps_B36_40,
  lastmonth.download_speed_mbps_B40_44 as last_month_download_speed_mbps_B40_44,
  lastmonth.download_speed_mbps_B44_48 as last_month_download_speed_mbps_B44_48,
  lastmonth.download_speed_mbps_B48_52 as last_month_download_speed_mbps_B48_52,
  lastmonth.download_speed_mbps_B52_56 as last_month_download_speed_mbps_B52_56,
  lastmonth.download_speed_mbps_B56_60 as last_month_download_speed_mbps_B56_60,
  lastmonth.download_speed_mbps_B60_64 as last_month_download_speed_mbps_B60_64,
  lastmonth.download_speed_mbps_B64_68 as last_month_download_speed_mbps_B64_68,
  lastmonth.download_speed_mbps_B68_72 as last_month_download_speed_mbps_B68_72,
  lastmonth.download_speed_mbps_B72_76 as last_month_download_speed_mbps_B72_76,
  lastmonth.download_speed_mbps_B76_80 as last_month_download_speed_mbps_B76_80,
  lastmonth.download_speed_mbps_B80_84 as last_month_download_speed_mbps_B80_84,
  lastmonth.download_speed_mbps_B84_88 as last_month_download_speed_mbps_B84_88,
  lastmonth.download_speed_mbps_B88_92 as last_month_download_speed_mbps_B88_92,
  lastmonth.download_speed_mbps_B92_96 as last_month_download_speed_mbps_B92_96,
  lastmonth.download_speed_mbps_B96_100 as last_month_download_speed_mbps_B96_100,
  lastmonth.download_speed_mbps_BGT100 as last_month_download_speed_mbps_BGT100,

  lastmonth.upload_speed_mbps_B4_8 as last_month_upload_speed_mbps_B4_8,
  lastmonth.upload_speed_mbps_B8_12 as last_month_upload_speed_mbps_B8_12,
  lastmonth.upload_speed_mbps_B12_16 as last_month_upload_speed_mbps_B12_16,
  lastmonth.upload_speed_mbps_B16_20 as last_month_upload_speed_mbps_B16_20,
  lastmonth.upload_speed_mbps_B20_24 as last_month_upload_speed_mbps_B20_24,
  lastmonth.upload_speed_mbps_B24_28 as last_month_upload_speed_mbps_B24_28,
  lastmonth.upload_speed_mbps_B28_32 as last_month_upload_speed_mbps_B28_32,
  lastmonth.upload_speed_mbps_B32_36 as last_month_upload_speed_mbps_B32_36,
  lastmonth.upload_speed_mbps_B36_40 as last_month_upload_speed_mbps_B36_40,
  lastmonth.upload_speed_mbps_B40_44 as last_month_upload_speed_mbps_B40_44,
  lastmonth.upload_speed_mbps_B44_48 as last_month_upload_speed_mbps_B44_48,
  lastmonth.upload_speed_mbps_B48_52 as last_month_upload_speed_mbps_B48_52,
  lastmonth.upload_speed_mbps_B52_56 as last_month_upload_speed_mbps_B52_56,
  lastmonth.upload_speed_mbps_B56_60 as last_month_upload_speed_mbps_B56_60,
  lastmonth.upload_speed_mbps_B60_64 as last_month_upload_speed_mbps_B60_64,
  lastmonth.upload_speed_mbps_B64_68 as last_month_upload_speed_mbps_B64_68,
  lastmonth.upload_speed_mbps_B68_72 as last_month_upload_speed_mbps_B68_72,
  lastmonth.upload_speed_mbps_B72_76 as last_month_upload_speed_mbps_B72_76,
  lastmonth.upload_speed_mbps_B76_80 as last_month_upload_speed_mbps_B76_80,
  lastmonth.upload_speed_mbps_B80_84 as last_month_upload_speed_mbps_B80_84,
  lastmonth.upload_speed_mbps_B84_88 as last_month_upload_speed_mbps_B84_88,
  lastmonth.upload_speed_mbps_B88_92 as last_month_upload_speed_mbps_B88_92,
  lastmonth.upload_speed_mbps_B92_96 as last_month_upload_speed_mbps_B92_96,
  lastmonth.upload_speed_mbps_B96_100 as last_month_upload_speed_mbps_B96_100,
  lastmonth.upload_speed_mbps_BGT100 as last_month_upload_speed_mbps_BGT100,
  -- last year measurements
  lastyear.last_year_test_count as last_year_test_count,
  lastyear.download_speed_mbps_median as last_year_download_speed_mbps_median,
  lastyear.upload_speed_mbps_median as last_year_upload_speed_mbps_median,
  lastyear.download_speed_mbps_avg as last_year_download_speed_mbps_avg,
  lastyear.upload_speed_mbps_avg as last_year_upload_speed_mbps_avg,
  lastyear.download_speed_mbps_min as last_year_download_speed_mbps_min,
  lastyear.download_speed_mbps_max as last_year_download_speed_mbps_max,
  lastyear.upload_speed_mbps_min as last_year_upload_speed_mbps_min,
  lastyear.upload_speed_mbps_max as last_year_upload_speed_mbps_max,
  lastyear.download_speed_mbps_stddev as last_year_download_speed_mbps_stddev,
  lastyear.upload_speed_mbps_stddev as last_year_upload_speed_mbps_stddev,

  lastyear.download_speed_mbps_B4_8 as last_year_download_speed_mbps_B4_8,
  lastyear.download_speed_mbps_B8_12 as last_year_download_speed_mbps_B8_12,
  lastyear.download_speed_mbps_B12_16 as last_year_download_speed_mbps_B12_16,
  lastyear.download_speed_mbps_B16_20 as last_year_download_speed_mbps_B16_20,
  lastyear.download_speed_mbps_B20_24 as last_year_download_speed_mbps_B20_24,
  lastyear.download_speed_mbps_B24_28 as last_year_download_speed_mbps_B24_28,
  lastyear.download_speed_mbps_B28_32 as last_year_download_speed_mbps_B28_32,
  lastyear.download_speed_mbps_B32_36 as last_year_download_speed_mbps_B32_36,
  lastyear.download_speed_mbps_B36_40 as last_year_download_speed_mbps_B36_40,
  lastyear.download_speed_mbps_B40_44 as last_year_download_speed_mbps_B40_44,
  lastyear.download_speed_mbps_B44_48 as last_year_download_speed_mbps_B44_48,
  lastyear.download_speed_mbps_B48_52 as last_year_download_speed_mbps_B48_52,
  lastyear.download_speed_mbps_B52_56 as last_year_download_speed_mbps_B52_56,
  lastyear.download_speed_mbps_B56_60 as last_year_download_speed_mbps_B56_60,
  lastyear.download_speed_mbps_B60_64 as last_year_download_speed_mbps_B60_64,
  lastyear.download_speed_mbps_B64_68 as last_year_download_speed_mbps_B64_68,
  lastyear.download_speed_mbps_B68_72 as last_year_download_speed_mbps_B68_72,
  lastyear.download_speed_mbps_B72_76 as last_year_download_speed_mbps_B72_76,
  lastyear.download_speed_mbps_B76_80 as last_year_download_speed_mbps_B76_80,
  lastyear.download_speed_mbps_B80_84 as last_year_download_speed_mbps_B80_84,
  lastyear.download_speed_mbps_B84_88 as last_year_download_speed_mbps_B84_88,
  lastyear.download_speed_mbps_B88_92 as last_year_download_speed_mbps_B88_92,
  lastyear.download_speed_mbps_B92_96 as last_year_download_speed_mbps_B92_96,
  lastyear.download_speed_mbps_B96_100 as last_year_download_speed_mbps_B96_100,
  lastyear.download_speed_mbps_BGT100 as last_year_download_speed_mbps_BGT100,
  lastyear.upload_speed_mbps_B4_8 as last_year_upload_speed_mbps_B4_8,
  lastyear.upload_speed_mbps_B8_12 as last_year_upload_speed_mbps_B8_12,
  lastyear.upload_speed_mbps_B12_16 as last_year_upload_speed_mbps_B12_16,
  lastyear.upload_speed_mbps_B16_20 as last_year_upload_speed_mbps_B16_20,
  lastyear.upload_speed_mbps_B20_24 as last_year_upload_speed_mbps_B20_24,
  lastyear.upload_speed_mbps_B24_28 as last_year_upload_speed_mbps_B24_28,
  lastyear.upload_speed_mbps_B28_32 as last_year_upload_speed_mbps_B28_32,
  lastyear.upload_speed_mbps_B32_36 as last_year_upload_speed_mbps_B32_36,
  lastyear.upload_speed_mbps_B36_40 as last_year_upload_speed_mbps_B36_40,
  lastyear.upload_speed_mbps_B40_44 as last_year_upload_speed_mbps_B40_44,
  lastyear.upload_speed_mbps_B44_48 as last_year_upload_speed_mbps_B44_48,
  lastyear.upload_speed_mbps_B48_52 as last_year_upload_speed_mbps_B48_52,
  lastyear.upload_speed_mbps_B52_56 as last_year_upload_speed_mbps_B52_56,
  lastyear.upload_speed_mbps_B56_60 as last_year_upload_speed_mbps_B56_60,
  lastyear.upload_speed_mbps_B60_64 as last_year_upload_speed_mbps_B60_64,
  lastyear.upload_speed_mbps_B64_68 as last_year_upload_speed_mbps_B64_68,
  lastyear.upload_speed_mbps_B68_72 as last_year_upload_speed_mbps_B68_72,
  lastyear.upload_speed_mbps_B72_76 as last_year_upload_speed_mbps_B72_76,
  lastyear.upload_speed_mbps_B76_80 as last_year_upload_speed_mbps_B76_80,
  lastyear.upload_speed_mbps_B80_84 as last_year_upload_speed_mbps_B80_84,
  lastyear.upload_speed_mbps_B84_88 as last_year_upload_speed_mbps_B84_88,
  lastyear.upload_speed_mbps_B88_92 as last_year_upload_speed_mbps_B88_92,
  lastyear.upload_speed_mbps_B92_96 as last_year_upload_speed_mbps_B92_96,
  lastyear.upload_speed_mbps_B96_100 as last_year_upload_speed_mbps_B96_100,
  lastyear.upload_speed_mbps_BGT100 as last_year_upload_speed_mbps_BGT100,

  FROM [mlab-oti:bocoup_prod.all_ip_by_day] all,

  left join
  (
    SELECT
      count(*) as last_week_test_count,

      client_country,
      client_continent,

      client_asn_number,
      client_asn_name,

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

      -- download bins
      SUM(IF(download_speed_mbps < 4 ,1,0)) AS download_speed_mbps_B0_4,
      SUM(IF(download_speed_mbps >= 4 AND download_speed_mbps < 8, 1,0)) AS download_speed_mbps_B4_8,
      SUM(IF(download_speed_mbps >= 8 AND download_speed_mbps < 12, 1,0)) AS download_speed_mbps_B8_12,
      SUM(IF(download_speed_mbps >= 12 AND download_speed_mbps < 16, 1,0)) AS download_speed_mbps_B12_16,
      SUM(IF(download_speed_mbps >= 16 AND download_speed_mbps < 20, 1,0)) AS download_speed_mbps_B16_20,
      SUM(IF(download_speed_mbps >= 20 AND download_speed_mbps < 24, 1,0)) AS download_speed_mbps_B20_24,
      SUM(IF(download_speed_mbps >= 24 AND download_speed_mbps < 28, 1,0)) AS download_speed_mbps_B24_28,
      SUM(IF(download_speed_mbps >= 28 AND download_speed_mbps < 32, 1,0)) AS download_speed_mbps_B28_32,
      SUM(IF(download_speed_mbps >= 32 AND download_speed_mbps < 36, 1,0)) AS download_speed_mbps_B32_36,
      SUM(IF(download_speed_mbps >= 36 AND download_speed_mbps < 40, 1,0)) AS download_speed_mbps_B36_40,
      SUM(IF(download_speed_mbps >= 40 AND download_speed_mbps < 44, 1,0)) AS download_speed_mbps_B40_44,
      SUM(IF(download_speed_mbps >= 44 AND download_speed_mbps < 48, 1,0)) AS download_speed_mbps_B44_48,
      SUM(IF(download_speed_mbps >= 48 AND download_speed_mbps < 52, 1,0)) AS download_speed_mbps_B48_52,
      SUM(IF(download_speed_mbps >= 52 AND download_speed_mbps < 56, 1,0)) AS download_speed_mbps_B52_56,
      SUM(IF(download_speed_mbps >= 56 AND download_speed_mbps < 60, 1,0)) AS download_speed_mbps_B56_60,
      SUM(IF(download_speed_mbps >= 60 AND download_speed_mbps < 64, 1,0)) AS download_speed_mbps_B60_64,
      SUM(IF(download_speed_mbps >= 64 AND download_speed_mbps < 68, 1,0)) AS download_speed_mbps_B64_68,
      SUM(IF(download_speed_mbps >= 68 AND download_speed_mbps < 72, 1,0)) AS download_speed_mbps_B68_72,
      SUM(IF(download_speed_mbps >= 72 AND download_speed_mbps < 76, 1,0)) AS download_speed_mbps_B72_76,
      SUM(IF(download_speed_mbps >= 76 AND download_speed_mbps < 80, 1,0)) AS download_speed_mbps_B76_80,
      SUM(IF(download_speed_mbps >= 80 AND download_speed_mbps < 84, 1,0)) AS download_speed_mbps_B80_84,
      SUM(IF(download_speed_mbps >= 84 AND download_speed_mbps < 88, 1,0)) AS download_speed_mbps_B84_88,
      SUM(IF(download_speed_mbps >= 88 AND download_speed_mbps < 92, 1,0)) AS download_speed_mbps_B88_92,
      SUM(IF(download_speed_mbps >= 92 AND download_speed_mbps < 96, 1,0)) AS download_speed_mbps_B92_96,
      SUM(IF(download_speed_mbps >= 96 AND download_speed_mbps < 100, 1,0)) AS download_speed_mbps_B96_100,
      SUM(IF(download_speed_mbps >= 100, 1,0)) AS download_speed_mbps_BGT100,

      -- upload bins
      SUM(IF(upload_speed_mbps < 4 ,1,0)) AS upload_speed_mbps_B0_4,
      SUM(IF(upload_speed_mbps >= 4 AND upload_speed_mbps < 8, 1,0)) AS upload_speed_mbps_B4_8,
      SUM(IF(upload_speed_mbps >= 8 AND upload_speed_mbps < 12, 1,0)) AS upload_speed_mbps_B8_12,
      SUM(IF(upload_speed_mbps >= 12 AND upload_speed_mbps < 16, 1,0)) AS upload_speed_mbps_B12_16,
      SUM(IF(upload_speed_mbps >= 16 AND upload_speed_mbps < 20, 1,0)) AS upload_speed_mbps_B16_20,
      SUM(IF(upload_speed_mbps >= 20 AND upload_speed_mbps < 24, 1,0)) AS upload_speed_mbps_B20_24,
      SUM(IF(upload_speed_mbps >= 24 AND upload_speed_mbps < 28, 1,0)) AS upload_speed_mbps_B24_28,
      SUM(IF(upload_speed_mbps >= 28 AND upload_speed_mbps < 32, 1,0)) AS upload_speed_mbps_B28_32,
      SUM(IF(upload_speed_mbps >= 32 AND upload_speed_mbps < 36, 1,0)) AS upload_speed_mbps_B32_36,
      SUM(IF(upload_speed_mbps >= 36 AND upload_speed_mbps < 40, 1,0)) AS upload_speed_mbps_B36_40,
      SUM(IF(upload_speed_mbps >= 40 AND upload_speed_mbps < 44, 1,0)) AS upload_speed_mbps_B40_44,
      SUM(IF(upload_speed_mbps >= 44 AND upload_speed_mbps < 48, 1,0)) AS upload_speed_mbps_B44_48,
      SUM(IF(upload_speed_mbps >= 48 AND upload_speed_mbps < 52, 1,0)) AS upload_speed_mbps_B48_52,
      SUM(IF(upload_speed_mbps >= 52 AND upload_speed_mbps < 56, 1,0)) AS upload_speed_mbps_B52_56,
      SUM(IF(upload_speed_mbps >= 56 AND upload_speed_mbps < 60, 1,0)) AS upload_speed_mbps_B56_60,
      SUM(IF(upload_speed_mbps >= 60 AND upload_speed_mbps < 64, 1,0)) AS upload_speed_mbps_B60_64,
      SUM(IF(upload_speed_mbps >= 64 AND upload_speed_mbps < 68, 1,0)) AS upload_speed_mbps_B64_68,
      SUM(IF(upload_speed_mbps >= 68 AND upload_speed_mbps < 72, 1,0)) AS upload_speed_mbps_B68_72,
      SUM(IF(upload_speed_mbps >= 72 AND upload_speed_mbps < 76, 1,0)) AS upload_speed_mbps_B72_76,
      SUM(IF(upload_speed_mbps >= 76 AND upload_speed_mbps < 80, 1,0)) AS upload_speed_mbps_B76_80,
      SUM(IF(upload_speed_mbps >= 80 AND upload_speed_mbps < 84, 1,0)) AS upload_speed_mbps_B80_84,
      SUM(IF(upload_speed_mbps >= 84 AND upload_speed_mbps < 88, 1,0)) AS upload_speed_mbps_B84_88,
      SUM(IF(upload_speed_mbps >= 88 AND upload_speed_mbps < 92, 1,0)) AS upload_speed_mbps_B88_92,
      SUM(IF(upload_speed_mbps >= 92 AND upload_speed_mbps < 96, 1,0)) AS upload_speed_mbps_B92_96,
      SUM(IF(upload_speed_mbps >= 96 AND upload_speed_mbps < 100, 1,0)) AS upload_speed_mbps_B96_100,
      SUM(IF(upload_speed_mbps >= 100, 1,0)) AS upload_speed_mbps_BGT100,

    from [mlab-oti:bocoup_prod.all_ip_by_day]
    where
      test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -7, "DAY") and
      client_asn_number is not null and
      client_asn_number is not null

    group by
      client_asn_number,
      client_asn_name,
      client_country,
      client_continent

  ) lastweek on
    all.client_country = lastweek.client_country and
    all.client_continent = lastweek.client_continent and
    all.client_asn_number = lastweek.client_asn_number and
    all.client_asn_name = lastweek.client_asn_name

  -- Compute metrics for the current month
  left join
  (
    SELECT
      count(*) as last_month_test_count,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name,

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

      -- download bins
      SUM(IF(download_speed_mbps < 4 ,1,0)) AS download_speed_mbps_B0_4,
      SUM(IF(download_speed_mbps >= 4 AND download_speed_mbps < 8, 1,0)) AS download_speed_mbps_B4_8,
      SUM(IF(download_speed_mbps >= 8 AND download_speed_mbps < 12, 1,0)) AS download_speed_mbps_B8_12,
      SUM(IF(download_speed_mbps >= 12 AND download_speed_mbps < 16, 1,0)) AS download_speed_mbps_B12_16,
      SUM(IF(download_speed_mbps >= 16 AND download_speed_mbps < 20, 1,0)) AS download_speed_mbps_B16_20,
      SUM(IF(download_speed_mbps >= 20 AND download_speed_mbps < 24, 1,0)) AS download_speed_mbps_B20_24,
      SUM(IF(download_speed_mbps >= 24 AND download_speed_mbps < 28, 1,0)) AS download_speed_mbps_B24_28,
      SUM(IF(download_speed_mbps >= 28 AND download_speed_mbps < 32, 1,0)) AS download_speed_mbps_B28_32,
      SUM(IF(download_speed_mbps >= 32 AND download_speed_mbps < 36, 1,0)) AS download_speed_mbps_B32_36,
      SUM(IF(download_speed_mbps >= 36 AND download_speed_mbps < 40, 1,0)) AS download_speed_mbps_B36_40,
      SUM(IF(download_speed_mbps >= 40 AND download_speed_mbps < 44, 1,0)) AS download_speed_mbps_B40_44,
      SUM(IF(download_speed_mbps >= 44 AND download_speed_mbps < 48, 1,0)) AS download_speed_mbps_B44_48,
      SUM(IF(download_speed_mbps >= 48 AND download_speed_mbps < 52, 1,0)) AS download_speed_mbps_B48_52,
      SUM(IF(download_speed_mbps >= 52 AND download_speed_mbps < 56, 1,0)) AS download_speed_mbps_B52_56,
      SUM(IF(download_speed_mbps >= 56 AND download_speed_mbps < 60, 1,0)) AS download_speed_mbps_B56_60,
      SUM(IF(download_speed_mbps >= 60 AND download_speed_mbps < 64, 1,0)) AS download_speed_mbps_B60_64,
      SUM(IF(download_speed_mbps >= 64 AND download_speed_mbps < 68, 1,0)) AS download_speed_mbps_B64_68,
      SUM(IF(download_speed_mbps >= 68 AND download_speed_mbps < 72, 1,0)) AS download_speed_mbps_B68_72,
      SUM(IF(download_speed_mbps >= 72 AND download_speed_mbps < 76, 1,0)) AS download_speed_mbps_B72_76,
      SUM(IF(download_speed_mbps >= 76 AND download_speed_mbps < 80, 1,0)) AS download_speed_mbps_B76_80,
      SUM(IF(download_speed_mbps >= 80 AND download_speed_mbps < 84, 1,0)) AS download_speed_mbps_B80_84,
      SUM(IF(download_speed_mbps >= 84 AND download_speed_mbps < 88, 1,0)) AS download_speed_mbps_B84_88,
      SUM(IF(download_speed_mbps >= 88 AND download_speed_mbps < 92, 1,0)) AS download_speed_mbps_B88_92,
      SUM(IF(download_speed_mbps >= 92 AND download_speed_mbps < 96, 1,0)) AS download_speed_mbps_B92_96,
      SUM(IF(download_speed_mbps >= 96 AND download_speed_mbps < 100, 1,0)) AS download_speed_mbps_B96_100,
      SUM(IF(download_speed_mbps >= 100, 1,0)) AS download_speed_mbps_BGT100,

      -- upload bins
      SUM(IF(upload_speed_mbps < 4 ,1,0)) AS upload_speed_mbps_B0_4,
      SUM(IF(upload_speed_mbps >= 4 AND upload_speed_mbps < 8, 1,0)) AS upload_speed_mbps_B4_8,
      SUM(IF(upload_speed_mbps >= 8 AND upload_speed_mbps < 12, 1,0)) AS upload_speed_mbps_B8_12,
      SUM(IF(upload_speed_mbps >= 12 AND upload_speed_mbps < 16, 1,0)) AS upload_speed_mbps_B12_16,
      SUM(IF(upload_speed_mbps >= 16 AND upload_speed_mbps < 20, 1,0)) AS upload_speed_mbps_B16_20,
      SUM(IF(upload_speed_mbps >= 20 AND upload_speed_mbps < 24, 1,0)) AS upload_speed_mbps_B20_24,
      SUM(IF(upload_speed_mbps >= 24 AND upload_speed_mbps < 28, 1,0)) AS upload_speed_mbps_B24_28,
      SUM(IF(upload_speed_mbps >= 28 AND upload_speed_mbps < 32, 1,0)) AS upload_speed_mbps_B28_32,
      SUM(IF(upload_speed_mbps >= 32 AND upload_speed_mbps < 36, 1,0)) AS upload_speed_mbps_B32_36,
      SUM(IF(upload_speed_mbps >= 36 AND upload_speed_mbps < 40, 1,0)) AS upload_speed_mbps_B36_40,
      SUM(IF(upload_speed_mbps >= 40 AND upload_speed_mbps < 44, 1,0)) AS upload_speed_mbps_B40_44,
      SUM(IF(upload_speed_mbps >= 44 AND upload_speed_mbps < 48, 1,0)) AS upload_speed_mbps_B44_48,
      SUM(IF(upload_speed_mbps >= 48 AND upload_speed_mbps < 52, 1,0)) AS upload_speed_mbps_B48_52,
      SUM(IF(upload_speed_mbps >= 52 AND upload_speed_mbps < 56, 1,0)) AS upload_speed_mbps_B52_56,
      SUM(IF(upload_speed_mbps >= 56 AND upload_speed_mbps < 60, 1,0)) AS upload_speed_mbps_B56_60,
      SUM(IF(upload_speed_mbps >= 60 AND upload_speed_mbps < 64, 1,0)) AS upload_speed_mbps_B60_64,
      SUM(IF(upload_speed_mbps >= 64 AND upload_speed_mbps < 68, 1,0)) AS upload_speed_mbps_B64_68,
      SUM(IF(upload_speed_mbps >= 68 AND upload_speed_mbps < 72, 1,0)) AS upload_speed_mbps_B68_72,
      SUM(IF(upload_speed_mbps >= 72 AND upload_speed_mbps < 76, 1,0)) AS upload_speed_mbps_B72_76,
      SUM(IF(upload_speed_mbps >= 76 AND upload_speed_mbps < 80, 1,0)) AS upload_speed_mbps_B76_80,
      SUM(IF(upload_speed_mbps >= 80 AND upload_speed_mbps < 84, 1,0)) AS upload_speed_mbps_B80_84,
      SUM(IF(upload_speed_mbps >= 84 AND upload_speed_mbps < 88, 1,0)) AS upload_speed_mbps_B84_88,
      SUM(IF(upload_speed_mbps >= 88 AND upload_speed_mbps < 92, 1,0)) AS upload_speed_mbps_B88_92,
      SUM(IF(upload_speed_mbps >= 92 AND upload_speed_mbps < 96, 1,0)) AS upload_speed_mbps_B92_96,
      SUM(IF(upload_speed_mbps >= 96 AND upload_speed_mbps < 100, 1,0)) AS upload_speed_mbps_B96_100,
      SUM(IF(upload_speed_mbps >= 100, 1,0)) AS upload_speed_mbps_BGT100,

    from [mlab-oti:bocoup_prod.all_ip_by_day]
    -- current month:
    where
      test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "MONTH") and
      client_asn_number is not null and
      client_asn_number is not null

    group by
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name
  ) lastmonth on
    all.client_country = lastmonth.client_country and
    all.client_continent = lastmonth.client_continent and
    all.client_asn_number = lastmonth.client_asn_number and
    all.client_asn_name = lastmonth.client_asn_name

  -- Compute metrics for the current year
  left join
  (
    SELECT
      count(*) as last_year_test_count,
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name,

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

      -- download bins
      SUM(IF(download_speed_mbps < 4 ,1,0)) AS download_speed_mbps_B0_4,
      SUM(IF(download_speed_mbps >= 4 AND download_speed_mbps < 8, 1,0)) AS download_speed_mbps_B4_8,
      SUM(IF(download_speed_mbps >= 8 AND download_speed_mbps < 12, 1,0)) AS download_speed_mbps_B8_12,
      SUM(IF(download_speed_mbps >= 12 AND download_speed_mbps < 16, 1,0)) AS download_speed_mbps_B12_16,
      SUM(IF(download_speed_mbps >= 16 AND download_speed_mbps < 20, 1,0)) AS download_speed_mbps_B16_20,
      SUM(IF(download_speed_mbps >= 20 AND download_speed_mbps < 24, 1,0)) AS download_speed_mbps_B20_24,
      SUM(IF(download_speed_mbps >= 24 AND download_speed_mbps < 28, 1,0)) AS download_speed_mbps_B24_28,
      SUM(IF(download_speed_mbps >= 28 AND download_speed_mbps < 32, 1,0)) AS download_speed_mbps_B28_32,
      SUM(IF(download_speed_mbps >= 32 AND download_speed_mbps < 36, 1,0)) AS download_speed_mbps_B32_36,
      SUM(IF(download_speed_mbps >= 36 AND download_speed_mbps < 40, 1,0)) AS download_speed_mbps_B36_40,
      SUM(IF(download_speed_mbps >= 40 AND download_speed_mbps < 44, 1,0)) AS download_speed_mbps_B40_44,
      SUM(IF(download_speed_mbps >= 44 AND download_speed_mbps < 48, 1,0)) AS download_speed_mbps_B44_48,
      SUM(IF(download_speed_mbps >= 48 AND download_speed_mbps < 52, 1,0)) AS download_speed_mbps_B48_52,
      SUM(IF(download_speed_mbps >= 52 AND download_speed_mbps < 56, 1,0)) AS download_speed_mbps_B52_56,
      SUM(IF(download_speed_mbps >= 56 AND download_speed_mbps < 60, 1,0)) AS download_speed_mbps_B56_60,
      SUM(IF(download_speed_mbps >= 60 AND download_speed_mbps < 64, 1,0)) AS download_speed_mbps_B60_64,
      SUM(IF(download_speed_mbps >= 64 AND download_speed_mbps < 68, 1,0)) AS download_speed_mbps_B64_68,
      SUM(IF(download_speed_mbps >= 68 AND download_speed_mbps < 72, 1,0)) AS download_speed_mbps_B68_72,
      SUM(IF(download_speed_mbps >= 72 AND download_speed_mbps < 76, 1,0)) AS download_speed_mbps_B72_76,
      SUM(IF(download_speed_mbps >= 76 AND download_speed_mbps < 80, 1,0)) AS download_speed_mbps_B76_80,
      SUM(IF(download_speed_mbps >= 80 AND download_speed_mbps < 84, 1,0)) AS download_speed_mbps_B80_84,
      SUM(IF(download_speed_mbps >= 84 AND download_speed_mbps < 88, 1,0)) AS download_speed_mbps_B84_88,
      SUM(IF(download_speed_mbps >= 88 AND download_speed_mbps < 92, 1,0)) AS download_speed_mbps_B88_92,
      SUM(IF(download_speed_mbps >= 92 AND download_speed_mbps < 96, 1,0)) AS download_speed_mbps_B92_96,
      SUM(IF(download_speed_mbps >= 96 AND download_speed_mbps < 100, 1,0)) AS download_speed_mbps_B96_100,
      SUM(IF(download_speed_mbps >= 100, 1,0)) AS download_speed_mbps_BGT100,

      -- upload bins
      SUM(IF(upload_speed_mbps < 4 ,1,0)) AS upload_speed_mbps_B0_4,
      SUM(IF(upload_speed_mbps >= 4 AND upload_speed_mbps < 8, 1,0)) AS upload_speed_mbps_B4_8,
      SUM(IF(upload_speed_mbps >= 8 AND upload_speed_mbps < 12, 1,0)) AS upload_speed_mbps_B8_12,
      SUM(IF(upload_speed_mbps >= 12 AND upload_speed_mbps < 16, 1,0)) AS upload_speed_mbps_B12_16,
      SUM(IF(upload_speed_mbps >= 16 AND upload_speed_mbps < 20, 1,0)) AS upload_speed_mbps_B16_20,
      SUM(IF(upload_speed_mbps >= 20 AND upload_speed_mbps < 24, 1,0)) AS upload_speed_mbps_B20_24,
      SUM(IF(upload_speed_mbps >= 24 AND upload_speed_mbps < 28, 1,0)) AS upload_speed_mbps_B24_28,
      SUM(IF(upload_speed_mbps >= 28 AND upload_speed_mbps < 32, 1,0)) AS upload_speed_mbps_B28_32,
      SUM(IF(upload_speed_mbps >= 32 AND upload_speed_mbps < 36, 1,0)) AS upload_speed_mbps_B32_36,
      SUM(IF(upload_speed_mbps >= 36 AND upload_speed_mbps < 40, 1,0)) AS upload_speed_mbps_B36_40,
      SUM(IF(upload_speed_mbps >= 40 AND upload_speed_mbps < 44, 1,0)) AS upload_speed_mbps_B40_44,
      SUM(IF(upload_speed_mbps >= 44 AND upload_speed_mbps < 48, 1,0)) AS upload_speed_mbps_B44_48,
      SUM(IF(upload_speed_mbps >= 48 AND upload_speed_mbps < 52, 1,0)) AS upload_speed_mbps_B48_52,
      SUM(IF(upload_speed_mbps >= 52 AND upload_speed_mbps < 56, 1,0)) AS upload_speed_mbps_B52_56,
      SUM(IF(upload_speed_mbps >= 56 AND upload_speed_mbps < 60, 1,0)) AS upload_speed_mbps_B56_60,
      SUM(IF(upload_speed_mbps >= 60 AND upload_speed_mbps < 64, 1,0)) AS upload_speed_mbps_B60_64,
      SUM(IF(upload_speed_mbps >= 64 AND upload_speed_mbps < 68, 1,0)) AS upload_speed_mbps_B64_68,
      SUM(IF(upload_speed_mbps >= 68 AND upload_speed_mbps < 72, 1,0)) AS upload_speed_mbps_B68_72,
      SUM(IF(upload_speed_mbps >= 72 AND upload_speed_mbps < 76, 1,0)) AS upload_speed_mbps_B72_76,
      SUM(IF(upload_speed_mbps >= 76 AND upload_speed_mbps < 80, 1,0)) AS upload_speed_mbps_B76_80,
      SUM(IF(upload_speed_mbps >= 80 AND upload_speed_mbps < 84, 1,0)) AS upload_speed_mbps_B80_84,
      SUM(IF(upload_speed_mbps >= 84 AND upload_speed_mbps < 88, 1,0)) AS upload_speed_mbps_B84_88,
      SUM(IF(upload_speed_mbps >= 88 AND upload_speed_mbps < 92, 1,0)) AS upload_speed_mbps_B88_92,
      SUM(IF(upload_speed_mbps >= 92 AND upload_speed_mbps < 96, 1,0)) AS upload_speed_mbps_B92_96,
      SUM(IF(upload_speed_mbps >= 96 AND upload_speed_mbps < 100, 1,0)) AS upload_speed_mbps_B96_100,
      SUM(IF(upload_speed_mbps >= 100, 1,0)) AS upload_speed_mbps_BGT100,

    from [mlab-oti:bocoup_prod.all_ip_by_day]
    -- current year:
    where
      test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, "YEAR") and
      client_asn_number is not null and
      client_asn_number is not null
    group by
      client_country,
      client_continent,
      client_asn_number,
      client_asn_name

  ) lastyear on
    all.client_country = lastyear.client_country and
    all.client_continent = lastyear.client_continent and
    all.client_asn_number = lastyear.client_asn_number and
    all.client_asn_name = lastyear.client_asn_name

  GROUP BY
    -- key fields:
    location_id,
    client_asn_number,

    -- metadata:
    client_asn_name,
    type,

    client_country,
    client_country_code,
    client_continent,
    client_continent_code,

    -- last week:
    last_week_test_count,
    last_week_download_speed_mbps_median,
    last_week_upload_speed_mbps_median,
    last_week_download_speed_mbps_avg,
    last_week_upload_speed_mbps_avg,
    last_week_download_speed_mbps_min,
    last_week_download_speed_mbps_max,
    last_week_upload_speed_mbps_min,
    last_week_upload_speed_mbps_max,
    last_week_download_speed_mbps_stddev,
    last_week_upload_speed_mbps_stddev,

    -- last month:
    last_month_test_count,
    last_month_download_speed_mbps_median,
    last_month_upload_speed_mbps_median,
    last_month_download_speed_mbps_avg,
    last_month_upload_speed_mbps_avg,
    last_month_download_speed_mbps_min,
    last_month_download_speed_mbps_max,
    last_month_upload_speed_mbps_min,
    last_month_upload_speed_mbps_max,
    last_month_download_speed_mbps_stddev,
    last_month_upload_speed_mbps_stddev,

    -- last year:
    last_year_test_count,
    last_year_download_speed_mbps_median,
    last_year_upload_speed_mbps_median,
    last_year_download_speed_mbps_avg,
    last_year_upload_speed_mbps_avg,
    last_year_download_speed_mbps_min,
    last_year_download_speed_mbps_max,
    last_year_upload_speed_mbps_min,
    last_year_upload_speed_mbps_max,
    last_year_download_speed_mbps_stddev,
    last_year_upload_speed_mbps_stddev
);
