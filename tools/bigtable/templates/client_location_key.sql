
  REPLACE(LOWER(CONCAT(IFNULL(client_continent_code, ""), ""
    ,IFNULL(client_country_code, ""), ""
    ,IFNULL(client_region_code, ""), ""
    ,IFNULL(client_city, ""), ""
  )), " ", "") as client_location_key
