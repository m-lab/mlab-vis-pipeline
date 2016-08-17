REPLACE(LOWER(CONCAT(IFNULL(client_city, ""), ""
  ,IFNULL(client_region_code, ""), ""
  ,IFNULL(client_country_code, ""), ""
  ,IFNULL(client_continent_code, ""), ""
)), " ", "") as reverse_client_location_key
