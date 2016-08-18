
SELECT REPLACE(LOWER(CONCAT(IFNULL(location, ""), "",IFNULL(client_region_code, ""), "",IFNULL(client_country_code, ""), "",IFNULL(client_continent_code, ""), "")), " ", "") as reverse_location_key,
       test_count,
       last_three_month_test_count,
       location,
       type,
       client_region,
       client_country,
       client_continent
from -- Type: city

  (select -- city/region/country/continent
 "city" as type, -- data:
 count(*) as test_count,
 threemonths.last_three_month_test_count, -- which field is the location field?
 all.client_city as location, -- metadata location fields and their names
 -- in the form all.field as field:
 all.client_city as client_city,
 all.client_region as client_region,
 all.client_country as client_country,
 all.client_continent as client_continent,
 all.client_region_code as client_region_code,
 all.client_country_code as client_country_code,
 all.client_continent_code as client_continent_code
   FROM {0} all
   left join
     (SELECT count(*) as last_three_month_test_count, -- which location fields? list
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
      group by -- location fields to group by, same as selected
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) threemonths on -- grouped on location fields. list
 all.client_city = threemonths.client_city
   and all.client_region = threemonths.client_region
   and all.client_country = threemonths.client_country
   and all.client_continent = threemonths.client_continent
   and all.client_region_code = threemonths.client_region_code
   and all.client_country_code = threemonths.client_country_code
   and all.client_continent_code = threemonths.client_continent_code
   GROUP BY location, -- group by location fields
 client_city,
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code,
 threemonths.last_three_month_test_count) , -- Type: region

  (select -- city/region/country/continent
 "region" as type, -- data:
 count(*) as test_count,
 threemonths.last_three_month_test_count, -- which field is the location field?
 all.client_region as location, -- metadata location fields and their names
 -- in the form all.field as field:
 all.client_region as client_region,
 all.client_country as client_country,
 all.client_continent as client_continent,
 all.client_region_code as client_region_code,
 all.client_country_code as client_country_code,
 all.client_continent_code as client_continent_code
   FROM {0} all
   left join
     (SELECT count(*) as last_three_month_test_count, -- which location fields? list
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
      group by -- location fields to group by, same as selected
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code ) threemonths on -- grouped on location fields. list
 all.client_region = threemonths.client_region
   and all.client_country = threemonths.client_country
   and all.client_continent = threemonths.client_continent
   and all.client_region_code = threemonths.client_region_code
   and all.client_country_code = threemonths.client_country_code
   and all.client_continent_code = threemonths.client_continent_code
   GROUP BY location, -- group by location fields
 client_region,
 client_country,
 client_continent,
 client_region_code,
 client_country_code,
 client_continent_code,
 threemonths.last_three_month_test_count) , -- Type: country

  (select -- city/region/country/continent
 "country" as type, -- data:
 count(*) as test_count,
 threemonths.last_three_month_test_count, -- which field is the location field?
 all.client_country as location, -- metadata location fields and their names
 -- in the form all.field as field:
 all.client_country as client_country,
 all.client_continent as client_continent,
 all.client_country_code as client_country_code,
 all.client_continent_code as client_continent_code
   FROM {0} all
   left join
     (SELECT count(*) as last_three_month_test_count, -- which location fields? list
 client_country,
 client_continent,
 client_country_code,
 client_continent_code
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
      group by -- location fields to group by, same as selected
 client_country,
 client_continent,
 client_country_code,
 client_continent_code ) threemonths on -- grouped on location fields. list
 all.client_country = threemonths.client_country
   and all.client_continent = threemonths.client_continent
   and all.client_country_code = threemonths.client_country_code
   and all.client_continent_code = threemonths.client_continent_code
   GROUP BY location, -- group by location fields
 client_country,
 client_continent,
 client_country_code,
 client_continent_code,
 threemonths.last_three_month_test_count) , -- Type: continent

  (select -- city/region/country/continent
 "continent" as type, -- data:
 count(*) as test_count,
 threemonths.last_three_month_test_count, -- which field is the location field?
 all.client_continent as location, -- metadata location fields and their names
 -- in the form all.field as field:
 all.client_continent as client_continent,
 all.client_continent_code as client_continent_code
   FROM {0} all
   left join
     (SELECT count(*) as last_three_month_test_count, -- which location fields? list
 client_continent,
 client_continent_code
      from {0}
      where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
      group by -- location fields to group by, same as selected
 client_continent,
 client_continent_code ) threemonths on -- grouped on location fields. list
 all.client_continent = threemonths.client_continent
   and all.client_continent_code = threemonths.client_continent_code
   GROUP BY location, -- group by location fields
 client_continent,
 client_continent_code,
 threemonths.last_three_month_test_count)
WHERE location IS NOT NULL;

