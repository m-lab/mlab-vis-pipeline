
-- Type: {0}
(select

  -- city/region/country/continent
  "{0}" as type,

  -- data:
  count(*) as test_count,
  {4}.last_three_month_test_count,

  -- which field is the location field?
  all.{5} as location,
  -- metadata location fields and their names
  -- in the form all.field as field:
  {2},
  {6}

  FROM {{0}} all
  left join
  (SELECT
    count(*) as last_three_month_test_count,



    -- which location fields? list
    {1}
    from {{0}}
    where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
    group by
      -- location fields to group by, same as selected
      {1}
    ) {4} on
        -- grouped on location fields. list
        {3}
  GROUP BY
    location,
    location_key,

    -- group by location fields
    {1},
    {4}.last_three_month_test_count
)
