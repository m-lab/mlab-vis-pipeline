SELECT

  -- keys:
  lower(REGEXP_REPLACE(server_asn_name, r"[^\w|_]", "")) as server_asn_name_lookup,

  -- metadata:
  server_asn_name,
  server_asn_number,

  -- data:
  test_count,
  last_three_month_test_count
from

(SELECT

  all.server_asn_name as server_asn_name,
  all.server_asn_number as server_asn_number,

  count(*) as test_count,
  threemonths.last_three_month_test_count

  FROM {0} all
  left join

  -- last three months:
  (SELECT
    count(*) as last_three_month_test_count,
    server_asn_name,
    server_asn_number
    from {0}
    where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
    group by
      server_asn_name,
      server_asn_number
  ) threemonths on
    all.server_asn_name = threemonths.server_asn_name and
    all.server_asn_number = threemonths.server_asn_number

  GROUP BY
    server_asn_name,
    server_asn_number,
    threemonths.last_three_month_test_count
)

WHERE
  server_asn_name IS NOT NULL AND
  server_asn_number IS NOT NULL;
