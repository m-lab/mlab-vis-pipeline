SELECT

  -- keys:
  client_asn_number,
  server_asn_number,

  -- metadata:
  client_asn_name,
  server_asn_name,

  -- data:
  test_count,
  last_three_month_test_count
from

(SELECT

  all.client_asn_number as client_asn_number,
  all.server_asn_name as server_asn_name,
  all.client_asn_name as client_asn_name,
  all.server_asn_number as server_asn_number,

  count(*) as test_count,
  threemonths.last_three_month_test_count

  FROM {0} all
  left join

  -- last three months:
  (SELECT
    count(*) as last_three_month_test_count,
    client_asn_number,
    server_asn_name,
    client_asn_name,
    server_asn_number
    from {0}
    where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
    group by
      client_asn_number,
      server_asn_name,
      client_asn_name,
      server_asn_number
  ) threemonths on
    all.client_asn_number = threemonths.client_asn_number and
    all.server_asn_name = threemonths.server_asn_name and
    all.client_asn_name = threemonths.client_asn_name

  GROUP BY
    client_asn_number,
    server_asn_name,
    client_asn_name,
    server_asn_number,
    threemonths.last_three_month_test_count
)

WHERE
  client_asn_name IS NOT NULL and
  server_asn_name IS NOT NULL and
  server_asn_number IS NOT NULL and
  client_asn_number IS NOT NULL;
