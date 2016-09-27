SELECT

  -- keys:
  server_asn_number,
  client_asn_number,

  -- metadata:
  server_asn_name,
  client_asn_name,

  -- data:
  test_count,
  last_three_month_test_count
from

(SELECT

  all.server_asn_number as server_asn_number,
  all.client_asn_number as client_asn_number,
  all.server_asn_name as server_asn_name,
  all.client_asn_name as client_asn_name,

  count(*) as test_count,
  threemonths.last_three_month_test_count

  FROM {0} all
  left join

  -- last three months:
  (SELECT
    server_asn_number,
    client_asn_number,
    count(*) as last_three_month_test_count
    from {0}
    where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
    group by
      server_asn_number,
      client_asn_number,
  ) threemonths on
    all.client_asn_number = threemonths.client_asn_number and
    all.server_asn_number = threemonths.server_asn_number

  GROUP BY
    server_asn_number,
    client_asn_number,
    server_asn_name,
    client_asn_name,
    threemonths.last_three_month_test_count
)

WHERE
  server_asn_number IS NOT NULL and
  client_asn_number IS NOT NULL and
  client_asn_name IS NOT NULL and
  server_asn_name IS NOT NULL;
