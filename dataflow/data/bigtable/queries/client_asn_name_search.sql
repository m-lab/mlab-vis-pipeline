SELECT

  -- keys:
  lower(REGEXP_REPLACE(client_asn_name, r'[^\w|_]', '')) as client_asn_name_lookup,
  client_asn_number,

  -- metadata:
  client_asn_name,

  -- data:
  test_count,
  last_three_month_test_count
from

(SELECT

  all.client_asn_number as client_asn_number,
  all.client_asn_name as client_asn_name,

  count(*) as test_count,
  threemonths.last_three_month_test_count

  FROM {0} all
  left join

  -- last three months:
  (SELECT
    count(*) as last_three_month_test_count,
    client_asn_number,
    client_asn_name
    from {0}
    where test_date >= DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -3, "MONTH")
    group by
      client_asn_number,
      client_asn_name
  ) threemonths on
    all.client_asn_number = threemonths.client_asn_number and
    all.client_asn_name = threemonths.client_asn_name

  GROUP BY
    client_asn_number,
    client_asn_name,
    threemonths.last_three_month_test_count
)

WHERE
  client_asn_name IS NOT NULL and
  client_asn_number IS NOT NULL;
