-- ============
-- Type: {2}
-- ============
(SELECT
  {0}, -- parent key
  {1}, -- child key

  "{2}" AS type, -- type
  count(*) as test_count,

  {3}, -- meta fields we are selecting

  {4} -- static fields

  FROM {{0}} all

  -- left join madness here!
  {5}

  {6}

  GROUP BY

  {7}

)
