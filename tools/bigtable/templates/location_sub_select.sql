-- ============
-- Type: {1}
-- ============
(SELECT
  {0},

  -- what is its type?
  "{1}" AS type,

  -- meta fields we are selecting
  {2},

  -- timed fields
  {3}

  FROM {{0}} all

  -- left join madness here!
  {4}

  GROUP BY

  {5}

)
