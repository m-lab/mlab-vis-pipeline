-- ============
-- Type: {2}
-- ============
(SELECT
  {0},

  -- which field is the child location?
  REPLACE(LOWER(all.{1}), " ", "") AS child_location_name,

  -- what is its type?
  "{2}" AS type,

  -- meta fields we are selecting
  {3},

  -- timed fields
  {4}

  FROM {{0}} all

  -- left join madness here!
  {5}

  GROUP BY

  {6}

)
