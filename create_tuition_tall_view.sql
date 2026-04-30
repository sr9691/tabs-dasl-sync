-- Purpose-built tall/long view for charting tuition across grades & schools.
-- One row per (school, year, program_type, fee_type, grade_level).
-- Use for line charts with grade on x-axis, amount on y-axis, school as breakdown.

CREATE OR REPLACE VIEW `dasl-493214.dasl.vw_tuition_tall` AS
WITH grade_map AS (
  SELECT
    var_id,
    section,
    question,
    -- Program type from the section
    CASE
      WHEN REGEXP_CONTAINS(section, r'(?i)5[- ]day\s+boarding') THEN '5-Day Boarding'
      WHEN REGEXP_CONTAINS(section, r'(?i)7[- ]day\s+boarding') THEN '7-Day Boarding'
      WHEN REGEXP_CONTAINS(section, r'(?i)5[- ]day\s+domestic')  THEN '5-Day Domestic Boarders'
      WHEN REGEXP_CONTAINS(section, r'(?i)7[- ]day\s+domestic')  THEN '7-Day Domestic Boarders'
      WHEN REGEXP_CONTAINS(section, r'(?i)day\s+students|day\s+domestic') THEN 'Day Students'
      ELSE section
    END AS program_type,
    -- Fee type from the question
    CASE
      WHEN REGEXP_CONTAINS(question, r'(?i)tuition\s+and\s+fees') THEN 'Tuition + Fees'
      WHEN REGEXP_CONTAINS(question, r'(?i)tuition\s+only')       THEN 'Tuition'
      WHEN REGEXP_CONTAINS(question, r'(?i)fees\s+only')          THEN 'Fees'
      ELSE NULL
    END AS fee_type,
    -- Grade label. Anchor to the END of the question so "Grade 1 to Grade 12: ... : Grade 9"
    -- picks up "Grade 9", not "Grade 1".
    CASE
      WHEN REGEXP_CONTAINS(question, r'(?i)preschool|2 years old|3 years old|4 years old') THEN 'Preschool'
      WHEN REGEXP_CONTAINS(question, r'(?i)kindergarten\s*$') THEN 'Kindergarten'
      WHEN REGEXP_CONTAINS(question, r'(?i)pre[- ]first') THEN 'Pre-First'
      WHEN REGEXP_CONTAINS(question, r'Grade\s+\d+\s*$')
        THEN CONCAT('Grade ', REGEXP_EXTRACT(question, r'Grade\s+(\d+)\s*$'))
      ELSE NULL
    END AS grade_level,
    -- Numeric order (Preschool=-1, Kindergarten=1, Pre-First=2, Grade N = N+2).
    CASE
      WHEN REGEXP_CONTAINS(question, r'(?i)preschool|years old') THEN -1
      WHEN REGEXP_CONTAINS(question, r'(?i)kindergarten\s*$') THEN 1
      WHEN REGEXP_CONTAINS(question, r'(?i)pre[- ]first') THEN 2
      WHEN REGEXP_CONTAINS(question, r'Grade\s+\d+\s*$')
        THEN CAST(REGEXP_EXTRACT(question, r'Grade\s+(\d+)\s*$') AS INT64) + 2
      ELSE NULL
    END AS grade_order
  FROM `dasl-493214.dasl.dim_variable`
  WHERE category = 'Tuition and Fees'
    -- Only keep the straightforward "grade × fee-type" matrix.
    -- Exclude "family paying X% of tuition" breakouts and boarder counts —
    -- those belong in separate views.
    AND question NOT LIKE '%Family paying%'
    AND question NOT LIKE '%Total count%'
    AND question NOT LIKE '%(TABS)%'
)
SELECT
  f.school_id,
  s.school_name,
  s.state_code,
  f.year,
  g.program_type,
  g.fee_type,
  g.grade_level,
  g.grade_order,
  f.value_numeric AS amount
FROM `dasl-493214.dasl.fact_school_data` f
JOIN `dasl-493214.dasl.dim_school` s USING (school_id)
JOIN grade_map g USING (var_id)
WHERE f.value_numeric IS NOT NULL
  AND g.grade_level IS NOT NULL
  AND g.fee_type IS NOT NULL;
