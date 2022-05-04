WITH source AS (

    SELECT 
      month_year                                                       AS month_date,
      CASE WHEN department = 'People' AND month_year <='2019-08-31' 
            THEN 'People Ops'
           WHEN department = 'People' AND month_year>='2020-05-31' 
             THEN 'People Success'
           WHEN department = 'Brand and Digital Design' 
             THEN 'Brand & Digital Design'
           WHEN department= 'Outreach' AND month_year<'2020-02-29' 
             THEN 'Community Relations'
           ELSE department END                                          AS department,
      plan                                                              AS headcount
    FROM {{ ref ('sheetload_hire_plan') }}
    WHERE month_year <='2020-05-31'

), employee_directory AS (

    SELECT *
    FROM {{ ref ('employee_directory_analysis') }}

), department_division_mapping AS (

    SELECT DISTINCT 
      department, 
      department_modified,
      division_mapped_current AS division
    FROM {{ ref ('bamboohr_job_info_current_division_base') }}   
    WHERE department IS NOT NULL
      
), all_company AS (

    SELECT 
      source.month_date,
      'all_company_breakout'                                                AS breakout_type,
      'all_company_breakout'                                                AS department,
      'all_company_breakout'                                                AS division,
      SUM(headcount)                                                        AS planned_headcount,
      planned_headcount - lag(planned_headcount) OVER (ORDER BY month_date) AS planned_hires       
    FROM source
    GROUP BY 1,2,3
  
), division_level AS (

    SELECT 
      source.month_date,
      'division_breakout'                                                   AS breakout_type,
      'division_breakout'                                                   AS department,
      department_division_mapping.division,
      SUM(headcount)                                                        AS planned_headcount,
      planned_headcount - lag(planned_headcount) 
        OVER (PARTITION BY department_division_mapping.division 
              ORDER BY source.month_date)                                   AS planned_hires       
    FROM source
    LEFT JOIN department_division_mapping 
      ON department_division_mapping.department = source.department
    GROUP BY 1,2,3,4

), department_level AS (

    SELECT 
      source.month_date,
      'department_division_breakout'                                     AS breakout_type,
      source.department                                                  AS department,
      department_division_mapping.division,
      SUM(headcount)                                                     AS planned_headcount,
      planned_headcount - lag(planned_headcount) 
        OVER (PARTITION BY department_division_mapping.division, source.department 
              ORDER BY source.month_date)                                AS planned_hires       
    FROM source
    LEFT JOIN department_division_mapping 
      ON department_division_mapping.department = source.department
    GROUP BY 1,2,3,4

)

SELECT *
FROM all_company 

UNION All

SELECT * 
FROM division_level

UNION ALL 

SELECT *
FROM department_level
