    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_milestones_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                           AS milestone_id,
      title::VARCHAR                        AS milestone_title,
      description::VARCHAR                  AS milestone_description,
      project_id::NUMBER                   AS project_id,
      group_id::NUMBER                     AS group_id,
      start_date::DATE                      AS start_date,
      due_date::DATE                        AS due_date,
      state::VARCHAR                        AS milestone_status,

      created_at::TIMESTAMP                 AS created_at,
      updated_at::TIMESTAMP                 AS updated_at

    FROM source

)

SELECT *
FROM renamed
