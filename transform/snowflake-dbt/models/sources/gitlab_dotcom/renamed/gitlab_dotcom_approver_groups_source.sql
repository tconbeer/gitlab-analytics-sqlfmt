WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_approver_groups_dedupe_source') }}
  
), renamed AS (

  SELECT
    id::NUMBER                   AS approver_group_id,
    target_type::VARCHAR          AS target_type,
    group_id::NUMBER             AS group_id,
    created_at::TIMESTAMP         AS created_at,
    updated_at::TIMESTAMP         AS updated_at

  FROM source

)


SELECT *
FROM renamed
