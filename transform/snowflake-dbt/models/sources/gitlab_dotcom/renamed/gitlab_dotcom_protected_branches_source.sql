    
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_protected_branches_dedupe_source') }}
    
), renamed AS (
  
    SELECT
      id::NUMBER                           AS protected_branch_id,
      name::VARCHAR                         AS protected_branch_name,
      project_id::VARCHAR                   AS project_id,
      created_at::TIMESTAMP                 AS created_at,
      updated_at::TIMESTAMP                 AS updated_at,
      code_owner_approval_required::BOOLEAN AS is_code_owner_approval_required
    FROM source
    
)

SELECT * 
FROM renamed
