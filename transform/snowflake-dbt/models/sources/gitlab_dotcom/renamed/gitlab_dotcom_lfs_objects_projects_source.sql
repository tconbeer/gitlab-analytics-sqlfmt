WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_lfs_objects_projects_dedupe_source') }}
      
)

, renamed AS (
  
    SELECT 
      id::NUMBER              AS lfs_object_project_id,
      lfs_object_id::NUMBER   AS lfs_object_id,
      project_id::NUMBER      AS project_id,
      created_at::TIMESTAMP    AS created_at,
      updated_at::TIMESTAMP    AS updated_at,
      repository_type::VARCHAR AS repository_type
      
    FROM source
  
)

SELECT * 
FROM renamed
