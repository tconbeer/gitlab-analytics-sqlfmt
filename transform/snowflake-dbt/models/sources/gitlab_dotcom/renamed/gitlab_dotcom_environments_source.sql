    
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_environments_dedupe_source') }}
    
), renamed AS (

    SELECT
      id::NUMBER                                      AS environment_id,
      project_id::NUMBER                              AS project_id,
      name::VARCHAR                                    AS environment_name,
      created_at::TIMESTAMP                            AS created_at,
      updated_at::TIMESTAMP                            AS updated_at,
      external_url::VARCHAR                            AS external_url,
      environment_type::VARCHAR                        AS environment_type,
      state::VARCHAR                                   AS state,
      slug::VARCHAR                                    AS slug
    FROM source

)
SELECT *
FROM renamed
