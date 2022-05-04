WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_platform_metrics_dedupe_source') }}
    
), renamed AS (

    SELECT
      id::NUMBER                                      AS metric_id,
      recorded_at::TIMESTAMP                          AS recorded_at,
      platform_target::VARCHAR                        AS platform_target,
      count::NUMBER                                   AS target_count                     
    FROM source

)

SELECT *
FROM renamed
