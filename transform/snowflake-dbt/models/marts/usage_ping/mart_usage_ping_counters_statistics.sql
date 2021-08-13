/* grain: one record per host per metric per month */

{{ simple_cte([('flattened_usage_data', 'prep_usage_data_flattende'),
                ('dim_gitlab_releases', 'dim_gitlab_releases'),
                ]
                )
}}
                
, transformed AS (
  
    SELECT DISTINCT
      metrics_path, 
      IFF(edition='CE', edition, 'EE')                        AS edition,
      SPLIT_PART(metrics_path, '.', 1)                        AS main_json_name,
      SPLIT_PART(metrics_path, '.', -1)                       AS feature_name,
      FIRST_VALUE(flattened_usage_data.major_minor_version) OVER (
        PARTITION BY metrics_path 
        ORDER BY release_date ASC
      )                                                       AS first_version_with_counter,
      MIN(flattened_usage_data.major_version) OVER (
        PARTITION BY metrics_path
      )                                                       AS first_major_version_with_counter,
      FIRST_VALUE(flattened_usage_data.minor_version) OVER (
        PARTITION BY metrics_path 
        ORDER BY release_date ASC
      )                                                       AS first_minor_version_with_counter,
      LAST_VALUE(flattened_usage_data.major_minor_version) OVER (
        PARTITION BY metrics_path 
        ORDER BY release_date ASC
      )                                                       AS last_version_with_counter,
      MAX(flattened_usage_data.major_version) OVER (
        PARTITION BY metrics_path
      )                                                       AS last_major_version_with_counter,
      LAST_VALUE(flattened_usage_data.minor_version) OVER (
        PARTITION BY metrics_path 
        ORDER BY release_date ASC
      )                                                       AS last_minor_version_with_counter,
      COUNT(DISTINCT instance_id) OVER (PARTITION BY metrics_path)    AS count_instances
    FROM flattened_usage_data
    LEFT JOIN dim_gitlab_releases
      ON flattened_usage_data.major_minor_version = dim_gitlab_releases.major_minor_version
    WHERE TRY_TO_DECIMAL(metric_value::TEXT) > 0
      -- Removing SaaS
      AND instance_id <> 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
      -- Removing pre-releases
      AND version NOT LIKE '%pre'

)

SELECT *
FROM transformed
