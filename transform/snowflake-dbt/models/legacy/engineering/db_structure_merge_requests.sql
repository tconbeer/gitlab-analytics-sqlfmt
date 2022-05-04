WITH source AS (

    SELECT *
    FROM {{ ref('engineering_part_of_product_merge_requests_source') }}

), mr_information AS (

    SELECT * FROM
    {{ ref('gitlab_dotcom_merge_requests_xf') }}
    WHERE ARRAY_CONTAINS('database::approved'::VARIANT, labels)
    AND merged_at IS NOT NULL
    AND project_id = 278964 --where the db schema is

), changes_to_db_structure AS (

    SELECT 
      DISTINCT
      'gitlab.com' || plain_diff_url_path AS mr_path,
      mr_information.merge_request_updated_at,
      merged_at
    FROM source
      INNER JOIN LATERAL FLATTEN(INPUT => file_diffs, outer => true) d
      INNER JOIN mr_information
        ON source.product_merge_request_iid = mr_information.merge_request_iid
    WHERE target_branch_name = 'master' 
      AND d.value['file_path'] = 'db/structure.sql'

)

SELECT *
FROM changes_to_db_structure
ORDER BY merged_at DESC