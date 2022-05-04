WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_requests_closing_issues_dedupe_source') }}
  
), renamed AS (

    SELECT DISTINCT 
      id::NUMBER                AS merge_request_issue_relation_id,
      merge_request_id::NUMBER  AS merge_request_id,
      issue_id::NUMBER          AS issue_id,
      created_at::TIMESTAMP      AS created_at,
      updated_at::TIMESTAMP      AS updated_at

    FROM source

)

SELECT *
FROM renamed
