    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_approval_merge_request_rules_dedupe_source') }}
  
), renamed AS (

  SELECT
    id::NUMBER                      AS approval_merge_request_rule_id,
    merge_request_id::NUMBER        AS merge_request_id,
    approvals_required::NUMBER      AS is_approvals_required,
    rule_type::VARCHAR              AS rule_type,
    report_type::VARCHAR            AS report_type,
    created_at::TIMESTAMP           AS created_at,
    updated_at::TIMESTAMP           AS updated_at

  FROM source

)

SELECT *
FROM renamed
