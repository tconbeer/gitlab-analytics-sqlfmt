WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_push_rules_dedupe_source') }}
    
), renamed AS (
  
    SELECT
      id::NUMBER                           AS push_rule_id,
      force_push_regex::VARCHAR             AS force_push_regex,
      delete_branch_regex::VARCHAR          AS delete_branch_regex,
      commit_message_regex::VARCHAR         AS commit_message_regex,
      deny_delete_tag::BOOLEAN              AS deny_delete_tag,
      project_id::NUMBER                   AS project_id,
      created_at::TIMESTAMP                 AS created_at,
      updated_at::TIMESTAMP                 AS updated_at,
      author_email_regex::VARCHAR           AS author_email_regex,
      member_check::BOOLEAN                 AS has_member_check,
      file_name_regex::VARCHAR              AS file_name_regex,
      is_sample::BOOLEAN                    AS is_sample,
      max_file_size::NUMBER                AS max_file_size,
      branch_name_regex::VARCHAR            AS branch_name_regex,
      commit_message_negative_regex::VARCHAR AS commit_message_negative_regex
    FROM source
    
)

SELECT * 
FROM renamed
