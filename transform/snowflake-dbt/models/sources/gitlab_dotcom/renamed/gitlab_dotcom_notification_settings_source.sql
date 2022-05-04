WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_notification_settings_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                                     AS notification_settings_id,
      user_id::NUMBER                                AS user_id,
      source_id::NUMBER                              AS source_id,
      source_type::VARCHAR                            AS source_type,
      level::NUMBER                                  AS settings_level,
      created_at::TIMESTAMP                           AS created_at,
      updated_at::TIMESTAMP                           AS updated_at,
      new_note::BOOLEAN                               AS has_new_note_enabled,
      new_issue::BOOLEAN                              AS has_new_issue_enabled,
      reopen_issue::BOOLEAN                           AS has_reopen_issue_enabled,
      close_issue::BOOLEAN                            AS has_close_issue_enabled,
      reassign_issue::BOOLEAN                         AS has_reassign_issue_enabled,
      new_merge_request::BOOLEAN                      AS has_new_merge_request_enabled,
      reopen_merge_request::BOOLEAN                   AS has_reopen_merge_request_enabled,
      close_merge_request::BOOLEAN                    AS has_close_merge_request_enabled,
      reassign_merge_request::BOOLEAN                 AS has_reassign_merge_request_enabled,
      merge_merge_request::BOOLEAN                    AS has_merge_merge_request_enabled,
      failed_pipeline::BOOLEAN                        AS has_failed_pipeline_enabled,
      success_pipeline::BOOLEAN                       AS has_success_pipeline_enabled

    FROM source

)

SELECT *
FROM renamed
