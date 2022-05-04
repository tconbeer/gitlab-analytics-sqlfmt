    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_issues_dedupe_source') }}
  WHERE created_at::VARCHAR NOT IN ('0001-01-01 12:00:00','1000-01-01 12:00:00','10000-01-01 12:00:00')
    AND LEFT(created_at::VARCHAR , 10) != '1970-01-01'
  
), renamed AS (

    SELECT

      id::NUMBER                                               AS issue_id,
      iid::NUMBER                                              AS issue_iid,
      author_id::NUMBER                                        AS author_id,
      source.project_id::NUMBER                                AS project_id,
      milestone_id::NUMBER                                     AS milestone_id,
      sprint_id::NUMBER                                        AS sprint_id,
      updated_by_id::NUMBER                                    AS updated_by_id,
      last_edited_by_id::NUMBER                                AS last_edited_by_id,
      moved_to_id::NUMBER                                      AS moved_to_id,
      created_at::TIMESTAMP                                    AS created_at,
      updated_at::TIMESTAMP                                    AS updated_at,
      last_edited_at::TIMESTAMP                                AS issue_last_edited_at,
      closed_at::TIMESTAMP                                     AS issue_closed_at,
      confidential::BOOLEAN                                    AS is_confidential,
      title::VARCHAR                                           AS issue_title,
      description::VARCHAR                                     AS issue_description,

      -- Override state by mapping state_id. See issue #3344.
      {{ map_state_id('state_id') }}                           AS state,

      weight::NUMBER                                           AS weight,
      due_date::DATE                                           AS due_date,
      lock_version::NUMBER                                     AS lock_version,
      time_estimate::NUMBER                                    AS time_estimate,
      discussion_locked::BOOLEAN                               AS has_discussion_locked,
      closed_by_id::NUMBER                                     AS closed_by_id,
      relative_position::NUMBER                                AS relative_position,
      service_desk_reply_to::VARCHAR                           AS service_desk_reply_to,
      state_id::NUMBER                                         AS state_id,
      duplicated_to_id::NUMBER                                 AS duplicated_to_id,
      promoted_to_epic_id::NUMBER                              AS promoted_to_epic_id,
      issue_type::NUMBER                                       AS issue_type

    FROM source

)

SELECT *
FROM renamed
