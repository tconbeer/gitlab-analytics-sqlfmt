WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_notes_dedupe_source') }}

), renamed AS (

    SELECT
      id::NUMBER                                           AS note_id,
      note::VARCHAR                                         AS note,
      IFF(noteable_type = '', NULL, noteable_type)::VARCHAR AS noteable_type,
      author_id::NUMBER                                    AS note_author_id,
      created_at::TIMESTAMP                                 AS created_at,
      updated_at::TIMESTAMP                                 AS updated_at,
      project_id::NUMBER                                   AS project_id,
      attachment::VARCHAR                                   AS attachment,
      line_code::VARCHAR                                    AS line_code,
      commit_id::VARCHAR                                    AS commit_id,
      noteable_id::NUMBER                                  AS noteable_id,
      system::BOOLEAN                                       AS system,
      --st_diff (hidden because not relevant to our current analytics needs)
      updated_by_id::NUMBER                                AS note_updated_by_id,
      --type (hidden because legacy and can be easily confused with noteable_type)
      position::VARCHAR                                     AS position,
      original_position::VARCHAR                            AS original_position,
      resolved_at::TIMESTAMP                                AS resolved_at,
      resolved_by_id::NUMBER                               AS resolved_by_id,
      discussion_id::VARCHAR                                AS discussion_id,
      cached_markdown_version::NUMBER                      AS cached_markdown_version,
      resolved_by_push::BOOLEAN                             AS resolved_by_push
    FROM source

)

SELECT *
FROM renamed
WHERE note_id NOT IN (
  203215238 --https://gitlab.com/gitlab-data/analytics/merge_requests/1423
)
