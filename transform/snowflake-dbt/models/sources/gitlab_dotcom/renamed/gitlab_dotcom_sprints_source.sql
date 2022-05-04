WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_sprints_dedupe_source') }}

), parsed_columns AS (

    SELECT
      id::NUMBER                      AS sprint_id,
      created_at::TIMESTAMP           AS created_at,
      updated_at::TIMESTAMP           AS updated_at,
      start_date::DATE                AS start_date,
      due_date::DATE                  AS due_date,
      project_id::NUMBER              AS project_id,
      group_id::NUMBER                AS group_id,
      iid::NUMBER                     AS sprint_iid,
      cached_markdown_version::NUMBER AS cached_markdown_version,
      title::VARCHAR                  AS sprint_title,
      title_html::VARCHAR             AS sprint_title_html,
      description::VARCHAR            AS sprint_description,
      description_html::VARCHAR       AS sprint_description_html,
      state_enum::NUMBER              AS sprint_state_enum
    FROM source

)

SELECT *
FROM parsed_columns
