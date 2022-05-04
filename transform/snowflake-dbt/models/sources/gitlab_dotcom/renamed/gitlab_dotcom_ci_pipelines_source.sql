WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_pipelines_dedupe_source') }}
  WHERE created_at IS NOT NULL

), renamed AS (

  SELECT
    id::NUMBER                   AS ci_pipeline_id,
    created_at::TIMESTAMP         AS created_at,
    updated_at::TIMESTAMP         AS updated_at,
    ref::VARCHAR                  AS ref,
    tag::BOOLEAN                  AS has_tag,
    yaml_errors::VARCHAR          AS yaml_errors,
    committed_at::TIMESTAMP       AS committed_at,
    project_id::NUMBER           AS project_id,
    status::VARCHAR               AS status,
    started_at::TIMESTAMP         AS started_at,
    finished_at::TIMESTAMP        AS finished_at,
    duration::NUMBER             AS ci_pipeline_duration,
    user_id::NUMBER              AS user_id,
    lock_version::NUMBER         AS lock_version,
    auto_canceled_by_id::NUMBER  AS auto_canceled_by_id,
    pipeline_schedule_id::NUMBER AS pipeline_schedule_id,
    source::NUMBER               AS ci_pipeline_source,
    config_source::NUMBER        AS config_source,
    protected::BOOLEAN            AS is_protected,
    failure_reason::VARCHAR       AS failure_reason,
    iid::NUMBER                  AS ci_pipeline_iid,
    merge_request_id::NUMBER     AS merge_request_id
  FROM source

)

SELECT *
FROM renamed
