{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('prep_project', 'prep_project')
]) }}

, gitlab_dotcom_labels_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_labels_source')}}

), renamed AS (
  
    SELECT
      gitlab_dotcom_labels_source.label_id       AS dim_label_id,
      -- FOREIGN KEYS
      prep_project.dim_project_id,
      --
      gitlab_dotcom_labels_source.group_id       AS dim_namespace_id,
      gitlab_dotcom_labels_source.label_title,
      gitlab_dotcom_labels_source.label_type,
      gitlab_dotcom_labels_source.created_at,
      gitlab_dotcom_labels_source.updated_at
    FROM gitlab_dotcom_labels_source
    LEFT JOIN prep_project ON gitlab_dotcom_labels_source.project_id = prep_project.dim_project_id

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@dtownsend",
    updated_by="@dtownsend",
    created_date="2021-07-15",
    updated_date="2021-07-15"
) }}