{{ config(tags=["mnpi_exception"]) }}

{{
    config(
        {
            "materialized": "view",
            "alias": "gitlab_dotcom_usage_data_events",
            "post-hook": '{{ apply_dynamic_data_masking(columns = [{"event_primary_key":"string"},{"event_name":"string"},{"namespace_id":"number"},{"user_id":"number"},{"parent_id":"number"},{"plan_id_at_event_date":"string"},{"plan_was_paid_at_event_date":"boolean"}]) }}',
        }
    )
}}


select *
from {{ ref("gitlab_dotcom_usage_data_pipelines") }}

union all

select *
from {{ ref("gitlab_dotcom_usage_data_issues") }}

union all

select *
from {{ ref("gitlab_dotcom_usage_data_notes") }}

union all

select *
from {{ ref("gitlab_dotcom_usage_data_ci_builds") }}
