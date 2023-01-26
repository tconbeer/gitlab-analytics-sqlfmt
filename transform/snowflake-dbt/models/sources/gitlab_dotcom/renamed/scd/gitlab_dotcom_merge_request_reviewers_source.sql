{{ config({"materialized": "table"}) }}

with
    {{ distinct_source(source=source("gitlab_dotcom", "merge_request_reviewers")) }},
    renamed as (

        select

            id::number as merge_request_reviewer_id,
            user_id::number as user_id,
            merge_request_id::number as merge_request_id,
            state::integer as reviewer_state,
            created_at::timestamp as created_at,
            valid_from  -- Column was added in distinct_source CTE

        from distinct_source

    )

    {{
        scd_type_2(
            primary_key_renamed="merge_request_reviewer_id", primary_key_raw="id"
        )
    }}
