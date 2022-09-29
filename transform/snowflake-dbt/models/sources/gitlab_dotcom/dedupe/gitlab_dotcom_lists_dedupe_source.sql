{{ config({"materialized": "incremental", "unique_key": "id"}) }}

with
    source as (select * from {{ source("gitlab_dotcom", "lists") }}),
    partitioned as (

        select *
        from source

        {% if is_incremental() %}

        where updated_at >= (select max(updated_at) from {{ this }})

        {% endif %}

        qualify row_number() over (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select
            id as id,
            board_id as board_id,
            label_id as label_id,
            list_type as list_type,
            position as position,
            created_at as created_at,
            updated_at as updated_at,
            user_id::number as user_id,
            milestone_id as milestone_id,
            max_issue_count as max_issue_count,
            max_issue_weight as max_issue_weight,
            limit_metric as limit_metric,
            _uploaded_at as _uploaded_at
        from partitioned

    )

select *
from renamed
