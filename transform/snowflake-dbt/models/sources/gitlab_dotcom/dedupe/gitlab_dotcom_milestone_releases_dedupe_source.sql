{{ config({"materialized": "incremental", "unique_key": "primary_key"}) }}

with
    base as (

        select *
        from {{ source("gitlab_dotcom", "milestone_releases") }}
        {% if is_incremental() %}

            where _uploaded_at >= (select max(_uploaded_at) from {{ this }})

        {% endif %}
        qualify
            row_number() over (
                partition by milestone_id, release_id order by _uploaded_at desc
            )
            = 1

    ),
    renamed as (

        select
            {{ dbt_utils.surrogate_key(["milestone_id", "release_id"]) }}
            as primary_key,
            milestone_id::int as milestone_id,
            release_id::int as release_id,
            _uploaded_at as _uploaded_at
        from base

    )

select *
from renamed
