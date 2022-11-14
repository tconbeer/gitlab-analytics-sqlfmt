{%- macro scd_type_2(
    primary_key_renamed,
    primary_key_raw,
    source_cte="distinct_source",
    casted_cte="renamed"
) -%}

,
max_by_primary_key as (

    select
        {{ primary_key_raw }} as primary_key,
        max(
            iff(
                max_task_instance
                in (select max(max_task_instance) from {{ source_cte }}),
                1,
                0
            )
        ) as is_in_most_recent_task,
        max(max_uploaded_at) as max_timestamp
    from {{ source_cte }}
    group by 1

),
windowed as (

    select
        {{ casted_cte }}.*,

        -- First, look for the row immediately following by PK and subtract one
        -- millisecond from its timestamp.
        coalesce(
            dateadd(
                'millisecond',
                -1,
                lead(valid_from) over (
                    partition by {{ casted_cte }}.{{ primary_key_renamed }}
                    order by valid_from
                )
            ),
            -- If row has no following rows, check when it's valid until (NULL if it
            -- appeared in latest task instance.)
            iff(is_in_most_recent_task = false, max_by_primary_key.max_timestamp, null)
        ) as valid_to,
        iff(valid_to is null, true, false) as is_currently_valid

    from {{ casted_cte }}
    left join
        max_by_primary_key
        on {{ casted_cte }}.{{ primary_key_renamed }} = max_by_primary_key.primary_key
    order by valid_from, valid_to

)

select *
from windowed

{%- endmacro -%}
