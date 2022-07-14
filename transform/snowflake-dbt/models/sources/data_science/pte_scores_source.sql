with
    source as (select * from {{ source("data_science", "pte_scores") }}),
    intermediate as (

        select d.value as data_by_row, uploaded_at
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    parsed as (

        select

            data_by_row['crm_account_id']::varchar as crm_account_id,
            data_by_row['decile']::int as decile,
            data_by_row['grouping']::int as score_group,
            data_by_row['importance']::int as importance,
            data_by_row['score']::number(38, 4) as score,
            data_by_row['insights']::varchar as insights,
            uploaded_at::timestamp as uploaded_at

        from intermediate

    )
select *
from parsed
