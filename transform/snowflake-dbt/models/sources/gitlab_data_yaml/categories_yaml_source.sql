with
    source as (

        select
            *,
            rank() over (
                partition by date_trunc('day', uploaded_at) order by uploaded_at desc
            ) as rank
        from {{ source("gitlab_data_yaml", "categories") }}
        order by uploaded_at desc

    ),
    intermediate as (

        select
            d.value as data_by_row,
            date_trunc('day', uploaded_at)::date as snapshot_date,
            rank
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['acquisition_appetite']::varchar as acquisition_appetite,
            data_by_row['alt_link']::varchar as alt_link,
            try_to_timestamp(data_by_row['available']::varchar) as available,
            data_by_row['body']::varchar as body,
            try_to_timestamp(data_by_row['complete']::varchar) as complete,
            data_by_row['description']::varchar as description,
            data_by_row['direction']::varchar as direction,
            data_by_row['documentation']::varchar as documentation,
            data_by_row['feature_labels']::varchar as feature_labels,
            try_to_timestamp(data_by_row['lovable']::varchar) as lovable,
            data_by_row['label']::varchar as label,
            try_to_boolean(data_by_row['marketing']::varchar) as marketing,
            data_by_row['marketing_page']::varchar as marketing_page,
            data_by_row['maturity']::varchar as maturity,
            data_by_row['name']::varchar as name,
            try_to_boolean(data_by_row['new_maturity']::varchar) as new_maturity,
            data_by_row['partnership_appetite']::varchar as partnership_appetite,
            data_by_row['priority_level']::varchar as priority_level,
            data_by_row['roi']::varchar as roi,
            data_by_row['stage']::varchar as stage,
            try_to_timestamp(data_by_row['viable']::varchar) as viable,
            snapshot_date,
            rank
        from intermediate

    )

select *
from renamed
