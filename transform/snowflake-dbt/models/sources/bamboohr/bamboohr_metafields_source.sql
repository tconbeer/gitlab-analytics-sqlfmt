with
    source as (

        select *
        from {{ source("bamboohr", "meta_fields") }}
        order by uploaded_at desc
        limit 1

    ),
    renamed as (

        select
            metafield.value['id']::int as metafield_id,
            metafield.value['name']::varchar as metafield_name,
            metafield.value['alias']::varchar as metafield_alias_name
        from
            source,
            lateral flatten(input => parse_json(jsontext), outer => true) metafield

    )

select *
from renamed
