with
    {{ distinct_source(source=source("gitlab_ops", "label_links")) }},
    renamed as (

        select

            id::number as label_link_id,
            label_id::number as label_id,
            target_id::number as target_id,
            target_type::varchar as target_type,
            created_at::timestamp as label_link_created_at,
            updated_at::timestamp as label_link_updated_at,
            valid_from  -- Column was added in distinct_source CTE

        from distinct_source

    )

    {{ scd_type_2(primary_key_renamed="label_link_id", primary_key_raw="id") }}
