with
    greenhouse_openings as (select * from {{ ref("greenhouse_openings_source") }}),
    custom_fields as (

        select
            opening_id,
            {{
                dbt_utils.pivot(
                    "opening_custom_field",
                    dbt_utils.get_column_values(
                        ref("greenhouse_opening_custom_fields_source"),
                        "opening_custom_field",
                    ),
                    agg="MAX",
                    then_value="opening_custom_field_display_value",
                    else_value="NULL",
                    quote_identifiers=False,
                )
            }}
        from {{ ref("greenhouse_opening_custom_fields_source") }}
        group by opening_id

    ),
    final as (

        select
            custom_fields.opening_id as job_opening_id,
            greenhouse_openings.job_id,
            greenhouse_openings.opening_id,
            custom_fields.type as job_opening_type,
            hiring_manager,
            finance_id
        from custom_fields
        left join
            greenhouse_openings
            on custom_fields.opening_id = greenhouse_openings.job_opening_id

    )

select *
from final
