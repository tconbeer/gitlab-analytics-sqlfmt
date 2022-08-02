with
    sfdc_zqu_quote_source as (

        select * from {{ ref("sfdc_zqu_quote_source") }} where is_deleted = false

    ),
    final as (

        select
            quote_id as dim_quote_id,
            zqu__number as quote_number,
            zqu_quote_name as quote_name,
            zqu__status as quote_status,
            zqu__primary as is_primary_quote,
            zqu__start_date as quote_start_date
        from sfdc_zqu_quote_source

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@snalamaru",
            updated_by="@snalamaru",
            created_date="2021-01-07",
            updated_date="2021-01-07",
        )
    }}
