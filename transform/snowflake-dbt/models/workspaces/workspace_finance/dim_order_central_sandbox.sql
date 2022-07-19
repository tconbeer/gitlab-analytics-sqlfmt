with
    base as (

        select *
        from {{ ref("zuora_central_sandbox_order_source") }}
        where is_deleted = false

    ),
    final as (

        select

            order_id as dim_order_id,
            description as order_description,
            created_date as order_created_date,
            order_date,
            order_number,
            state as order_state,
            status as order_status,
            created_by_migration as is_created_by_migration

        from base

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@michellecooper",
            updated_by="@michellecooper",
            created_date="2022-03-31",
            updated_date="2022-03-31",
        )
    }}
