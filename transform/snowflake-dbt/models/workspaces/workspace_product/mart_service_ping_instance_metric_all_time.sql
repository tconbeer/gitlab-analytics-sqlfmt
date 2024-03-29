{{
    config(
        tags=["product", "mnpi_exception"],
        materialized="incremental",
        unique_key="mart_service_ping_instance_metric_id",
    )
}}

{{
    simple_cte(
        [("mart_service_ping_instance_metric", "mart_service_ping_instance_metric")]
    )
}},
final as (

    select
        {{
            dbt_utils.star(
                from=ref("mart_service_ping_instance_metric"),
                except=[
                    "CREATED_BY",
                    "UPDATED_BY",
                    "MODEL_CREATED_DATE",
                    "MODEL_UPDATED_DATE",
                    "DBT_CREATED_AT",
                    "DBT_UPDATED_AT",
                ],
            )
        }}
    from mart_service_ping_instance_metric
    where
        time_frame = 'all'
        {% if is_incremental() %}
            and ping_created_at >= (select max(ping_created_at) from {{ this }})
        {% endif %}

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-04-08",
        updated_date="2022-04-08",
    )
}}
