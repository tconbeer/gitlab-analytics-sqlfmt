{{
    config(
        {
            "alias": "dim_date",
            "post-hook": '{{ apply_dynamic_data_masking(columns = [{"updated_by":"string"},{"created_by":"string"}]) }}',
        }
    )
}}

with
    dates as (select * from {{ ref("date_details") }}),
    final as (select {{ get_date_id("date_actual") }} as date_id, * from dates)

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@msendal",
            updated_by="@michellecooper",
            created_date="2020-06-01",
            updated_date="2022-03-04",
        )
    }}
