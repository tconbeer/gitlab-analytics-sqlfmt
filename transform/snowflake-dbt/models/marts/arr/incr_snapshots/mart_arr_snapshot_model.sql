{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "mart_arr_snapshot_id",
            "tags": ["edm_snapshot", "arr_snapshots"],
            "schema": "restricted_safe_common_mart_sales",
        }
    )
}}

/* grain: one record per subscription, product per month */
with
    snapshot_dates as (

        select *
        from {{ ref("dim_date") }}
        where
            date_actual >= '2020-03-01' and date_actual <= current_date
            {% if is_incremental() %}

                -- this filter will only be applied on an incremental run
                and date_id > (select max(snapshot_id) from {{ this }})

            {% endif %}

    ),
    mart_arr as (select * from {{ ref("prep_mart_arr_snapshot_base") }}),
    mart_arr_spined as (

        select
            snapshot_dates.date_id as snapshot_id,
            snapshot_dates.date_actual as snapshot_date,
            mart_arr.*
        from mart_arr
        inner join
            snapshot_dates
            on snapshot_dates.date_actual >= mart_arr.dbt_valid_from
            and snapshot_dates.date_actual
            < {{ coalesce_to_infinity("mart_arr.dbt_valid_to") }}

    ),
    final as (

        select
            {{ dbt_utils.surrogate_key(["snapshot_id", "primary_key"]) }}
            as mart_arr_snapshot_id,
            *
        from mart_arr_spined

    )

select *
from final
