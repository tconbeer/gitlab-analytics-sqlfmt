{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "mart_available_to_renew_snapshot_id",
            "tags": ["edm_snapshot", "atr_snapshots"],
        }
    )
}}

with
    snapshot_dates as (

        select *
        from {{ ref("dim_date") }}
        where
            date_actual >= '2020-03-01' and date_actual <= current_date
            {% if is_incremental() %}

            -- this filter will only be applied on an incremental run
            and date_id > (select max(snapshot_id) from {{ this }}) {% endif %}

    ),
    mart_available_to_renew as (

        select * from {{ ref("prep_mart_available_to_renew_snapshot_base") }}

    ),
    mart_available_to_renew_spined as (

        select
            snapshot_dates.date_id as snapshot_id,
            snapshot_dates.date_actual as snapshot_date,
            mart_available_to_renew.*
        from mart_available_to_renew
        inner join
            snapshot_dates
            on snapshot_dates.date_actual >= mart_available_to_renew.dbt_valid_from
            and snapshot_dates.date_actual
            < {{ coalesce_to_infinity("mart_available_to_renew.dbt_valid_to") }}

    ),
    final as (

        select
            {{ dbt_utils.surrogate_key(["snapshot_id", "primary_key"]) }}
            as mart_available_to_renew_snapshot_id,
            *
        from mart_available_to_renew_spined

    )

select *
from final
