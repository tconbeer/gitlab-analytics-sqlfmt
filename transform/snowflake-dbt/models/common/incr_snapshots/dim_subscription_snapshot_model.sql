{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "dim_subscription_snapshot_id",
            "tags": ["edm_snapshot", "subscription_snapshot"],
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
    dim_subscription as (

        select * from {{ ref("prep_dim_subscription_snapshot_base") }}

    ),
    dim_subscription_spined as (

        select snapshot_dates.date_id as snapshot_id, dim_subscription.*
        from dim_subscription
        inner join
            snapshot_dates
            on snapshot_dates.date_actual >= dim_subscription.dbt_valid_from
            and snapshot_dates.date_actual
            < {{ coalesce_to_infinity("dim_subscription.dbt_valid_to") }}

    ),
    final as (

        select
            {{ dbt_utils.surrogate_key(["snapshot_id", "dim_subscription_id"]) }}
            as dim_subscription_snapshot_id,
            *
        from dim_subscription_spined

    )

select *
from final
