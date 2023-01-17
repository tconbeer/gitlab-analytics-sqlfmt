{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "fct_mrr_snapshot_id",
            "tags": ["edm_snapshot", "arr_snapshots"],
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
    fct_mrr as (select * from {{ ref("prep_fct_mrr_snapshot_base") }}),
    prep_charge as (

        select
            prep_charge.*, charge_created_date as valid_from, '9999-12-31' as valid_to
        from {{ ref("prep_charge") }}
        where rate_plan_charge_name = 'manual true up allocation'

    ),
    manual_charges as (

        select
            date_id as snapshot_id,
            {{
                dbt_utils.surrogate_key(
                    ["date_id", "subscription_name", "dim_product_detail_id", "mrr"]
                )
            }} as mrr_id,
            date_id as dim_date_id,
            dim_charge_id as dim_charge_id,
            dim_product_detail_id as dim_product_detail_id,
            dim_subscription_id as dim_subscription_id,
            dim_billing_account_id as dim_billing_account_id,
            dim_crm_account_id as dim_crm_account_id,
            mrr as mrr,
            arr as arr,
            quantity as quantity,
            array_agg(unit_of_measure) as unit_of_measure,
            null as created_by,
            null as updated_by,
            null as model_created_date,
            null as model_updated_date,
            null as dbt_created_at,
            null as dbt_scd_id,
            null as dbt_updated_at,
            valid_from as dbt_valid_from,
            '9999-12-31' as dbt_valid_to,
            subscription_status
        from prep_charge
        inner join
            snapshot_dates
            on snapshot_dates.date_actual >= prep_charge.valid_from
            and snapshot_dates.date_actual
            < coalesce(prep_charge.valid_to, '9999-12-31'::timestamp)
        -- NOTE THE GAP IN THE GROUPINGS BELOW,
        -- We need to group by everything except for unit of measure.
        group by
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22

    ),
    non_manual_charges as (

        select snapshot_dates.date_id as snapshot_id, fct_mrr.*
        from fct_mrr
        inner join
            snapshot_dates
            on snapshot_dates.date_actual >= fct_mrr.dbt_valid_from
            and snapshot_dates.date_actual
            < {{ coalesce_to_infinity("fct_mrr.dbt_valid_to") }}

    ),
    combined_charges as (

        select *
        from manual_charges

        union all

        select *
        from non_manual_charges

    ),
    final as (

        select
            {{ dbt_utils.surrogate_key(["snapshot_id", "mrr_id"]) }}
            as fct_mrr_snapshot_id,
            *
        from combined_charges

    )

select *
from final
