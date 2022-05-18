{{ config(tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("map_merged_crm_account", "map_merged_crm_account"),
            ("zuora_contact", "zuora_contact_source"),
        ]
    )
}}

,
snapshot_dates as (

    select *
    from {{ ref("dim_date") }}
    where date_actual >= '2020-03-01' and date_actual <= current_date

),
zuora_account as (

    select *
    from {{ ref("zuora_account_snapshots_source") }}
    where is_deleted = false and lower(live_batch) != 'batch20'

),
zuora_account_spined as (

    select snapshot_dates.date_id as snapshot_id, zuora_account.*
    from zuora_account
    inner join
        snapshot_dates
        on snapshot_dates.date_actual >= zuora_account.dbt_valid_from
        and snapshot_dates.date_actual
        < {{ coalesce_to_infinity("zuora_account.dbt_valid_to") }}

),
joined as (

    select
        zuora_account_spined.snapshot_id,
        zuora_account_spined.account_id as dim_billing_account_id,
        map_merged_crm_account.dim_crm_account_id,
        zuora_account_spined.account_number as billing_account_number,
        zuora_account_spined.account_name as billing_account_name,
        zuora_account_spined.status as account_status,
        zuora_account_spined.parent_id,
        zuora_account_spined.sfdc_account_code,
        zuora_account_spined.currency as account_currency,
        zuora_contact.country as sold_to_country,
        zuora_account_spined.ssp_channel,
        zuora_account_spined.po_required,
        zuora_account_spined.is_deleted,
        zuora_account_spined.batch
    from zuora_account_spined
    left join
        zuora_contact on coalesce(
            zuora_account_spined.sold_to_contact_id,
            zuora_account_spined.bill_to_contact_id
        ) = zuora_contact.contact_id
    left join
        map_merged_crm_account
        on zuora_account_spined.crm_id = map_merged_crm_account.sfdc_account_id

),
final as (

    select
        {{ dbt_utils.surrogate_key(["snapshot_id", "dim_billing_account_id"]) }}
        as billing_account_snapshot_id,
        joined.*
    from joined

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@iweeks",
        updated_by="@jpeguero",
        created_date="2021-08-09",
        updated_date="2021-10-21",
    )
}}
