{{ config(tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("crm_accounts", "dim_crm_account"),
            ("gainsight_instance_info", "gainsight_instance_info_source"),
        ]
    )
}},
final as (

    select distinct
        gainsight_instance_info.instance_uuid as instance_uuid,
        gainsight_instance_info.instance_hostname as instance_hostname,
        gainsight_instance_info.namespace_id as namespace_id,
        gainsight_instance_info.instance_type as instance_type,
        {{ get_keyed_nulls("crm_accounts.dim_crm_account_id") }} as dim_crm_account_id,
        crm_accounts.crm_account_name
    from gainsight_instance_info
    left join
        crm_accounts
        on gainsight_instance_info.crm_account_id = crm_accounts.dim_crm_account_id
)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@snalamaru",
        updated_by="@snalamaru",
        created_date="2021-04-01",
        updated_date="2021-10-04",
    )
}}
