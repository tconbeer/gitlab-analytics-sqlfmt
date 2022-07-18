{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_usage_ping_id"}) }}

{{
    simple_cte(
        [
            ("usage_pings", "prep_usage_ping"),
            ("map_license_account", "map_license_subscription_account"),
            ("instance_types", "dim_host_instance_type"),
        ]
    )
}},
core_usage_pings as (

    select
        usage_pings.dim_usage_ping_id,
        usage_pings.dim_product_tier_id,
        usage_pings.ping_created_at,
        usage_pings.ping_created_at_28_days_earlier,
        usage_pings.ping_created_at_year,
        usage_pings.ping_created_at_month,
        usage_pings.ping_created_at_week,
        usage_pings.ping_created_at_date,
        usage_pings.raw_usage_data_id,
        usage_pings.raw_usage_data_payload,
        usage_pings.license_md5,
        usage_pings.original_edition,
        usage_pings.edition,
        usage_pings.main_edition,
        usage_pings.product_tier,
        usage_pings.main_edition_product_tier,
        usage_pings.cleaned_version,
        usage_pings.version_is_prerelease,
        usage_pings.major_version,
        usage_pings.minor_version,
        usage_pings.major_minor_version,
        usage_pings.ping_source,
        usage_pings.is_internal,
        usage_pings.is_staging,
        usage_pings.dim_location_country_id,
        map_license_account.dim_subscription_id,
        map_license_account.dim_crm_account_id,
        map_license_account.dim_parent_crm_account_id
    from usage_pings
    left join
        map_license_account on usage_pings.license_md5 = map_license_account.license_md5
    where usage_pings.product_tier = 'Core'

),
joined as (

    select

        {{ default_usage_ping_information() }}

        instance_types.instance_type,
        core_usage_pings.dim_subscription_id,
        core_usage_pings.dim_crm_account_id,
        core_usage_pings.dim_parent_crm_account_id,
        core_usage_pings.dim_location_country_id,

        {{ sales_wave_2_3_metrics() }}

    from core_usage_pings
    left join
        instance_types
        on core_usage_pings.raw_usage_data_payload['uuid']::varchar
        = instance_types.instance_uuid
        and core_usage_pings.raw_usage_data_payload['hostname']::varchar
        = instance_types.instance_hostname
    qualify
        row_number() over (partition by dim_usage_ping_id order by ping_created_at desc)
        = 1
)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@ischweickartDD",
        updated_by="@ischweickartDD",
        created_date="2021-06-04",
        updated_date="2021-06-09",
    )
}}
