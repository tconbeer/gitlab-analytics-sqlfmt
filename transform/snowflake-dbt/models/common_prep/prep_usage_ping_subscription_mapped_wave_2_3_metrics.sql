{{ config(tags=["product", "mnpi_exception"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_usage_ping_id"}) }}

{{ simple_cte([("instance_types", "dim_host_instance_type")]) }},
prep_usage_ping as (

    select *
    from {{ ref("prep_usage_ping_subscription_mapped") }}
    where license_md5 is not null

),
final as (

    select

        {{ default_usage_ping_information() }}

        instance_types.instance_type,
        -- subscription_info
        is_usage_ping_license_in_licensedot,
        dim_license_id,
        dim_subscription_id,
        is_license_mapped_to_subscription,
        is_license_subscription_id_valid,
        prep_usage_ping.dim_crm_account_id,
        dim_parent_crm_account_id,
        dim_location_country_id,
        license_user_count,

        {{ sales_wave_2_3_metrics() }}

    from prep_usage_ping
    left join
        instance_types
        on prep_usage_ping.raw_usage_data_payload['uuid']::varchar
        = instance_types.instance_uuid
        and prep_usage_ping.raw_usage_data_payload['hostname']::varchar
        = instance_types.instance_hostname
    qualify
        row_number() over (partition by dim_usage_ping_id order by ping_created_at desc)
        = 1
)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@kathleentam",
        updated_by="@michellecooper",
        created_date="2021-01-10",
        updated_date="2021-04-30",
    )
}}
