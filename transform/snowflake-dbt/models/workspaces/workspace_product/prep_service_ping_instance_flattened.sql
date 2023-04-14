{{
    config(
        tags=["product", "mnpi_exception"],
        materialized="incremental",
        unique_key="prep_service_ping_instance_flattened_id",
    )
}}


with
    source as (

        select *
        from {{ ref("prep_service_ping_instance") }} as usage
        {% if is_incremental() %}
            where ping_created_at >= (select max(ping_created_at) from {{ this }})
        {% endif %}

    ),
    flattened_high_level as (
        select
            {{ dbt_utils.surrogate_key(["dim_service_ping_instance_id", "path"]) }}
            as prep_service_ping_instance_flattened_id,
            dim_service_ping_instance_id as dim_service_ping_instance_id,
            dim_host_id as dim_host_id,
            dim_instance_id as dim_instance_id,
            dim_installation_id as dim_installation_id,
            ping_created_at as ping_created_at,
            ip_address_hash as ip_address_hash,
            license_md5 as license_md5,
            original_edition as original_edition,
            main_edition as main_edition,
            product_tier as product_tier,
            to_date(
                source.raw_usage_data_payload:license_trial_ends_on::text
            ) as license_trial_ends_on,
            (
                source.raw_usage_data_payload:license_subscription_id::text
            ) as license_subscription_id,
            source.raw_usage_data_payload:usage_activity_by_stage_monthly.manage.events
            ::number as umau_value,
            path as metrics_path,
            iff(value = -1, 0, value) as metric_value,
            iff(value = -1, true, false) as has_timed_out
        from source, lateral flatten(input => raw_usage_data_payload, recursive => true)

    )

    {{
        dbt_audit(
            cte_ref="flattened_high_level",
            created_by="@icooper-acp",
            updated_by="@icooper-acp",
            created_date="2022-03-17",
            updated_date="2022-03-17",
        )
    }}
