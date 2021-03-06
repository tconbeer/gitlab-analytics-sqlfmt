{{
    config(
        {
            "tags": ["product", "mnpi_exception"],
            "materialized": "table",
            "alias": "fct_usage_ping_payload",
            "post-hook": '{{ apply_dynamic_data_masking(columns = [{"dim_usage_ping_id":"number"},{"instance_user_count":"number"},{"dim_product_tier_id":"string"},{"dim_subscription_id":"string"},{"dim_license_id":"number"},{"raw_usage_data_id":"number"},{"raw_usage_data_payload":"variant"},{"host_name":"string"},{"umau_value":"number"},{"license_subscription_id":"string"},{"created_by":"string"},{"updated_by":"string"} ]) }}',
        }
    )
}}

{{ simple_cte([("prep_usage_ping_payload", "prep_usage_ping_payload")]) }},
final as (

    select
        dim_usage_ping_id,
        dim_product_tier_id,
        dim_subscription_id,
        dim_license_id,
        dim_location_country_id,
        dim_date_id,
        dim_instance_id,
        ping_created_at,
        ping_created_at_28_days_earlier,
        ping_created_at_year,
        ping_created_at_month,
        ping_created_at_week,
        ping_created_at_date,
        raw_usage_data_id,
        raw_usage_data_payload,
        edition,
        product_tier,
        edition_product_tier,
        version_is_prerelease,
        major_version,
        minor_version,
        major_minor_version,
        usage_ping_delivery_type,
        is_internal,
        is_staging,
        is_trial,
        instance_user_count,
        host_name,
        umau_value,
        license_subscription_id
    from prep_usage_ping_payload

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@mpeychet",
        updated_by="@mpeychet",
        created_date="2021-05-10",
        updated_date="2021-07-22",
    )
}}
