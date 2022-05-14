{{
    config(
        tags=["product", "mnpi_exception"],
        materialized="incremental",
        unique_key="fct_service_ping_instance_metric_id",
    )
}}

{%- set settings_columns = dbt_utils.get_column_values(
    table=ref("prep_usage_ping_metrics_setting"),
    column="metrics_path",
    max_records=1000,
    default=[""],
) %}

{{
    simple_cte(
        [
            ("prep_license", "prep_license"),
            ("prep_subscription", "prep_subscription"),
            ("prep_usage_ping_metrics_setting", "prep_usage_ping_metrics_setting"),
            ("dim_date", "dim_date"),
            ("map_ip_to_country", "map_ip_to_country"),
            ("locations", "prep_location_country"),
            ("prep_service_ping_instance", "prep_service_ping_instance_flattened"),
            ("dim_service_ping_metric", "dim_service_ping_metric"),
        ]
    )
}}

,
map_ip_location as (

    select
        map_ip_to_country.ip_address_hash as ip_address_hash,
        map_ip_to_country.dim_location_country_id as dim_location_country_id
    from map_ip_to_country
    inner join locations
    where map_ip_to_country.dim_location_country_id = locations.dim_location_country_id

),
source as (

    select prep_service_ping_instance.*
    from prep_service_ping_instance
    {% if is_incremental() %}
    where ping_created_at >= (select max(ping_created_at) from {{ this }})
    {% endif %}

),
add_country_info_to_usage_ping as (

    select source.*, map_ip_location.dim_location_country_id as dim_location_country_id
    from source
    left join
        map_ip_location on source.ip_address_hash = map_ip_location.ip_address_hash

),
dim_product_tier as (select * from {{ ref("dim_product_tier") }}),
prep_usage_ping_cte as (

    select
        dim_service_ping_instance_id as dim_service_ping_instance_id,
        dim_host_id as dim_host_id,
        dim_instance_id as dim_instance_id,
        dim_installation_id as dim_installation_id,
        dim_product_tier.dim_product_tier_id as dim_product_tier_id,
        ping_created_at as ping_created_at,
        license_md5 as license_md5,
        dim_location_country_id as dim_location_country_id,
        license_trial_ends_on as license_trial_ends_on,
        license_subscription_id as license_subscription_id,
        umau_value as umau_value,
        product_tier as product_tier,
        main_edition as main_edition,
        metrics_path as metrics_path,
        metric_value as metric_value,
        has_timed_out as has_timed_out
    from add_country_info_to_usage_ping
    left join
        dim_product_tier on trim(
            lower(add_country_info_to_usage_ping.product_tier)
        ) = trim(lower(dim_product_tier.product_tier_historical_short)) and iff(
            add_country_info_to_usage_ping.dim_instance_id
            = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f',
            'SaaS',
            'Self-Managed'
        ) = dim_product_tier.product_delivery_type
-- AND main_edition = 'EE'
),
joined_payload as (

    select
        prep_usage_ping_cte.*,
        prep_license.dim_license_id as dim_license_id,
        dim_date.date_id as dim_service_ping_date_id,
        coalesce(
            license_subscription_id, prep_subscription.dim_subscription_id
        ) as dim_subscription_id,
        iff(
            prep_usage_ping_cte.ping_created_at < license_trial_ends_on, true, false
        ) as is_trial
    from prep_usage_ping_cte
    left join prep_license on prep_usage_ping_cte.license_md5 = prep_license.license_md5
    left join
        prep_subscription
        on prep_license.dim_subscription_id = prep_subscription.dim_subscription_id
    left join
        dim_date on to_date(prep_usage_ping_cte.ping_created_at) = dim_date.date_day

),
flattened_high_level as (
    select
        dim_service_ping_instance_id as dim_service_ping_instance_id,
        metrics_path as metrics_path,
        metric_value as metric_value,
        has_timed_out as has_timed_out,
        dim_product_tier_id as dim_product_tier_id,
        dim_subscription_id as dim_subscription_id,
        dim_location_country_id as dim_location_country_id,
        dim_service_ping_date_id as dim_service_ping_date_id,
        dim_instance_id as dim_instance_id,
        dim_host_id as dim_host_id,
        dim_installation_id as dim_installation_id,
        dim_license_id as dim_license_id,
        ping_created_at as ping_created_at,
        umau_value as umau_value,
        license_subscription_id as dim_subscription_license_id,
        'VERSION_DB' as data_source
    from joined_payload

),
-- WHERE time_frame != 'none'
metric_attributes as (select * from dim_service_ping_metric),
final as (

    select
        {{
            dbt_utils.surrogate_key(
                ["dim_service_ping_instance_id", "flattened_high_level.metrics_path"]
            )
        }} as fct_service_ping_instance_metric_id,
        flattened_high_level.*,
        metric_attributes.time_frame as time_frame
    from flattened_high_level
    left join
        metric_attributes
        on flattened_high_level.metrics_path = metric_attributes.metrics_path

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-03-08",
        updated_date="2022-03-11",
    )
}}
