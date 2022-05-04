{{ config(tags=["product", "mnpi_exception"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_usage_ping_id"}) }}

{%- set settings_columns = dbt_utils.get_column_values(
    table=ref("prep_usage_ping_metrics_setting"),
    column="metrics_path",
    max_records=1000,
    default=[""],
) %}

{{
    simple_cte(
        [
            ("raw_usage_data", "version_raw_usage_data_source"),
            ("prep_license", "prep_license"),
            ("prep_subscription", "prep_subscription"),
            ("raw_usage_data", "version_raw_usage_data_source"),
            ("prep_usage_ping_metrics_setting", "prep_usage_ping_metrics_setting"),
            ("dim_date", "dim_date"),
            ("prep_usage_ping", "prep_usage_ping"),
        ]
    )
}}

,
joined as (

    select
        {{ dbt_utils.star(from=ref('prep_usage_ping'), relation_alias='prep_usage_ping', except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
        main_edition as edition,
        main_edition_product_tier as edition_product_tier,
        ping_source as usage_ping_delivery_type,
        prep_license.dim_license_id,
        prep_subscription.dim_subscription_id,
        dim_date.date_id,
        to_date(
            raw_usage_data.raw_usage_data_payload:license_trial_ends_on::text
        ) as license_trial_ends_on,
        (
            raw_usage_data.raw_usage_data_payload:license_subscription_id::text
        ) as license_subscription_id,
        raw_usage_data.raw_usage_data_payload:usage_activity_by_stage_monthly.manage.events::number
        as umau_value,
        iff(ping_created_at < license_trial_ends_on, true, false) as is_trial

    from prep_usage_ping
    left join
        raw_usage_data
        on prep_usage_ping.raw_usage_data_id = raw_usage_data.raw_usage_data_id
    left join prep_license on prep_usage_ping.license_md5 = prep_license.license_md5
    left join
        prep_subscription
        on prep_license.dim_subscription_id = prep_subscription.dim_subscription_id
    left join dim_date on to_date(ping_created_at) = dim_date.date_day

),
dim_product_tier as (

    select *
    from {{ ref("dim_product_tier") }}
    where product_delivery_type = 'Self-Managed'

),
final as (

    select
        joined.dim_usage_ping_id,
        dim_product_tier.dim_product_tier_id as dim_product_tier_id,
        coalesce(license_subscription_id, dim_subscription_id) as dim_subscription_id,
        dim_license_id,
        dim_location_country_id,
        date_id as dim_date_id,
        dim_instance_id,

        -- timestamps
        ping_created_at,
        dateadd('days', -28, ping_created_at) as ping_created_at_28_days_earlier,
        date_trunc('YEAR', ping_created_at) as ping_created_at_year,
        date_trunc('MONTH', ping_created_at) as ping_created_at_month,
        date_trunc('WEEK', ping_created_at) as ping_created_at_week,
        date_trunc('DAY', ping_created_at) as ping_created_at_date,
        raw_usage_data_id as raw_usage_data_id,

        -- metadata
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
    from joined
    left join
        dim_product_tier on trim(lower(joined.product_tier)) = trim(
            lower(dim_product_tier.product_tier_historical_short)
        ) and edition = 'EE'

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@mpeychet",
        updated_by="@mpeychet",
        created_date="2021-05-10",
        updated_date="2021-05-26",
    )
}}
