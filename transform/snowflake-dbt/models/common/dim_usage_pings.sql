{{ config(tags=["product"]) }}

{{ config({"schema": "legacy"}) }}

with
    usage_ping_data as (

        select {{ hash_sensitive_columns("version_usage_data_source") }}
        from {{ ref("version_usage_data_source") }}
        where uuid is not null

    ),
    version_edition_cleaned as (

        select
            usage_ping_data.*,
            {{ get_date_id("usage_ping_data.created_at") }} as created_date_id,
            regexp_replace(nullif(version, ''), '\-.*') as cleaned_version,
            split_part(cleaned_version, '.', 1) as major_version,
            split_part(cleaned_version, '.', 2) as minor_version,
            major_version || '.' || minor_version as major_minor_version,
            iff(
                version like '%-pre%' or version like '%-rc%', true, false
            )::boolean as is_pre_release,
            iff(edition = 'CE', 'CE', 'EE') as main_edition,
            case
                when edition = 'CE'
                then 'Core'
                when edition = 'EE Free'
                then 'Core'
                when license_expires_at < usage_ping_data.created_at
                then 'Core'
                when edition = 'EE'
                then 'Starter'
                when edition = 'EES'
                then 'Starter'
                when edition = 'EEP'
                then 'Premium'
                when edition = 'EEU'
                then 'Ultimate'
                else null
            end as product_tier,
            main_edition || ' - ' || product_tier as main_edition_product_tier,
            iff(
                uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f', 'SaaS', 'Self-Managed'
            ) as ping_source
        from usage_ping_data

    ),
    internal_identified as (

        select
            *,
            case
                when installation_type = 'gitlab-development-kit'
                then true
                when hostname = 'gitlab.com'
                then true
                when hostname ilike '%.gitlab.com'
                then true
                else false
            end as is_internal,
            iff(hostname ilike '%staging.%', true, false) as is_staging
        from version_edition_cleaned

    ),
    raw_usage_data as (select * from {{ ref("version_raw_usage_data_source") }}),
    renamed as (

        select
            internal_identified.*,
            to_date(
                raw_usage_data.raw_usage_data_payload:license_trial_ends_on::text
            ) as license_trial_ends_on,
            (
                raw_usage_data.raw_usage_data_payload:license_subscription_id::text
            ) as license_subscription_id,
            raw_usage_data.raw_usage_data_payload:usage_activity_by_stage_monthly.manage.events::number
            as umau_value,
            iff(
                internal_identified.created_at < license_trial_ends_on, true, false
            ) as is_trial,
            raw_usage_data.raw_usage_data_payload
        from internal_identified
        left join
            raw_usage_data
            on internal_identified.raw_usage_data_id = raw_usage_data.raw_usage_data_id

    )

select *
from renamed
