{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_usage_ping_id"}) }}

with
    source as (

        select
            id as dim_usage_ping_id,
            created_at::timestamp(0) as ping_created_at,
            *,
            {{ nohash_sensitive_columns("version_usage_data_source", "source_ip") }}
            as ip_address_hash
        from {{ ref("version_usage_data_source") }}

    ),
    raw_usage_data as (select * from {{ ref("version_raw_usage_data_source") }}),
    map_ip_to_country as (select * from {{ ref("map_ip_to_country") }}),
    locations as (select * from {{ ref("prep_location_country") }}),
    usage_data as (

        select
            dim_usage_ping_id,
            host_id as dim_host_id,
            uuid as dim_instance_id,
            ping_created_at,
            source_ip_hash as ip_address_hash,
            {{ dbt_utils.star(from=ref('version_usage_data_source'), except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
            edition as original_edition,
            iff(
                license_expires_at >= ping_created_at or license_expires_at is null,
                edition,
                'EE Free'
            ) as cleaned_edition,
            regexp_replace(nullif(version, ''), '[^0-9.]+') as cleaned_version,
            iff(version ilike '%-pre', true, false) as version_is_prerelease,
            split_part(cleaned_version, '.', 1)::number as major_version,
            split_part(cleaned_version, '.', 2)::number as minor_version,
            major_version || '.' || minor_version as major_minor_version
        from source
        -- Messy data that's not worth parsing
        where uuid is not null and version not like ('%VERSION%')

    ),
    joined as (

        select
            dim_usage_ping_id,
            dim_host_id,
            usage_data.dim_instance_id,
            ping_created_at,
            ip_address_hash,
            {{ dbt_utils.star(from=ref('version_usage_data_source'), relation_alias='usage_data', except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
            original_edition,
            cleaned_edition as edition,
            iff(original_edition = 'CE', 'CE', 'EE') as main_edition,
            case
                when original_edition = 'CE'
                then 'Core'
                when original_edition = 'EE Free'
                then 'Core'
                when license_expires_at < ping_created_at
                then 'Core'
                when original_edition = 'EE'
                then 'Starter'
                when original_edition = 'EES'
                then 'Starter'
                when original_edition = 'EEP'
                then 'Premium'
                when original_edition = 'EEU'
                then 'Ultimate'
                else null
            end as product_tier,
            main_edition || ' - ' || product_tier as main_edition_product_tier,
            cleaned_version,
            version_is_prerelease,
            major_version,
            minor_version,
            major_minor_version,
            case
                when uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
                then 'SaaS'
                else 'Self-Managed'
            end as ping_source,
            case
                when ping_source = 'SaaS'
                then true
                when installation_type = 'gitlab-development-kit'
                then true
                when hostname = 'gitlab.com'
                then true
                when hostname ilike '%.gitlab.com'
                then true
                else false
            end as is_internal,
            case
                when hostname ilike 'staging.%'
                then true
                when hostname in ('staging.gitlab.com', 'dr.gitlab.com')
                then true
                else false
            end as is_staging,
            hostname as host_name,
            coalesce(
                raw_usage_data.raw_usage_data_payload,
                usage_data.raw_usage_data_payload_reconstructed
            ) as raw_usage_data_payload
        from usage_data
        left join
            raw_usage_data
            on usage_data.raw_usage_data_id = raw_usage_data.raw_usage_data_id

    ),
    map_ip_location as (

        select
            map_ip_to_country.ip_address_hash, map_ip_to_country.dim_location_country_id
        from map_ip_to_country
        inner join locations
        where
            map_ip_to_country.dim_location_country_id
            = locations.dim_location_country_id

    ),
    add_country_info_to_usage_ping as (

        select joined.*, map_ip_location.dim_location_country_id
        from joined
        left join
            map_ip_location on joined.ip_address_hash = map_ip_location.ip_address_hash

    ),
    dim_product_tier as (

        select *
        from {{ ref("dim_product_tier") }}
        where product_delivery_type = 'Self-Managed'

    ),
    final as (

        select
            dim_usage_ping_id,
            dim_host_id,
            dim_instance_id,
            dim_product_tier.dim_product_tier_id as dim_product_tier_id,
            host_name,
            ping_created_at,
            dateadd('days', -28, ping_created_at) as ping_created_at_28_days_earlier,
            date_trunc('YEAR', ping_created_at) as ping_created_at_year,
            date_trunc('MONTH', ping_created_at) as ping_created_at_month,
            date_trunc('WEEK', ping_created_at) as ping_created_at_week,
            date_trunc('DAY', ping_created_at) as ping_created_at_date,
            raw_usage_data_id as raw_usage_data_id,
            raw_usage_data_payload,
            license_md5,
            original_edition,
            edition,
            main_edition,
            product_tier,
            main_edition_product_tier,
            cleaned_version,
            version_is_prerelease,
            major_version,
            minor_version,
            major_minor_version,
            ping_source,
            is_internal,
            is_staging,
            instance_user_count,
            license_user_count,
            dim_location_country_id
        from add_country_info_to_usage_ping
        left outer join
            dim_product_tier on trim(
                lower(add_country_info_to_usage_ping.product_tier)
            ) = trim(
                lower(dim_product_tier.product_tier_historical_short)
            ) and main_edition = 'EE'

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@kathleentam",
            updated_by="@chrissharp",
            created_date="2021-01-10",
            updated_date="2021-09-30",
        )
    }}
