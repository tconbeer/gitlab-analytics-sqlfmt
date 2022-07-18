{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "ip_address_hash",
        }
    )
}}

with
    all_hashed_ips_version_usage as (

        select {{ nohash_sensitive_columns("version_usage_data_source", "source_ip") }}
        from {{ ref("version_usage_data_source") }}

    ),
    all_distinct_ips as (

        select distinct
            source_ip_hash,
            parse_ip(source_ip, 'inet')[
                'ip_fields'
            ][0]::number as source_ip_numeric
        from all_hashed_ips_version_usage
        {% if is_incremental() %}
        where source_ip_hash not in (select ip_address_hash from {{ this }})
        {% endif %}

    ),
    maxmind_ip_ranges as (

        select * from {{ ref("sheetload_maxmind_ip_ranges_source") }}

    ),
    newly_mapped_ips as (

        select source_ip_hash as ip_address_hash, geoname_id as dim_location_country_id
        from all_distinct_ips
        join maxmind_ip_ranges
        where
            all_distinct_ips.source_ip_numeric
            between maxmind_ip_ranges.ip_range_first_ip_numeric and maxmind_ip_ranges.ip_range_last_ip_numeric

    )

    {{
        dbt_audit(
            cte_ref="newly_mapped_ips",
            created_by="@m_walker",
            updated_by="@mcooperDD",
            created_date="2020-08-25",
            updated_date="2020-03-05",
        )
    }}
