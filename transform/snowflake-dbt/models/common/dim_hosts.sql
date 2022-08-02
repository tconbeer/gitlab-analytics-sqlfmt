{{ config({"schema": "legacy"}) }}

with
    usage_ping as (

        select {{ hash_sensitive_columns("version_usage_data_source") }}
        from {{ ref("version_usage_data_source") }}

    ),
    hosts as (

        select distinct
            host_id as host_id,
            first_value(hostname) over (
                partition by host_id order by hostname is not null desc, created_at desc
            ) as host_name,
            uuid as instance_id,
            source_ip_hash
        from usage_ping

    ),
    ip_to_country as (select * from {{ ref("map_ip_to_country") }}),
    usage_with_ip as (

        select hosts.*, ip_to_country.dim_location_country_id as location_id
        from hosts
        left join ip_to_country on hosts.source_ip_hash = ip_to_country.ip_address_hash

    ),
    renamed as (select * from usage_with_ip)


    {{
        dbt_audit(
            cte_ref="renamed",
            created_by="@mpeychet",
            updated_by="@mcooperDD",
            created_date="2020-11-24",
            updated_date="2021-03-05",
        )
    }}
