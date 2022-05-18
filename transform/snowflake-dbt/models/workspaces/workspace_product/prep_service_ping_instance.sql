{{
    config(
        tags=["product", "mnpi_exception"],
        materialized="incremental",
        unique_key="dim_service_ping_instance_id",
    )
}}


{{ simple_cte([("raw_usage_data", "version_raw_usage_data_source")]) }}

,
source as (

    select
        id as dim_service_ping_instance_id,
        created_at::timestamp(0) as ping_created_at,
        *,
        {{ nohash_sensitive_columns("version_usage_data_source", "source_ip") }}
        as ip_address_hash
    from {{ ref("version_usage_data_source") }} as usage

    {% if is_incremental() %}
    where ping_created_at >= (select max(ping_created_at) from {{ this }})
    {% endif %}

),
usage_data as (

    select
        dim_service_ping_instance_id as dim_service_ping_instance_id,
        host_id as dim_host_id,
        uuid as dim_instance_id,
        ping_created_at as ping_created_at,
        source_ip_hash as ip_address_hash,
        edition as original_edition,
        {{
            dbt_utils.star(
                from=ref("version_usage_data_source"),
                except=["EDITION", "CREATED_AT", "SOURCE_IP"],
            )
        }}
    from source
    where uuid is not null and version not like ('%VERSION%')

),
joined_ping as (

    select
        dim_service_ping_instance_id as dim_service_ping_instance_id,
        dim_host_id as dim_host_id,
        usage_data.dim_instance_id as dim_instance_id,
        {{ dbt_utils.surrogate_key(["dim_host_id", "dim_instance_id"]) }}
        as dim_installation_id,
        ping_created_at as ping_created_at,
        ip_address_hash as ip_address_hash,
        original_edition as original_edition,
        {{
            dbt_utils.star(
                from=ref("version_usage_data_source"),
                relation_alias="usage_data",
                except=["EDITION", "CREATED_AT", "SOURCE_IP"],
            )
        }},
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
        coalesce(
            raw_usage_data.raw_usage_data_payload,
            usage_data.raw_usage_data_payload_reconstructed
        ) as raw_usage_data_payload
    from usage_data
    left join
        raw_usage_data
        on usage_data.raw_usage_data_id = raw_usage_data.raw_usage_data_id

)

{{
    dbt_audit(
        cte_ref="joined_ping",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-03-17",
        updated_date="2022-03-17",
    )
}}
