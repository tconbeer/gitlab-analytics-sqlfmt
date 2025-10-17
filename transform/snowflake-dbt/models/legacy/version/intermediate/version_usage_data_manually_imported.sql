{{ config({"materialized": "table"}) }}

with
    source as (select * from {{ ref("snowflake_imports_usage_ping_payloads_source") }}),
    usage_data as (

        select
            {{
                dbt_utils.star(
                    from=ref("snowflake_imports_usage_ping_payloads_source"),
                    except=["EDITION"],
                )
            }},
            iff(
                license_expires_at >= recorded_at or license_expires_at is null,
                edition,
                'EE Free'
            ) as edition,
            regexp_replace(nullif(version, ''), '[^0-9.]+') as cleaned_version,
            iff(version ilike '%-pre', true, false) as version_is_prerelease,
            split_part(cleaned_version, '.', 1)::number as major_version,
            split_part(cleaned_version, '.', 2)::number as minor_version,
            major_version || '.' || minor_version as major_minor_version
        from source

    )

select *
from usage_data
