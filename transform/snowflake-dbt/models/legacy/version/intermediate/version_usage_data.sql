{{ config({"materialized": "table"}) }}

{%- set columns = adapter.get_columns_in_relation(ref("version_usage_data_source")) -%}

with
    source as (select * from {{ ref("version_usage_data_source") }}),
    usage_data as (

        select
            {{
                dbt_utils.star(
                    from=ref("version_usage_data_source"),
                    except=["EDITION", "RAW_USAGE_DATA_PAYLOAD_RECONSTRUCTED"],
                )
            }},
            iff(
                license_expires_at >= created_at or license_expires_at is null,
                edition,
                'EE Free'
            ) as cleaned_edition,
            regexp_replace(nullif(version, ''), '[^0-9.]+') as cleaned_version,
            iff(version ilike '%-pre', true, false) as version_is_prerelease,
            split_part(cleaned_version, '.', 1)::number as major_version,
            split_part(cleaned_version, '.', 2)::number as minor_version,
            major_version || '.' || minor_version as major_minor_version,
            raw_usage_data_payload_reconstructed
        from source
        where
            uuid is not null
            and version not like ('%VERSION%')  -- Messy data that's not worth parsing.
            -- Staging data has no current use cases for analysis.
            and hostname not in ('staging.gitlab.com', 'dr.gitlab.com')

    ),
    raw_usage_data as (select * from {{ ref("version_raw_usage_data_source") }}),
    joined as (

        select
            {{
                dbt_utils.star(
                    from=ref("version_usage_data_source"),
                    relation_alias="usage_data",
                    except=["EDITION"],
                )
            }},
            cleaned_edition as edition,
            cleaned_version,
            version_is_prerelease,
            major_version,
            minor_version,
            major_minor_version,
            coalesce(
                raw_usage_data.raw_usage_data_payload,
                raw_usage_data_payload_reconstructed
            ) as raw_usage_data_payload
        from usage_data
        left join
            raw_usage_data
            on usage_data.raw_usage_data_id = raw_usage_data.raw_usage_data_id
    )

select *
from joined
