{{ config({"schema": "legacy"}) }}

with
    versions as (select * from {{ ref("version_versions_source") }}),
    calculated as (

        select
            *,
            split_part(version, '.', 1)::number as major_version,
            split_part(version, '.', 2)::number as minor_version,
            split_part(version, '.', 3)::number as patch_number,
            iff(patch_number = 0, true, false) as is_monthly_release,
            created_at::date as created_date,
            updated_at::date as updated_date
        from versions

    ),
    renamed as (

        select
            id as version_id,
            version,
            major_version,
            minor_version,
            patch_number,
            is_monthly_release,
            is_vulnerable,
            created_date,
            updated_date
        from calculated

    )

    {{
        dbt_audit(
            cte_ref="renamed",
            created_by="@derekatwood",
            updated_by="@msendal",
            created_date="2020-08-06",
            updated_date="2020-09-17",
        )
    }}
