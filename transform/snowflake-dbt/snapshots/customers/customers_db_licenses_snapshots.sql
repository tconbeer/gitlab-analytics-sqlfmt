{% snapshot customers_db_licenses_snapshots %}

{{
    config(
        unique_key="id",
        strategy="timestamp",
        updated_at="updated_at",
    )
}}

with
    source as (

        select
            *,
            row_number() OVER (
                partition by id order by updated_at desc
            ) as license_rank_in_key
        from {{ source("customers", "customers_db_licenses") }}

    )

select
    {{
        dbt_utils.star(
            from=source("customers", "customers_db_licenses"),
            except=["CREATED_AT", "UPDATED_AT"],
        )
    }},
    to_timestamp_ntz(created_at) as created_at,
    to_timestamp_ntz(updated_at) as updated_at
from source
where license_rank_in_key = 1

{% endsnapshot %}
