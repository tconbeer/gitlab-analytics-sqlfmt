{% snapshot customers_db_customers_snapshots %}

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
            ) as customers_rank_in_key

        from {{ source("customers", "customers_db_customers") }}

    )

select *
from source
where customers_rank_in_key = 1

{% endsnapshot %}
