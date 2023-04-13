{% snapshot customers_db_orders_snapshots %}

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
                row_number() over (
                    partition by id order by updated_at desc
                ) as orders_rank_in_key
            from {{ source("customers", "customers_db_orders") }}
        )

    select *
    from source
    where orders_rank_in_key = 1

{% endsnapshot %}
