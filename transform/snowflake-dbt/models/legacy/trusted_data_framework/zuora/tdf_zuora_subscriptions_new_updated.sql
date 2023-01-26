with
    zuora_subscriptions_new as (select * from {{ ref("zuora_subscriptions_new") }}),
    zuora_subscriptions_updated as (

        select * from {{ ref("zuora_subscriptions_updated") }}

    ),
    dim_date as (select * from {{ ref("dim_date") }}),
    final as (

        select
            dates.date_day as date_day,
            new_records.rowcount as new_records,
            updated_records.rowcount as updated_records
        from dim_date dates
        left join
            zuora_subscriptions_new new_records on new_records.date_day = dates.date_day
        left join
            zuora_subscriptions_updated updated_records
            on updated_records.date_day = dates.date_day
        where (new_records.rowcount > 0 or updated_records.rowcount > 0)

    )

select *
from final
