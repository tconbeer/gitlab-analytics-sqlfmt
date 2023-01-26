with
    orders_snapshots as (select * from {{ ref("customers_db_orders_snapshots_base") }}),
    trials_snapshots as (select * from orders_snapshots where order_is_trial = true),
    latest_trials_from_trials_snapshot as (

        select *
        from trials_snapshots
        qualify
            row_number() over (
                partition by try_to_number(gitlab_namespace_id)
                order by valid_from desc, order_id desc
            )
            = 1

    )

select *
from latest_trials_from_trials_snapshot
