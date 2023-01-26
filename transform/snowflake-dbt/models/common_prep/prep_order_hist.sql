{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "table"}) }}

{{
    simple_cte(
        [
            ("versions", "customers_db_versions_source"),
            ("current_orders", "customers_db_orders_source"),
            ("dim_date", "dim_date"),
            ("namespaces", "prep_namespace"),
            ("subscriptions", "dim_subscription"),
            ("billing_accounts", "dim_billing_account"),
        ]
    )
}},
customers_db_versions as (

    select *
    from versions
    -- selecting only orders
    where item_type = 'Order' and object is not null

),
flattened_object as (

    -- objects look like a yaml table, splitting the object into rows at each linebreak
    -- column keys will be turned into column names and populated by the associated
    -- column values
    -- column values are all strings, some wrapped in extra quotations, some
    -- containing multiple colons
    select
        *,
        split_part(value, ': ', 1) as column_key,
        nullif(trim(split_part(value, column_key || ': ', 2), ''''), '') as column_value
    from customers_db_versions, lateral split_to_table(object, '\n')

),
cleaned as (

    -- this CTE attempts to further clean up column values
    -- namespace id: messy data from source, uses regular expression to remove all
    -- non-numeric characters
    -- boolean column: set NULL equal to FALSE
    -- timestamp columns: can come with 3-4 additional rows in the original object
    -- when the associated column_value for each timestamp column_key is not a
    -- timestamp the 3rd or 4th
    -- row following contains the actual timestamp value
    -- additionally, the created_at column sometimes contained '&1 ' before the
    -- timestamp value
    select
        version_id,
        item_id as order_id,
        created_at as valid_to,
        iff(column_key = 'customer_id', column_value::number, null) as customer_id,
        iff(
            column_key = 'product_rate_plan_id', column_value, null
        ) as product_rate_plan_id,
        iff(column_key = 'subscription_id', column_value, null) as subscription_id,
        iff(column_key = 'subscription_name', column_value, null) as subscription_name,
        iff(column_key = 'start_date', column_value::date, null) as order_start_date,
        iff(column_key = 'end_date', column_value::date, null) as order_end_date,
        iff(column_key = 'quantity', column_value::number, null) as order_quantity,
        iff(
            column_key = 'created_at',
            coalesce(
                try_to_timestamp(ltrim(column_value, '&1 ')),
                try_to_timestamp(
                    lag(column_value, 3) over (
                        partition by version_id, item_id, seq order by index desc
                    )
                ),
                try_to_timestamp(
                    lag(column_value, 4) over (
                        partition by version_id, item_id, seq order by index desc
                    )
                )
            ),
            null
        ) as order_created_at,
        iff(
            column_key = 'updated_at',
            coalesce(
                try_to_timestamp(column_value),
                try_to_timestamp(
                    lag(column_value, 3) over (
                        partition by version_id, item_id, seq order by index desc
                    )
                ),
                try_to_timestamp(
                    lag(column_value, 4) over (
                        partition by version_id, item_id, seq order by index desc
                    )
                )
            ),
            null
        ) as order_updated_at,
        iff(
            column_key = 'gl_namespace_id',
            try_to_number(regexp_replace(column_value, '[^0-9]+', '')),
            null
        ) as gitlab_namespace_id,
        iff(
            column_key = 'gl_namespace_name', column_value, null
        ) as gitlab_namespace_name,
        iff(column_key = 'amendment_type', column_value, null) as amendment_type,
        iff(
            column_key = 'trial', ifnull(column_value, false)::boolean, null
        ) as order_is_trial,
        iff(
            column_key = 'last_extra_ci_minutes_sync_at',
            coalesce(
                try_to_timestamp(column_value),
                try_to_timestamp(
                    lag(column_value, 3) over (
                        partition by version_id, item_id, seq order by index desc
                    )
                ),
                try_to_timestamp(
                    lag(column_value, 4) over (
                        partition by version_id, item_id, seq order by index desc
                    )
                )
            ),
            null
        ) as last_extra_ci_minutes_sync_at,
        iff(column_key = 'zuora_account_id', column_value, null) as zuora_account_id,
        iff(
            column_key = 'increased_billing_rate_notified_at',
            coalesce(
                try_to_timestamp(column_value),
                try_to_timestamp(
                    lag(column_value, 3) over (
                        partition by version_id, item_id, seq order by index desc
                    )
                ),
                try_to_timestamp(
                    lag(column_value, 4) over (
                        partition by version_id, item_id, seq order by index desc
                    )
                )
            ),
            null
        ) as increased_billing_rate_notified_at
    from flattened_object

),
pivoted as (

    select
        version_id,
        order_id,
        valid_to,
        max(customer_id) as customer_id,
        max(product_rate_plan_id) as product_rate_plan_id,
        max(subscription_id) as subscription_id,
        max(subscription_name) as subscription_name,
        max(order_start_date) as order_start_date,
        max(order_end_date) as order_end_date,
        max(order_quantity) as order_quantity,
        max(order_created_at) as order_created_at,
        max(order_updated_at) as order_updated_at,
        max(gitlab_namespace_id) as gitlab_namespace_id,
        max(gitlab_namespace_name) as gitlab_namespace_name,
        max(amendment_type) as amendment_type,
        max(order_is_trial) as order_is_trial,
        max(last_extra_ci_minutes_sync_at) as last_extra_ci_minutes_sync_at,
        max(zuora_account_id) as zuora_account_id,
        max(increased_billing_rate_notified_at) as increased_billing_rate_notified_at
    from cleaned {{ dbt_utils.group_by(n=3) }}

),
unioned as (

    select
        order_id as dim_order_id,
        customer_id,
        product_rate_plan_id,
        subscription_id as dim_subscription_id,
        subscription_name,
        order_start_date,
        order_end_date,
        order_quantity,
        order_created_at,
        gitlab_namespace_id::number as dim_namespace_id,
        gitlab_namespace_name as namespace_name,
        amendment_type,
        order_is_trial,
        last_extra_ci_minutes_sync_at,
        zuora_account_id as dim_billing_account_id,
        increased_billing_rate_notified_at,
        ifnull(
            lag(valid_to) over (partition by order_id order by version_id),
            order_created_at
        ) as valid_from,
        valid_to
    from pivoted
    where order_created_at is not null

    union all

    select
        order_id,
        customer_id,
        product_rate_plan_id,
        subscription_id,
        subscription_name,
        order_start_date,
        order_end_date,
        order_quantity,
        order_created_at,
        gitlab_namespace_id::number,
        gitlab_namespace_name,
        amendment_type,
        order_is_trial,
        last_extra_ci_minutes_sync_at,
        zuora_account_id,
        increased_billing_rate_notified_at,
        order_updated_at as valid_from,
        null as valid_to
    from current_orders

),
joined as (

    select
        unioned.dim_order_id,
        unioned.customer_id,
        unioned.product_rate_plan_id,
        unioned.order_created_at,
        start_dates.date_day as order_start_date,
        end_dates.date_day as order_end_date,
        unioned.order_quantity,
        subscriptions.dim_subscription_id,
        subscriptions.subscription_name,
        namespaces.dim_namespace_id,
        namespaces.namespace_name,
        billing_accounts.dim_billing_account_id,
        unioned.amendment_type,
        unioned.order_is_trial,
        unioned.last_extra_ci_minutes_sync_at,
        unioned.increased_billing_rate_notified_at,
        unioned.valid_from,
        unioned.valid_to
    from unioned
    left join
        subscriptions on unioned.dim_subscription_id = subscriptions.dim_subscription_id
    left join namespaces on unioned.dim_namespace_id = namespaces.dim_namespace_id
    left join
        billing_accounts
        on unioned.dim_billing_account_id = billing_accounts.dim_billing_account_id
    left join dim_date as start_dates on unioned.order_start_date = start_dates.date_day
    left join dim_date as end_dates on unioned.order_end_date = end_dates.date_day

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@ischweickartDD",
        updated_by="@ischweickartDD",
        created_date="2021-07-07",
        updated_date="2021-07-07",
    )
}}
