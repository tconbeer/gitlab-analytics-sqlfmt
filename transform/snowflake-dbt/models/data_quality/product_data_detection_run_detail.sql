{{ config(tags=["mnpi_exception"]) }}

{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "primary_key",
            "full_refresh": false,
        }
    )
}}

{{
    simple_cte(
        [
            ("product_data_detection_rule_3", "product_data_detection_rule_3"),
            ("dim_host_instance_type", "dim_host_instance_type"),
            ("dim_license", "dim_license"),
            ("dim_subscription", "dim_subscription"),
            ("map_license_subscription_account", "map_license_subscription_account"),
            ("fct_mrr", "fct_mrr"),
            ("dim_product_detail", "dim_product_detail"),
            ("dim_subscription", "dim_subscription"),
        ]
    )
}},
rule_run_date as (

    select distinct date_day as rule_run_date, 'Product' as type_of_data
    from {{ ref("dim_date") }}
    -- date when the code would be pushed to Production,we would be joining this with
    -- the dbt updated data for the models.
    where rule_run_date between '2021-06-23' and to_date(dbt_updated_at)

),
bdg_namespace_order_subscription as (

    select *
    from {{ ref("bdg_namespace_order_subscription") }}
    where is_subscription_active = 'Y'

),
self_managed_subs_with_licenses as (

    select distinct
        fct_mrr.dim_subscription_id,
        dim_subscription.subscription_name,
        iff(
            dim_license.license_start_date > current_date, true, false
        ) as is_license_start_date_future,
        iff(
            dim_license.license_start_date > dim_license.license_expire_date,
            true,
            false
        ) as is_license_start_date_greater_expire_date,
        fct_mrr.dbt_updated_at
    from fct_mrr
    left join
        dim_subscription
        on dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
    left join
        dim_product_detail
        on fct_mrr.dim_product_detail_id = dim_product_detail.dim_product_detail_id
    left join
        dim_license
        on dim_subscription.dim_subscription_id = dim_license.dim_subscription_id
    where
        dim_product_detail.product_delivery_type = 'Self-Managed'
        and dim_subscription.subscription_start_date <= current_date

),
expired_licenses_with_subs as (

    select distinct
        dim_subscription.dim_subscription_id,
        dim_license.dim_license_id,
        dim_license.license_md5,
        dim_license.license_start_date,
        dim_license.license_expire_date,
        dim_subscription.subscription_start_date,
        dim_subscription.subscription_end_date,
        iff(
            dim_license.license_expire_date <= current_date
            and dim_subscription.subscription_end_date <= current_date,
            true,
            false
        ) as is_license_expired_with_sub_end_date_past,
        dim_license.dbt_updated_at
    from dim_license
    left join
        dim_subscription
        on dim_license.dim_subscription_id = dim_subscription.dim_subscription_id
    where license_expire_date <= current_date

),
processed_passed_failed_record_count as (

    -- Missing instance types for UUID or Namespaces
    select
        1 as rule_id,
        (
            count(distinct(instance_uuid)) + count(distinct(namespace_id))
        ) as processed_record_count,
        (
            select count(distinct(ifnull(instance_uuid, namespace_id)))
            from dim_host_instance_type
            where instance_type not in ('Unknown')
        ) as passed_record_count,
        (processed_record_count - passed_record_count) as failed_record_count,
        dbt_updated_at as run_date
    from dim_host_instance_type
    group by run_date

    union

    -- Licenses with missing Subscriptions
    select
        2 as rule_id,
        count(distinct(dim_license_id)) as processed_record_count,
        (
            select count(distinct(dim_license_id))
            from dim_license
            where dim_subscription_id is not null
        ) as passed_record_count,
        (
            select count(distinct(dim_license_id))
            from dim_license
            where dim_subscription_id is null
        ) as failed_record_count,
        dbt_updated_at as run_date
    from dim_license
    group by run_date

    union

    -- Subscriptions with missing Licenses
    select
        3 as rule_id,
        count(distinct(subscription_name)) as processed_record_count,
        (
            select count(distinct(subscription_name))
            from product_data_detection_rule_3
            where dim_license_id is not null
        ) as passed_record_count,
        (
            select count(distinct(subscription_name))
            from product_data_detection_rule_3
            where dim_license_id is null
        ) as failed_record_count,
        dbt_updated_at as run_date
    from product_data_detection_rule_3
    group by run_date

    union

    -- Subscriptions with Self-Managed Plans having License Start dates in the future
    select
        4 as rule_id,
        count(distinct(dim_subscription_id)) as processed_record_count,
        count(distinct(dim_subscription_id))
        - count(
            distinct iff(is_license_start_date_future, dim_subscription_id, null)
        ) as passed_record_count,
        count(
            distinct iff(is_license_start_date_future, dim_subscription_id, null)
        ) as failed_record_count,
        dbt_updated_at as run_date
    from self_managed_subs_with_licenses
    group by run_date

    union

    -- Subscriptions with Self-Managed Plans having License Start Date greater than
    -- License Expire date
    select
        5 as rule_id,
        count(distinct(dim_subscription_id)) as processed_record_count,
        count(distinct(dim_subscription_id)) - count(
            distinct iff(
                is_license_start_date_greater_expire_date, dim_subscription_id, null
            )
        ) as passed_record_count,
        count(
            distinct iff(
                is_license_start_date_greater_expire_date, dim_subscription_id, null
            )
        ) as failed_record_count,
        dbt_updated_at as run_date
    from self_managed_subs_with_licenses
    group by run_date

    union

    -- Expired License IDs with Subscription End Dates in the Past
    select
        6 as rule_id,
        count(distinct(dim_license_id)) as processed_record_count,
        sum(
            iff(is_license_expired_with_sub_end_date_past, 0, 1)
        ) as passed_record_count,
        sum(
            iff(is_license_expired_with_sub_end_date_past, 1, 0)
        ) as failed_record_count,
        dbt_updated_at as run_date
    from expired_licenses_with_subs
    group by run_date

    union

    -- SaaS Subscriptions Not Mapped to Namespaces
    select
        7 as rule_id,
        count(distinct(dim_subscription_id)) as processed_record_count,
        count(
            distinct iff(
                dim_subscription_id is not null and dim_namespace_id is not null,
                dim_subscription_id,
                null
            )
        ) as passed_record_count,
        (processed_record_count - passed_record_count) as failed_record_count,
        dbt_updated_at as run_date
    from bdg_namespace_order_subscription
    group by run_date

),
final as (

    select
        -- primary_key
        {{
            dbt_utils.surrogate_key(
                [
                    "rule_run_date.rule_run_date",
                    "processed_passed_failed_record_count.rule_id",
                ]
            )
        }} as primary_key,

        -- Detection Rule record counts
        rule_id,
        processed_record_count,
        passed_record_count,
        failed_record_count,
        rule_run_date.rule_run_date,
        type_of_data
    from processed_passed_failed_record_count
    right outer join
        rule_run_date
        on to_date(processed_passed_failed_record_count.run_date)
        = rule_run_date.rule_run_date

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@snalamaru",
        updated_by="@jpguero",
        created_date="2021-06-16",
        updated_date="2021-11-15",
    )
}}
