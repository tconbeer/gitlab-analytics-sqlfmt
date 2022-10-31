{{ config({"schema": "legacy"}) }}

with
    zuora_subscription as (

        select
            *,
            row_number() over (
                partition by subscription_name, version order by updated_date desc
            ) as sub_row
        from {{ ref("zuora_subscription") }}

    /*
      The partition deduplicates the subscriptions when there are
      more than one version at the same time.
      See account_id = '2c92a0fc55a0dc530155c01a026806bd' for
      an example.
     */
    ),
    zuora_subs_filtered as (

        select *
        from zuora_subscription
        where subscription_status in ('Active', 'Cancelled')

    ),
    zuora_partitioned_filter as (

        select

            zuora_subs_filtered.*,
            -- Dates
            date_trunc('month', zuora_subs_filtered.subscription_start_date)::date
            as subscription_start_month,
            date_trunc(
                'month', dateadd('day', -1, zuora_subs_filtered.subscription_end_date)
            )::date as subscription_end_month,
            date_trunc('month', zuora_subs_filtered.contract_effective_date)::date
            as subscription_month,
            date_trunc('quarter', zuora_subs_filtered.contract_effective_date)::date
            as subscription_quarter,
            date_trunc('year', zuora_subs_filtered.contract_effective_date)::date
            as subscription_year

        from zuora_subs_filtered
        where zuora_subs_filtered.sub_row = 1

    ),
    circular as (

        -- Identify for, exclusion, subscriptions with circular references in renewals
        -- to prevent failure of zuora_subscription_lineage
        select distinct left_subs.subscription_id
        from zuora_partitioned_filter as left_subs
        inner join
            zuora_partitioned_filter as right_subs
            on left_subs.zuora_renewal_subscription_name = right_subs.subscription_name
        where left_subs.subscription_name = right_subs.zuora_renewal_subscription_name

    )

select *
from zuora_partitioned_filter
-- exclude circularly referenced subscriptions
where subscription_id not in (select subscription_id from circular)
