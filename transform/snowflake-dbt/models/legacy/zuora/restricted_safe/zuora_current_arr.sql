with
    subscription as (select * from {{ ref("zuora_subscription") }}),
    account as (select * from {{ ref("zuora_account") }}),
    rate_plan as (select * from {{ ref("zuora_rate_plan") }}),
    rate_plan_charge as (select * from {{ ref("zuora_rate_plan_charge") }}),
    arr as (
        select
            subscription.subscription_id,
            sum(rate_plan_charge.mrr * 12::number) as current_arr
        from subscription
        join account on subscription.account_id = account.account_id::varchar
        join
            rate_plan
            on rate_plan.subscription_id::varchar = subscription.subscription_id
        join
            rate_plan_charge
            on rate_plan_charge.rate_plan_id::varchar = rate_plan.rate_plan_id::varchar
        where
            (
                subscription.subscription_status not in ('Draft', 'Expired')
            )  -- DOUBLE CHECK THIS
            and rate_plan_charge.effective_start_date <= current_date
            and (
                rate_plan_charge.effective_end_date > current_date
                or rate_plan_charge.effective_end_date is null
            )
        group by subscription.subscription_id
    )

select
    sum(case when current_arr > 0 then 1 else 0 end) as over_0,
    sum(case when current_arr > 5000 then 1 else 0 end) as over_5k,
    sum(case when current_arr > 50000 then 1 else 0 end) as over_50k,
    sum(case when current_arr > 100000 then 1 else 0 end) as over_100k,
    sum(current_arr) as current_arr
from arr
