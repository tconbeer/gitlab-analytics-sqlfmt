{% set levels = ["zuora_subscription_id", "sfdc_account_id", "parent_account_id"] %}

with
    base as (
        select
            oldest_subscription_in_cohort as zuora_subscription_id,
            dim_parent_crm_account_id as parent_account_id,
            dim_crm_account_id as sfdc_account_id,
            {{
                dbt_utils.star(
                    from=ref("mart_arr"),
                    except=[
                        "oldest_subscription_in_cohort",
                        "dim_parent_crm_account_id",
                        "dim_crm_account_id",
                    ],
                )
            }}
        from {{ ref("mart_arr") }}

    {% for level in levels -%}
        ),
        {{ level }}_max_month as (

            select max(arr_month) as most_recent_mrr_month, {{ level }} as id
            from base
            where arr_month < dateadd(month, -1, current_date)
            group by 2

        ),
        {{ level }}_get_mrr as (

            select {{ level }}_max_month.*, sum(base.mrr) as mrr
            from {{ level }}_max_month
            left join
                base
                on {{ level }}_max_month.id = base.{{ level }}
                and {{ level }}_max_month.most_recent_mrr_month = base.arr_month
            group by 1, 2

        ),
        {{ level }}_get_segmentation as (

            select
                id,
                '{{level}}'::varchar as level_,
                mrr * 12 as arr,
                rank() over (partition by level_ order by arr desc) as arr_rank,
                case
                    when (mrr * 12) < 5000
                    then 'Under 5K'
                    when (mrr * 12) < 50000
                    then '5K to 50K'
                    when (mrr * 12) < 100000
                    then '50K to 100K'
                    when (mrr * 12) < 500000
                    then '100K to 500K'
                    when (mrr * 12) < 1000000
                    then '500K to 1M'
                    else '1M and above'
                end as arr_segmentation,
                case
                    when arr_rank < 26
                    then 'First 25 Customer'
                    when arr_rank < 51
                    then '26 - 50 Customer'
                    when arr_rank < 101
                    then '51 - 100 Customer'
                    when arr_rank < 501
                    then '101 - 500 Customer'
                    when arr_rank < 1001
                    then '501 - 1000 Customer'
                    when arr_rank < 5001
                    then '1001 - 5000 Customer'
                    else '5000+ Customer'
                end as rank_segmentation
            from {{ level }}_get_mrr
            group by 1, 2, 3

    {% endfor -%}),
    unioned as (

        {% for level in levels -%}

            select *
            from {{ level }}_get_segmentation
            {%- if not loop.last %}
                union all
            {%- endif %}

        {% endfor -%}
    )
select *
from unioned
group by 1, 2, 3, 4, 5, 6
