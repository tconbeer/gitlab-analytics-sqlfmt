with
    zuora_date as (

        {% set tables = [
    "account",
    "accounting_period",
    "amendment",
    "contact",
    "discount_applied_metrics",
    "invoice_item",
    "invoice",
    "invoice_payment",
    "product",
    "product_rate_plan",
    "product_rate_plan_charge",
    "product_rate_plan_charge_tier",
    "rate_plan",
    "rate_plan_charge",
    "rate_plan_charge_tier",
    "refund",
    "revenue_schedule_item",
    "subscription",
] %}

        {% for table in tables %}
        select '{{table}}' as table_name, max(createddate) as max_date
        from {{ source("zuora", table) }}


        {% if not loop.last %} union all {% endif %}

        {% endfor %}

    )


select *
from zuora_date
