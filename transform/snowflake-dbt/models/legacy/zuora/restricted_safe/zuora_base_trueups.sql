with
    invoice_details as (

        select *
        from {{ ref("zuora_base_invoice_details") }}
        where lower(charge_name) like '%trueup%'

    )

    ,
    final as (

        select
            -- PRIMARY KEY
            subscription_name_slugify,

            -- LOGICAL INFO
            account_number,
            charge_name,
            subscription_name,

            -- LINEAGE
            lineage,
            oldest_subscription_in_cohort,

            -- METADATA
            cohort_month,
            cohort_quarter,
            country,
            service_month as trueup_month,
            service_start_date,

            -- REVENUE DATA
            charge_amount,
            charge_amount / 12 as mrr,
            unit_of_measure,
            unit_price

        from invoice_details

    )

select *
from final
