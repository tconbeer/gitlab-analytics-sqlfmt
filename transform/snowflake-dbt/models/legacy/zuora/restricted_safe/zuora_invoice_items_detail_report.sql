with
    date_table as (select * from {{ ref("date_details") }} where day_of_month = 1),
    sfdc_accounts as (select * from {{ ref("sfdc_accounts_xf") }}),
    sfdc_deleted_accounts as (select * from {{ ref("sfdc_deleted_accounts") }}),
    zuora_accounts as (

        select * from {{ ref("zuora_account_source") }} where is_deleted = false

    ),
    zuora_invoices as (select * from {{ ref("zuora_invoice_charges") }}),
    zuora_product as (

        select * from {{ ref("zuora_product_source") }} where is_deleted = false

    ),
    zuora_product_rp as (

        select *
        from {{ ref("zuora_product_rate_plan_source") }}
        where is_deleted = false

    ),
    zuora_product_rpc as (

        select * from {{ ref("zuora_product_rate_plan_charge_source") }}

    ),
    zuora_product_rpct as (

        select * from {{ ref("zuora_product_rate_plan_charge_tier_source") }}

    ),
    initial_join_to_sfdc as (

        select
            invoice_number,
            invoice_item_id,
            zuora_accounts.crm_id as invoice_crm_id,
            sfdc_accounts.account_id as sfdc_account_id_int,
            zuora_accounts.account_name,
            invoice_date,
            date_trunc('month', invoice_date) as invoice_month,
            product_name,
            product_rate_plan_charge_id,
            {{ product_category("rate_plan_name") }},
            rate_plan_name,
            charge_type,
            invoice_item_unit_price,
            quantity as quantity,
            invoice_item_charge_amount as invoice_item_charge_amount
        from zuora_invoices
        left join
            zuora_accounts
            on zuora_invoices.invoice_account_id = zuora_accounts.account_id
        left join sfdc_accounts on zuora_accounts.crm_id = sfdc_accounts.account_id
        where invoice_item_charge_amount != 0

    ),
    replace_sfdc_account_id_with_master_record_id as (

        select
            coalesce(
                initial_join_to_sfdc.sfdc_account_id_int, sfdc_master_record_id
            ) as sfdc_account_id,
            initial_join_to_sfdc.*
        from initial_join_to_sfdc
        left join
            sfdc_deleted_accounts
            on initial_join_to_sfdc.invoice_crm_id
            = sfdc_deleted_accounts.sfdc_account_id

    ),
    joined as (

        select
            invoice_number,
            invoice_item_id,
            sfdc_account_id,
            case
                when ultimate_parent_account_segment = 'Unknown'
                then 'SMB'
                when ultimate_parent_account_segment = ''
                then 'SMB'
                else ultimate_parent_account_segment
            end as ultimate_parent_segment,
            replace_account_id.account_name,
            invoice_date,
            invoice_month,
            product_name,
            product_rate_plan_charge_id,
            product_category,
            account_type,
            rate_plan_name,
            charge_type,
            invoice_item_unit_price,
            quantity as quantity,
            invoice_item_charge_amount as invoice_item_charge_amount
        from replace_sfdc_account_id_with_master_record_id replace_account_id
        left join
            sfdc_accounts
            on replace_account_id.sfdc_account_id = sfdc_accounts.account_id

    ),
    list_price as (

        select
            zuora_product_rp.product_rate_plan_name,
            zuora_product_rpc.product_rate_plan_charge_name,
            zuora_product_rpc.product_rate_plan_charge_id,
            min(zuora_product_rpct.price) as billing_list_price
        from zuora_product
        inner join
            zuora_product_rp on zuora_product.product_id = zuora_product_rp.product_id
        inner join
            zuora_product_rpc
            on zuora_product_rp.product_rate_plan_id
            = zuora_product_rpc.product_rate_plan_id
        inner join
            zuora_product_rpct
            on zuora_product_rpc.product_rate_plan_charge_id
            = zuora_product_rpct.product_rate_plan_charge_id
        where
            zuora_product.effective_start_date <= current_date
            and zuora_product_rpct.currency = 'USD'
        group by 1, 2, 3
        order by 1, 2

    )

select
    joined.invoice_number,
    joined.invoice_item_id,
    sfdc_account_id,
    account_name,
    account_type,
    invoice_date,
    joined.product_name,
    joined.rate_plan_name,
    quantity,
    invoice_item_unit_price,
    invoice_item_charge_amount,
    case
        when lower(rate_plan_name) like '%month%'
        then (invoice_item_unit_price * 12)
        when lower(rate_plan_name) like '%2 years%'
        then (invoice_item_unit_price / 2)
        when lower(rate_plan_name) like '%2 year%'
        then (invoice_item_unit_price / 2)
        when lower(rate_plan_name) like '%3 years%'
        then (invoice_item_unit_price / 3)
        when lower(rate_plan_name) like '%3 year%'
        then (invoice_item_unit_price / 3)
        when lower(rate_plan_name) like '%4 years%'
        then (invoice_item_unit_price / 4)
        when lower(rate_plan_name) like '%4 year%'
        then (invoice_item_unit_price / 4)
        when lower(rate_plan_name) like '%5 years%'
        then (invoice_item_unit_price / 5)
        when lower(rate_plan_name) like '%5 year%'
        then (invoice_item_unit_price / 5)
        else invoice_item_unit_price
    end as annual_price,
    quantity * annual_price as quantity_times_annual,
    ultimate_parent_segment,
    product_category,
    invoice_month,
    fiscal_quarter_name_fy as fiscal_period,
    case
        when lower(rate_plan_name) like '%month%'
        then (billing_list_price * 12)
        when lower(rate_plan_name) like '%2 years%'
        then (billing_list_price / 2)
        when lower(rate_plan_name) like '%2 year%'
        then (billing_list_price / 2)
        when lower(rate_plan_name) like '%3 years%'
        then (billing_list_price / 3)
        when lower(rate_plan_name) like '%3 year%'
        then (billing_list_price / 3)
        when lower(rate_plan_name) like '%4 years%'
        then (billing_list_price / 4)
        when lower(rate_plan_name) like '%4 year%'
        then (billing_list_price / 4)
        when lower(rate_plan_name) like '%5 years%'
        then (billing_list_price / 5)
        when lower(rate_plan_name) like '%5 year%'
        then (billing_list_price / 5)
        when lower(charge_type) != 'recurring'
        then 0
        else billing_list_price
    end as list_price,
    case
        when annual_price = list_price
        then 0
        when lower(charge_type) != 'recurring'
        then 0
        else ( (annual_price - list_price) / nullif(list_price, 0)) * -1
    end as discount,
    case
        when lower(charge_type) != 'recurring' then 0 else quantity * list_price
    end as list_price_times_quantity
from joined
left join
    list_price
    on joined.product_rate_plan_charge_id = list_price.product_rate_plan_charge_id
left join date_table on joined.invoice_month = date_table.date_actual
order by invoice_date, invoice_number
