with
    zuora_account_source as (

        select *
        from {{ ref("zuora_account_source") }}
        where is_deleted = 'FALSE' and batch != 'Batch20'

    ),
    sfdc_opportunity_source as (

        select *
        from {{ ref("sfdc_opportunity_source") }}
        where is_deleted = 'FALSE' and stage_name != '10-Duplicate'

    ),
    zuora_subscription_source as (

        select prep_subscription.*
        from {{ ref("prep_subscription") }}
        inner join
            zuora_account_source
            on prep_subscription.dim_billing_account_id
            = zuora_account_source.account_id

    ),
    subscription_opps as (

        select distinct
            dim_subscription_id as subscription_id,
            dim_crm_opportunity_id as opportunity_id
        from zuora_subscription_source
        where
            opportunity_id is not null
            and (
                subscription_created_date >= '2021-04-12'
                or subscription_sales_type = 'Self-Service'
            )

    ),
    zuora_rate_plan_source as (

        select * from {{ ref("zuora_rate_plan_source") }} where is_deleted = 'FALSE'

    ),
    zuora_rate_plan_charge_source as (

        select zuora_rate_plan_charge_source.*, zuora_rate_plan_source.subscription_id
        from {{ ref("zuora_rate_plan_charge_source") }}
        left join
            zuora_rate_plan_source
            on zuora_rate_plan_charge_source.rate_plan_id
            = zuora_rate_plan_source.rate_plan_id
        where zuora_rate_plan_charge_source.is_deleted = 'FALSE'

    ),
    prep_crm_account as (

        select * from {{ ref("prep_crm_account") }} where is_deleted = 'FALSE'

    ),
    zuora_invoice_item_source as (

        select * from {{ ref("zuora_invoice_item_source") }} where is_deleted = 'FALSE'

    ),
    zuora_invoice_source as (

        select * from {{ ref("zuora_invoice_source") }} where is_deleted = 'FALSE'

    ),
    sfdc_zqu_quote_source as (

        select *
        from {{ ref("sfdc_zqu_quote_source") }}
        where is_deleted = 'FALSE' and sfdc_zqu_quote_source.zqu__primary = 'TRUE'

    ),
    quote_opps as (

        select distinct
            sfdc_zqu_quote_source.zqu__zuora_subscription_id as subscription_id,
            sfdc_zqu_quote_source.zqu__opportunity as opportunity_id,
            sfdc_opportunity_source.account_id as quote_opp_account_id,
            sfdc_opportunity_source.created_date as quote_opp_created_date,
            sfdc_opportunity_source.amount as quote_opp_total_contract_value
        from sfdc_zqu_quote_source
        inner join
            sfdc_opportunity_source
            on sfdc_zqu_quote_source.zqu__opportunity
            = sfdc_opportunity_source.opportunity_id
        where
            sfdc_zqu_quote_source.zqu__opportunity is not null
            and sfdc_zqu_quote_source.zqu__zuora_subscription_id is not null

    ),
    invoice_opps as (

        select distinct
            zuora_invoice_item_source.subscription_id,
            zuora_invoice_source.invoice_number,
            sum(zuora_invoice_item_source.charge_amount) as invoice_item_charge_amount,
            sum(zuora_invoice_item_source.quantity) as invoice_item_quantity,
            sfdc_opportunity_source.opportunity_id,
            sfdc_opportunity_source.account_id as invoice_opp_account_id,
            sfdc_opportunity_source.created_date as invoice_opp_created_date,
            sfdc_opportunity_source.amount as invoice_opp_total_contract_value
        from zuora_invoice_item_source
        left join
            zuora_invoice_source
            on zuora_invoice_item_source.invoice_id = zuora_invoice_source.invoice_id
        inner join
            sfdc_opportunity_source
            on zuora_invoice_source.invoice_number
            = sfdc_opportunity_source.invoice_number
        where
            zuora_invoice_source.status = 'Posted'
            and zuora_invoice_source.invoice_number is not null
            and sfdc_opportunity_source.opportunity_id is not null
        group by 1, 2, 5, 6, 7, 8

    ),
    subscription_quote_number_opps as (

        select
            zuora.subscription_id,
            zuora.sfdc_opportunity_id,
            zuora.crm_opportunity_name,
            sfdc_opportunity_source.opportunity_id,
            sfdc_opportunity_source.account_id
            as subscription_quote_number_opp_account_id,
            sfdc_opportunity_source.created_date
            as subscription_quote_number_opp_created_date,
            sfdc_opportunity_source.amount
            as subscription_quote_number_opp_total_contract_value
        from {{ ref("zuora_subscription_source") }} zuora
        left join
            sfdc_zqu_quote_source
            on zuora.quote_number = sfdc_zqu_quote_source.zqu__number
        inner join
            sfdc_opportunity_source
            on sfdc_zqu_quote_source.zqu__opportunity
            = sfdc_opportunity_source.opportunity_id

    ),
    final as (

        select distinct
            zuora_subscription_source.dim_subscription_id as dim_subscription_id,
            zuora_subscription_source.dim_billing_account_id as dim_billing_account_id,
            zuora_subscription_source.subscription_name as subscription_name,
            zuora_subscription_source.subscription_sales_type
            as subscription_sales_type,
            zuora_subscription_source.dim_crm_account_id as subscription_account_id,
            prep_crm_account.dim_parent_crm_account_id
            as subscription_parent_account_id,
            coalesce(
                invoice_opps.invoice_opp_account_id,
                lag(invoice_opps.invoice_opp_account_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as invoice_opp_account_id_forward,
            coalesce(
                invoice_opps.invoice_opp_account_id,
                lead(invoice_opps.invoice_opp_account_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as invoice_opp_account_id_backward,
            coalesce(
                quote_opps.quote_opp_account_id,
                lag(quote_opps.quote_opp_account_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as quote_opp_account_id_forward,
            coalesce(
                quote_opps.quote_opp_account_id,
                lead(quote_opps.quote_opp_account_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as quote_opp_account_id_backward,
            coalesce(
                subscription_quote_number_opps.subscription_quote_number_opp_account_id,
                lag(
                    subscription_quote_number_opps.subscription_quote_number_opp_account_id
                ) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as subscription_opp_name_opp_account_id_forward,
            coalesce(
                subscription_quote_number_opps.subscription_quote_number_opp_account_id,
                lead(
                    subscription_quote_number_opps.subscription_quote_number_opp_account_id
                ) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as subscription_opp_name_opp_account_id_backward,
            zuora_subscription_source.subscription_version as subscription_version,
            zuora_subscription_source.term_start_date as term_start_date,
            zuora_subscription_source.term_end_date as term_end_date,
            zuora_subscription_source.subscription_start_date
            as subscription_start_date,
            zuora_subscription_source.subscription_end_date as subscription_end_date,
            zuora_subscription_source.subscription_status as subscription_status,
            zuora_subscription_source.subscription_created_date
            as subscription_created_date,
            zuora_subscription_source.dim_crm_opportunity_id
            as subscription_source_opp_id,
            subscription_opps.opportunity_id as subscription_opp_id,
            coalesce(
                invoice_opps.opportunity_id,
                lag(invoice_opps.opportunity_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as invoice_opp_id_forward,
            coalesce(
                invoice_opps.opportunity_id,
                lead(invoice_opps.opportunity_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as invoice_opp_id_backward,
            coalesce(
                invoice_opps.opportunity_id,
                lag(invoice_opps.opportunity_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.term_start_date,
                        zuora_subscription_source.term_end_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as invoice_opp_id_forward_term_based,
            coalesce(
                invoice_opps.opportunity_id,
                lead(invoice_opps.opportunity_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.term_start_date,
                        zuora_subscription_source.term_end_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as invoice_opp_id_backward_term_based,
            coalesce(
                invoice_opps.opportunity_id,
                lag(invoice_opps.opportunity_id) ignore nulls over (
                    partition by zuora_subscription_source.subscription_name
                    order by zuora_subscription_source.subscription_version
                )
            ) as invoice_opp_id_forward_sub_name,
            invoice_opps.opportunity_id as unfilled_invoice_opp_id,
            coalesce(
                quote_opps.opportunity_id,
                lag(quote_opps.opportunity_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as quote_opp_id_forward,
            coalesce(
                quote_opps.opportunity_id,
                lead(quote_opps.opportunity_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as quote_opp_id_backward,
            coalesce(
                quote_opps.opportunity_id,
                lag(quote_opps.opportunity_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.term_start_date,
                        zuora_subscription_source.term_end_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as quote_opp_id_forward_term_based,
            coalesce(
                quote_opps.opportunity_id,
                lead(quote_opps.opportunity_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.term_start_date,
                        zuora_subscription_source.term_end_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as quote_opp_id_backward_term_based,
            coalesce(
                quote_opps.opportunity_id,
                lag(quote_opps.opportunity_id) ignore nulls over (
                    partition by zuora_subscription_source.subscription_name
                    order by zuora_subscription_source.subscription_version
                )
            ) as quote_opp_id_forward_sub_name,
            quote_opps.opportunity_id as unfilled_quote_opp_id,
            coalesce(
                subscription_quote_number_opps.opportunity_id,
                lag(subscription_quote_number_opps.opportunity_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as subscription_quote_number_opp_id_forward,
            coalesce(
                subscription_quote_number_opps.opportunity_id,
                lead(subscription_quote_number_opps.opportunity_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as subscription_quote_number_opp_id_backward,
            coalesce(
                subscription_quote_number_opps.opportunity_id,
                lag(subscription_quote_number_opps.opportunity_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.term_start_date,
                        zuora_subscription_source.term_end_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as subscription_quote_number_opp_id_forward_term_based,
            coalesce(
                subscription_quote_number_opps.opportunity_id,
                lead(subscription_quote_number_opps.opportunity_id) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.term_start_date,
                        zuora_subscription_source.term_end_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as subscription_quote_number_opp_id_backward_term_based,
            coalesce(
                subscription_quote_number_opps.opportunity_id,
                lag(subscription_quote_number_opps.opportunity_id) ignore nulls over (
                    partition by zuora_subscription_source.subscription_name
                    order by zuora_subscription_source.subscription_version
                )
            ) as subscription_quote_number_opp_id_forward_sub_name,
            subscription_quote_number_opps.opportunity_id
            as unfilled_subscription_quote_number_opp_id,
            case
                when
                    zuora_subscription_source.subscription_sales_type = 'Sales-Assisted'
                then
                    coalesce(
                        subscription_opp_id,
                        subscription_quote_number_opp_id_forward,
                        subscription_quote_number_opp_id_backward,
                        invoice_opp_id_forward,
                        invoice_opp_id_backward,
                        quote_opp_id_forward,
                        quote_opp_id_backward,
                        subscription_quote_number_opp_id_backward_term_based,
                        invoice_opp_id_backward_term_based,
                        invoice_opp_id_forward_term_based,
                        quote_opp_id_backward_term_based,
                        quote_opp_id_forward_term_based,
                        subscription_quote_number_opp_id_forward_sub_name,
                        invoice_opp_id_forward_sub_name,
                        quote_opp_id_forward_sub_name
                    )  -- prefer quote number on subscription if sales-assisted
                else
                    coalesce(
                        subscription_opp_id,
                        invoice_opp_id_forward,
                        invoice_opp_id_backward,
                        quote_opp_id_forward,
                        quote_opp_id_backward,
                        invoice_opp_id_backward_term_based,
                        invoice_opp_id_forward_term_based,
                        quote_opp_id_backward_term_based,
                        quote_opp_id_forward_term_based,
                        invoice_opp_id_forward_sub_name,
                        quote_opp_id_forward_sub_name
                    )  -- don't take quote_number on subscription for self-service
            end as combined_opportunity_id,
            coalesce(
                invoice_opps.invoice_opp_created_date,
                lead(invoice_opps.invoice_opp_created_date) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as invoice_opp_created_date_forward,
            coalesce(
                invoice_opps.invoice_opp_created_date,
                lag(invoice_opps.invoice_opp_created_date) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as invoice_opp_created_date_backward,
            coalesce(
                quote_opps.quote_opp_created_date,
                lead(quote_opps.quote_opp_created_date) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as quote_opp_created_date_forward,
            coalesce(
                quote_opps.quote_opp_created_date,
                lag(quote_opps.quote_opp_created_date) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as quote_opp_created_date_backward,
            coalesce(
                invoice_opps.invoice_opp_total_contract_value,
                lead(invoice_opps.invoice_opp_total_contract_value) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as invoice_opp_total_contract_value_forward,
            coalesce(
                invoice_opps.invoice_opp_total_contract_value,
                lag(invoice_opps.invoice_opp_total_contract_value) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as invoice_opp_total_contract_value_backward,
            coalesce(
                quote_opps.quote_opp_total_contract_value,
                lead(quote_opps.quote_opp_total_contract_value) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as quote_opp_total_contract_value_forward,
            coalesce(
                quote_opps.quote_opp_total_contract_value,
                lag(quote_opps.quote_opp_total_contract_value) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as quote_opp_total_contract_value_backward,
            coalesce(
                subscription_quote_number_opps.subscription_quote_number_opp_total_contract_value,
                lead(
                    subscription_quote_number_opps.subscription_quote_number_opp_total_contract_value
                ) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as subscription_quote_number_opp_total_contract_value_forward,
            coalesce(
                subscription_quote_number_opps.subscription_quote_number_opp_total_contract_value,
                lag(
                    subscription_quote_number_opps.subscription_quote_number_opp_total_contract_value
                ) ignore nulls over (
                    partition by
                        zuora_subscription_source.subscription_name,
                        zuora_subscription_source.subscription_created_date
                    order by zuora_subscription_source.subscription_version
                )
            ) as subscription_quote_number_opp_total_contract_value_backward,
            invoice_opps.invoice_number as invoice_number,
            invoice_opps.invoice_item_charge_amount as invoice_item_charge_amount,
            invoice_opps.invoice_item_quantity as invoice_item_quantity
        from zuora_subscription_source
        left join
            subscription_opps
            on zuora_subscription_source.dim_subscription_id
            = subscription_opps.subscription_id
        left join
            invoice_opps
            on zuora_subscription_source.dim_subscription_id
            = invoice_opps.subscription_id
        left join
            quote_opps
            on zuora_subscription_source.dim_subscription_id
            = quote_opps.subscription_id
        left join
            subscription_quote_number_opps
            on zuora_subscription_source.dim_subscription_id
            = subscription_quote_number_opps.subscription_id
        left join
            prep_crm_account
            on zuora_subscription_source.dim_crm_account_id
            = prep_crm_account.dim_crm_account_id

    ),
    final_subs_opps as (

        select final.*
        from final
        inner join
            zuora_account_source
            on final.dim_billing_account_id = zuora_account_source.account_id
        where subscription_created_date >= '2019-02-01'

    ),
    complete_subs as (

        select
            subscription_name,
            count_if(combined_opportunity_id is not null) as other_count_test,
            sum(
                case when combined_opportunity_id is not null then 1 else 0 end
            ) as count_test,
            count(dim_subscription_id) as sub_count
        from final_subs_opps
        group by 1

    ),
    -- All subscription_ids that do not have multiple opportunities associated with
    -- them
    non_duplicates as (

        select *
        from final_subs_opps
        where
            dim_subscription_id not in (
                select dim_subscription_id
                from final
                group by dim_subscription_id
                having count(*) > 1
            )

    ),
    -- GET ALL SUBSCRIPTION_IDS WITH MULTIPLE OPPORTUNITY_IDS, DUPLICATES (6,620)
    -- (4,600 -- with stage_name != '10-duplicate')
    dupes as (

        select *
        from final_subs_opps
        where
            dim_subscription_id in (
                select dim_subscription_id
                from final
                group by dim_subscription_id
                having count(*) > 1
            )

    ),
    invoice_item_amount as (

        select
            dim_invoice_id,
            invoice_number,
            dim_subscription_id,
            sum(invoice_item_charge_amount) as invoice_item_charge_amount,
            avg(quantity) as quantity
        from {{ ref("fct_invoice_item") }} {{ dbt_utils.group_by(n=3) }}

    ),
    multiple_opps_on_one_invoice as (

        select distinct
            ii.dim_subscription_id,
            dupes.subscription_name,
            dupes.subscription_version,
            ii.dim_invoice_id,
            ii.invoice_number,
            ii.quantity,
            to_varchar(quantity, '999,999,999,999') as formatted_quantity,
            trim(
                lower(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(opp.opportunity_name, '\\s+\\|{2}\\s+', '|'),
                            '[ ]{2,}',
                            ' '
                        ),
                        '[^A-Za-z0-9|]',
                        '-'
                    )
                )
            ) as opp_name_slugify,
            trim(
                lower(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(formatted_quantity, '\\s+\\|{2}\\s+', '|'),
                            '[ ]{2,}',
                            ' '
                        ),
                        '[^A-Za-z0-9|]',
                        '-'
                    )
                )
            ) as formatted_quantity_slugify,
            opp.dim_crm_opportunity_id,
            opp.opportunity_name,
            fct_opp.amount as opportunity_amount,
            ii.invoice_item_charge_amount,
            iff(
                round(opportunity_amount, 2) = round(ii.invoice_item_charge_amount, 2),
                5,
                0
            ) as opp_invoice_amount_match,
            iff(
                contains(opp_name_slugify, formatted_quantity_slugify), 5, 0
            ) as slugify_quantity_name_match,
            iff(
                contains(opportunity_name, formatted_quantity), 1, 0
            ) as formatted_quantity_name_match,
            opp_invoice_amount_match
            + slugify_quantity_name_match
            + formatted_quantity_name_match as total
        from dupes
        inner join
            invoice_item_amount ii
            on dupes.dim_subscription_id = ii.dim_subscription_id
            and dupes.invoice_number = ii.invoice_number
        inner join
            {{ ref("dim_crm_opportunity") }} as opp
            on ii.invoice_number = opp.invoice_number
        inner join
            {{ ref("fct_crm_opportunity") }} as fct_opp
            on opp.dim_crm_opportunity_id = fct_opp.dim_crm_opportunity_id
        where opp.stage_name <> '10-Duplicate'

    ),
    multiple_opps_on_one_invoice_matches as (

        select *
        from multiple_opps_on_one_invoice
        qualify
            row_number() over (partition by dim_subscription_id order by total desc) = 1

    ),
    dupes_with_amount_matches as (

        select dupes.*
        from dupes
        inner join
            multiple_opps_on_one_invoice_matches
            on dupes.dim_subscription_id
            = multiple_opps_on_one_invoice_matches.dim_subscription_id
            and dupes.unfilled_invoice_opp_id
            = multiple_opps_on_one_invoice_matches.dim_crm_opportunity_id
        where total > 0

    ),
    dupes_without_amount_matches as (

        select *
        from dupes
        where
            dim_subscription_id not in (
                select distinct dim_subscription_id from dupes_with_amount_matches
            )  -- 460 non-distinct, 200 distinct

    ),
    multi_invoice_subs_with_opp_amounts as (

        select
            dim_subscription_id,
            round(avg(invoice_item_charge_amount), 4) as invoice_amount,
            round(
                sum(invoice_opp_total_contract_value_forward), 4
            ) as invoice_opp_amount_forward,
            round(
                sum(invoice_opp_total_contract_value_backward), 4
            ) as invoice_opp_amount_backward,
            round(
                avg(quote_opp_total_contract_value_forward), 4
            ) as quote_opp_amount_forward,
            round(
                avg(quote_opp_total_contract_value_backward), 4
            ) as quote_opp_amount_backward,
            round(
                avg(subscription_quote_number_opp_total_contract_value_forward), 4
            ) as subscription_quote_number_opp_amount_forward,
            round(
                avg(subscription_quote_number_opp_total_contract_value_backward), 4
            ) as subscription_quote_number_opp_amount_backward
        from dupes_without_amount_matches
        group by 1

    ),
    multi_invoice_subs_with_opp_amounts_that_sum_to_invoice_total as (

        select *
        from multi_invoice_subs_with_opp_amounts
        where
            invoice_amount = invoice_opp_amount_forward
            or invoice_amount = invoice_opp_amount_backward

    ),
    multi_invoice_subs_with_opp_amounts_that_sum_to_invoice_total_first_opp as (

        select *
        from dupes
        where
            dim_subscription_id in (
                select distinct dim_subscription_id
                from multi_invoice_subs_with_opp_amounts_that_sum_to_invoice_total
            )
        qualify
            rank() over (
                partition by dim_subscription_id
                order by invoice_opp_created_date_forward
            )
            = 1

    ),
    final_matches_part_1 as (

        select *, 'non-duplicates' as source
        from non_duplicates

        union

        -- for invoices that have multiple subscriptions on the invoice, take the
        -- subscription-opportunity mapping where the invoice amount = opportunity
        -- amount
        select *, 'invoice amount matches opp amount' as source
        from dupes_with_amount_matches

        union

        -- for subscriptions spread across multiple invoices where the opp totals
        -- match the total across the invoices, take the first opportunity based on
        -- the opportunity created date
        select *, 'multi-invoice single sub' as source
        from multi_invoice_subs_with_opp_amounts_that_sum_to_invoice_total_first_opp

    ),
    -- the fixes applied to these duplicates are not as strong, so we are peeling them
    -- out and applying different solutions
    dupes_part_2 as (

        select *
        from dupes
        where
            dim_subscription_id
            not in (select distinct dim_subscription_id from final_matches_part_1)

    ),
    self_service_dupes_with_subscription_opp as (

        select *
        from dupes_part_2
        where
            subscription_sales_type = 'Self-Service' and subscription_opp_id is not null
        qualify
            rank() over (
                partition by dim_subscription_id
                order by
                    invoice_opp_id_forward,
                    invoice_opp_id_backward,
                    invoice_opp_id_backward_term_based
            )
            = 1

    ),
    sales_assisted_dupes_with_quote_num_on_sub as (

        select *
        from dupes_part_2
        where
            subscription_sales_type = 'Sales-Assisted'
            and coalesce(
                subscription_quote_number_opp_id_forward,
                subscription_quote_number_opp_id_backward,
                subscription_quote_number_opp_id_backward_term_based
            )
            is not null
        qualify
            rank() over (
                partition by dim_subscription_id
                order by
                    invoice_opp_id_forward,
                    invoice_opp_id_backward,
                    invoice_opp_id_backward_term_based
            )
            = 1

    ),
    dupes_all_raw_sub_options_match as (

        select *
        from dupes_part_2
        where
            unfilled_invoice_opp_id = unfilled_quote_opp_id
            and unfilled_quote_opp_id = unfilled_subscription_quote_number_opp_id
            and dim_subscription_id not in (
                select distinct dim_subscription_id
                from self_service_dupes_with_subscription_opp
                union
                select distinct dim_subscription_id
                from sales_assisted_dupes_with_quote_num_on_sub
            )

    ),
    final_matches as (

        select *
        from final_matches_part_1
        union

        -- for self-service dupes, take the most reliable connection (opportunity id
        -- on subscription)
        select *, 'self-service' as source
        from self_service_dupes_with_subscription_opp

        union

        -- for sales_assisted dupes, take the most reliable connection (quote number
        -- on subscription)
        select *, 'sales-assisted' as source
        from sales_assisted_dupes_with_quote_num_on_sub

        union

        -- for all dupes, take the subscription-opportunity options where the raw
        -- fields (opp on subscription, opp on invoice, and opp on quote number from
        -- subscription) match
        select *, 'all matching opps' as source
        from dupes_all_raw_sub_options_match

    ),
    final_matches_with_bad_data_flag as (

        select
            final_matches.*,
            iff(
                len(
                    split_part(
                        combined_opportunity_id, 'https://gitlab.my.salesforce.com/', 2
                    )
                )
                = 0,
                null,
                split_part(
                    combined_opportunity_id, 'https://gitlab.my.salesforce.com/', 2
                )
            ) as opp_id_remove_salesforce_url,
            {{ zuora_slugify("combined_opportunity_id") }} as opp_id_slugify,
            opp_name.opportunity_id as opp_id_name,
            coalesce(
                opp_id_remove_salesforce_url,
                opp_id_name,
                iff(
                    combined_opportunity_id not like '0%',
                    opp_id_slugify,
                    combined_opportunity_id
                )
            ) as combined_oportunity_id_coalesced,
            case
                when
                    subscription_opp_id is null
                    and invoice_opp_id_forward is null
                    and invoice_opp_id_backward is null
                    and invoice_opp_id_forward_term_based is null
                    and invoice_opp_id_backward_term_based is null
                    and unfilled_invoice_opp_id is null
                    and quote_opp_id_forward is null
                    and quote_opp_id_backward is null
                    and quote_opp_id_forward_term_based is null
                    and quote_opp_id_backward_term_based is null
                    and unfilled_quote_opp_id is null
                    and subscription_quote_number_opp_id_forward is null
                    and subscription_quote_number_opp_id_backward is null
                    and subscription_quote_number_opp_id_forward_term_based is null
                    and subscription_quote_number_opp_id_backward_term_based is null
                    and subscription_quote_number_opp_id_forward_sub_name is null
                    and unfilled_subscription_quote_number_opp_id is null
                    and (
                        invoice_opp_id_forward_sub_name is not null
                        or subscription_quote_number_opp_id_forward_sub_name is not null
                        or quote_opp_id_forward_sub_name is not null
                    )
                then 1
                else 0
            end as is_questionable_opportunity_mapping
        from final_matches
        left join
            {{ ref("sfdc_opportunity_source") }} opp_name
            on {{ zuora_slugify("final_matches.combined_opportunity_id") }}
            = {{ zuora_slugify("opp_name.opportunity_name") }}

    ),
    short_oppty_id as (

        select
            opportunity_id as long_oppty_id, left(opportunity_id, 15) as short_oppty_id
        from {{ ref("sfdc_opportunity_source") }}

    ),
    final_matches_with_long_oppty_id as (

        select
            final_matches_with_bad_data_flag.*,
            short_oppty_id.long_oppty_id as dim_crm_opportunity_id
        from final_matches_with_bad_data_flag
        left join
            short_oppty_id
            on left(
                final_matches_with_bad_data_flag.combined_oportunity_id_coalesced, 15
            )
            = short_oppty_id.short_oppty_id

    )

    {{
        dbt_audit(
            cte_ref="final_matches_with_long_oppty_id",
            created_by="@michellecooper",
            updated_by="@michellecooper",
            created_date="2021-11-10",
            updated_date="2022-01-19",
        )
    }}
