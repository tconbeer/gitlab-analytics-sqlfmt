with
    source as (select * from {{ source("zuora_api_sandbox", "order_action") }}),
    renamed as (

        select

            id as dim_order_action_id,

            -- keys
            orderid as dim_order_id,
            subscriptionid as dim_subscription_id,
            subscriptionversionamendmentid as dim_amendment_id,

            -- account info
            type as order_action_type,
            sequence as order_action_sequence,
            autorenew as is_auto_renew,
            cancellationpolicy as cancellation_policy,
            termtype as term_type,

            customeracceptancedate::date as customer_acceptance_date,
            contracteffectivedate::date as contract_effective_date,
            serviceactivationdate::date as service_activation_date,

            currentterm as current_term,
            currenttermperiodtype as current_term_period_type,

            renewalterm as renewal_term,
            renewaltermperiodtype as renewal_term_period_type,
            renewsetting as renewal_setting,

            termstartdate::date as term_start_date,

            -- metadata
            createddate::date as order_action_created_date,
            createdbyid as order_action_created_by_id,
            updateddate::date as updated_date,
            updatedbyid as updated_by_id,
            deleted as is_deleted

        from source

    )

select *
from renamed
