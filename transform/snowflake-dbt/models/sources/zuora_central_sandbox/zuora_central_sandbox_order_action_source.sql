with
    source as (select * from {{ source("zuora_central_sandbox", "order_action") }}),
    renamed as (

        select
            -- Keys
            id as order_action_id,

            order_id as order_id,
            account_id as account_id,
            subscription_version_amendment_id as subscription_version_amendment_id,
            subscription_id as subscription_id,
            default_payment_method_id as default_payment_method_id,
            bill_to_contact_id as bill_to_contact_id,
            sold_to_contact_id as sold_to_contact_id,

            auto_renew as auto_renew,
            cancellation_effective_date as cancellation_effective_date,
            cancellation_policy as cancellation_policy,
            contract_effective_date as contract_effective_date,
            current_term as current_term,
            current_term_period_type as current_term_period_type,
            customer_acceptance_date as customer_acceptance_date,
            renewal_term as renewal_term,
            renewal_term_period_type as renewal_term_period_type,
            renew_setting as renew_setting,
            resume_date as resume_date,
            sequence as sequence,
            service_activation_date as service_activation_date,
            suspend_date as suspend_date,
            term_start_date as term_start_date,
            term_type as term_type,
            type as type,

            -- metadata
            updated_by_id as updated_by_id,
            updated_date as updated_date,
            created_by_id as created_by_id,
            created_date as created_date,

            _fivetran_deleted as is_deleted

        from source

    )

select *
from renamed
