with
    zuora_amendment as (

        select * from {{ ref("zuora_amendment_source") }} where is_deleted = false

    ),
    base as (

        select
            -- Surrogate Key
            amendment_id as dim_amendment_id,

            -- Common Dimension keys
            subscription_id as dim_subscription_id,

            -- Information
            amendment_name,
            amendment_type,
            amendment_description,
            auto_renew,
            amendment_code,
            amendment_status,

            -- Term information
            term_type,
            current_term,
            current_term_period_type,
            renewal_term,
            renewal_term_period_type,
            renewal_setting,

            -- Dates
            term_start_date,
            effective_date,
            service_activation_date,
            customer_acceptance_date,
            contract_effective_date
        from zuora_amendment

        union all

        select
            -- Surrogate Keys
            md5('-1') as dim_amendment_id,

            -- Common Dimension keys
            md5('-1') as dim_subscription_id,

            -- Information
            'Missing amendment_name' as amendment_name,
            'Missing amendment_type' as amendment_type,
            'Missing amendment_description' as amendment_description,
            0 as auto_renew,
            'Missing amendment_code' as amendment_code,
            'Missing amendment_status' as amendment_status,

            -- Term information
            'Missing term_type' as term_type,
            -1 as current_term,
            'Missing current_term_period_type' as current_term_period_type,
            -1 as renewal_term,
            'Missing renewal_term_period_type' as renewal_term_period_type,
            'Missing renewal_setting' as renewal_setting,

            -- Dates
            '9999-12-31 00:00:00.000 +0000' as term_start_date,
            '9999-12-31 00:00:00.000 +0000' as effective_date,
            '9999-12-31 00:00:00.000 +0000' as service_activation_date,
            '9999-12-31 00:00:00.000 +0000' as customer_acceptance_date,
            '9999-12-31 00:00:00.000 +0000' as contract_effective_date

    )

    {{
        dbt_audit(
            cte_ref="base",
            created_by="@iweeks",
            updated_by="@iweeks",
            created_date="2021-05-10",
            updated_date="2021-05-10",
        )
    }}
