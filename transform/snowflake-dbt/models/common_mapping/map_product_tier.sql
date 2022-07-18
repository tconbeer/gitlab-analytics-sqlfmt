with
    zuora_product as (select * from {{ ref("zuora_product_source") }}),
    zuora_product_rate_plan as (

        select *
        from {{ ref("zuora_product_rate_plan_source") }}
        where is_deleted = false

    ),
    final as (

        select
            zuora_product_rate_plan.product_rate_plan_id as product_rate_plan_id,
            zuora_product_rate_plan.product_rate_plan_name as product_rate_plan_name,
            case
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name)
                    like '%saas - ultimate%'
                then 'SaaS - Ultimate'
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name)
                    like '%saas - premium%'
                then 'SaaS - Premium'
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name)
                    like '%ultimate%'
                then 'Self-Managed - Ultimate'
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name)
                    like '%premium%'
                then 'Self-Managed - Premium'
                when lower(zuora_product_rate_plan.product_rate_plan_name) like 'gold%'
                then 'SaaS - Gold'
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name) like 'silver%'
                then 'SaaS - Silver'
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name)
                    like '%bronze%'
                then 'SaaS - Bronze'
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name)
                    like '%starter%'
                then 'Self-Managed - Starter'
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name)
                    like 'gitlab enterprise edition%'
                then 'Self-Managed - Starter'
                when
                    zuora_product_rate_plan.product_rate_plan_name
                    = 'Pivotal Cloud Foundry Tile for GitLab EE'
                then 'Self-Managed - Starter'
                when lower(zuora_product_rate_plan.product_rate_plan_name) like 'plus%'
                then 'Plus'
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name)
                    like 'standard%'
                then 'Standard'
                when lower(zuora_product_rate_plan.product_rate_plan_name) like 'basic%'
                then 'Basic'
                when zuora_product_rate_plan.product_rate_plan_name = 'Trueup'
                then 'Trueup'
                when
                    ltrim(lower(zuora_product_rate_plan.product_rate_plan_name))
                    like 'githost%'
                then 'GitHost'
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name) like any (
                        '%quick start with ha%', '%proserv training per-seat add-on%'
                    )
                then 'Support'
                when
                    trim(zuora_product_rate_plan.product_rate_plan_name) in (
                        'GitLab Service Package',
                        'Implementation Services Quick Start',
                        'Implementation Support',
                        'Support Package',
                        'Admin Training',
                        'CI/CD Training',
                        'GitLab Project Management Training',
                        'GitLab with Git Basics Training',
                        'Travel Expenses',
                        'Training Workshop',
                        'GitLab for Project Managers Training - Remote',
                        'GitLab with Git Basics Training - Remote',
                        'GitLab for System Administrators Training - Remote',
                        'GitLab CI/CD Training - Remote',
                        'InnerSourcing Training - Remote for your team',
                        'GitLab DevOps Fundamentals Training',
                        'Self-Managed Rapid Results Consulting',
                        'Gitlab.com Rapid Results Consulting',
                        'GitLab Security Essentials Training - Remote Delivery',
                        'InnerSourcing Training - At your site',
                        'Migration+',
                        'One Time Discount',
                        'LDAP Integration',
                        'Dedicated Implementation Services',
                        'Quick Start without HA, less than 500 users',
                        'Jenkins Integration',
                        'Hourly Consulting',
                        'JIRA Integration',
                        'Custom PS Education Services'
                    )
                then 'Support'
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name)
                    like 'gitlab geo%'
                then 'SaaS - Other'
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name)
                    like 'ci runner%'
                then 'SaaS - Other'
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name)
                    like 'discount%'
                then 'Other'
                when
                    trim(zuora_product_rate_plan.product_rate_plan_name) in (
                        '#movingtogitlab',
                        'Payment Gateway Test',
                        'EdCast Settlement Revenue'
                    )
                then 'Other'
                when
                    trim(zuora_product_rate_plan.product_rate_plan_name) in (
                        'File Locking', 'Time Tracking', '1,000 CI Minutes'
                    )
                then 'SaaS - Other'
                when
                    trim(zuora_product_rate_plan.product_rate_plan_name) in (
                        'Gitlab Storage 10GB'
                    )
                then 'Storage'
                else 'Not Applicable'
            end as product_tier_historical,
            case
                when lower(product_tier_historical) like '%self-managed%'
                then 'Self-Managed'
                when
                    lower(product_tier_historical) like any (
                        '%saas%', 'storage', 'standard', 'basic', 'plus', 'githost'
                    )
                then 'SaaS'
                when lower(product_tier_historical) = 'SaaS - Other'
                then 'SaaS'
                when product_tier_historical in ('Other', 'Support', 'Trueup')
                then 'Others'
                else null
            end as product_delivery_type,
            case
                when
                    product_tier_historical in (
                        'SaaS - Gold', 'Self-Managed - Ultimate', 'SaaS - Ultimate'
                    )
                then 3
                when
                    product_tier_historical in (
                        'SaaS - Silver', 'Self-Managed - Premium', 'SaaS - Premium'
                    )
                then 2
                when
                    product_tier_historical in (
                        'SaaS - Bronze', 'Self-Managed - Starter'
                    )
                then 1
                else 0
            end as product_ranking,
            case
                when product_tier_historical = 'SaaS - Gold'
                then 'SaaS - Ultimate'
                when product_tier_historical = 'SaaS - Silver'
                then 'SaaS - Premium'
                else product_tier_historical
            end as product_tier
        from zuora_product
        inner join
            zuora_product_rate_plan
            on zuora_product.product_id = zuora_product_rate_plan.product_id

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@ischweickartDD",
            updated_by="@jpeguero",
            created_date="2020-12-14",
            updated_date="2021-09-17",
        )
    }}
