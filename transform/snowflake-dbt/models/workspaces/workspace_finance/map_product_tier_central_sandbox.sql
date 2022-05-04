WITH zuora_central_sandbox_product AS (

    SELECT *
    FROM {{ ref('zuora_central_sandbox_product_source') }}

), zuora_central_sandbox_product_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_central_sandbox_product_rate_plan_source') }}
    WHERE is_deleted = FALSE

), final AS (

    SELECT
      zuora_central_sandbox_product_rate_plan.product_rate_plan_id                  AS product_rate_plan_id,
      zuora_central_sandbox_product_rate_plan.product_rate_plan_name                AS product_rate_plan_name,
      CASE
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%saas - ultimate%'
          THEN 'SaaS - Ultimate'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%saas - premium%'
          THEN 'SaaS - Premium'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%ultimate%'
          THEN 'Self-Managed - Ultimate'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%premium%'
          THEN 'Self-Managed - Premium'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'gold%'
          THEN 'SaaS - Gold'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'silver%'
          THEN 'SaaS - Silver'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%bronze%'
          THEN 'SaaS - Bronze'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%starter%'
          THEN 'Self-Managed - Starter'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'gitlab enterprise edition%'
          THEN 'Self-Managed - Starter'
        WHEN zuora_central_sandbox_product_rate_plan.product_rate_plan_name = 'Pivotal Cloud Foundry Tile for GitLab EE'
          THEN 'Self-Managed - Starter'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'plus%'
          THEN 'Plus'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'standard%'
          THEN 'Standard'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'basic%'
          THEN 'Basic'
        WHEN zuora_central_sandbox_product_rate_plan.product_rate_plan_name = 'Trueup'
          THEN 'Trueup'
        WHEN LTRIM(LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name)) LIKE 'githost%'
          THEN 'GitHost'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE ANY ('%quick start with ha%', '%proserv training per-seat add-on%')
          THEN 'Support'
        WHEN TRIM(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) IN (
                                                                        'GitLab Service Package'
                                                                      , 'Implementation Services Quick Start'
                                                                      , 'Implementation Support'
                                                                      , 'Support Package'
                                                                      , 'Admin Training'
                                                                      , 'CI/CD Training'
                                                                      , 'GitLab Project Management Training'
                                                                      , 'GitLab with Git Basics Training'
                                                                      , 'Travel Expenses'
                                                                      , 'Training Workshop'
                                                                      , 'GitLab for Project Managers Training - Remote'
                                                                      , 'GitLab with Git Basics Training - Remote'
                                                                      , 'GitLab for System Administrators Training - Remote'
                                                                      , 'GitLab CI/CD Training - Remote'
                                                                      , 'InnerSourcing Training - Remote for your team'
                                                                      , 'GitLab DevOps Fundamentals Training'
                                                                      , 'Self-Managed Rapid Results Consulting'
                                                                      , 'Gitlab.com Rapid Results Consulting'
                                                                      , 'GitLab Security Essentials Training - Remote Delivery'
                                                                      , 'InnerSourcing Training - At your site'
                                                                      , 'Migration+'
                                                                      , 'One Time Discount'
                                                                      , 'LDAP Integration'
                                                                      , 'Dedicated Implementation Services'
                                                                      , 'Quick Start without HA, less than 500 users'
                                                                      , 'Jenkins Integration'
                                                                      , 'Hourly Consulting'
                                                                      , 'JIRA Integration'
                                                                      , 'Custom PS Education Services'
                                                                     )
          THEN 'Support'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'gitlab geo%'
          THEN 'Other'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'ci runner%'
          THEN 'Other'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'discount%'
          THEN 'Other'
        WHEN TRIM(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) IN (
                                                                        '#movingtogitlab'
                                                                      , 'File Locking'
                                                                      , 'Payment Gateway Test'
                                                                      , 'Time Tracking'
                                                                      , '1,000 CI Minutes'
                                                                      , 'Gitlab Storage 10GB'
                                                                      , 'EdCast Settlement Revenue'
                                                                     )
          THEN 'Other'
        ELSE 'Not Applicable'
      END                                                                       AS product_tier_historical,
      CASE
        WHEN LOWER(product_tier_historical) LIKE '%self-managed%'
          THEN 'Self-Managed'
        WHEN LOWER(product_tier_historical) LIKE '%saas%'
          THEN 'SaaS'
        WHEN product_tier_historical IN (
                                          'Basic'
                                        , 'GitHost'
                                        , 'Other'
                                        , 'Plus'
                                        , 'Standard'
                                        , 'Support'
                                        , 'Trueup'
                                        )
          THEN 'Others'
        ELSE NULL
      END                                                                       AS product_delivery_type,
      CASE
        WHEN product_tier_historical IN (
                                          'SaaS - Gold'
                                        , 'Self-Managed - Ultimate'
                                        , 'SaaS - Ultimate'
                                        )
          THEN 3
        WHEN product_tier_historical IN (
                                          'SaaS - Silver'
                                        , 'Self-Managed - Premium'
                                        , 'SaaS - Premium'
                                        )
          THEN 2
        WHEN product_tier_historical IN (
                                          'SaaS - Bronze'
                                        , 'Self-Managed - Starter'
                                        )
          THEN 1
        ELSE 0
      END                                                                       AS product_ranking,
      CASE
        WHEN product_tier_historical = 'SaaS - Gold'
          THEN 'SaaS - Ultimate'
        WHEN product_tier_historical = 'SaaS - Silver'
          THEN 'SaaS - Premium'
        ELSE product_tier_historical
      END                                                                       AS product_tier
    FROM zuora_central_sandbox_product
    INNER JOIN zuora_central_sandbox_product_rate_plan
      ON zuora_central_sandbox_product.product_id = zuora_central_sandbox_product_rate_plan.product_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-03-31",
    updated_date="2022-03-31"
) }}