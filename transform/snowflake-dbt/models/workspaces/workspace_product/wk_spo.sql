{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "table"}) }}

{{
    simple_cte(
        [
            ("wk_saas_spo", "wk_saas_spo"),
            ("wk_self_managed_spo", "wk_self_managed_spo"),
        ]
    )
}}

select
    reporting_month,
    organization_id,
    delivery,
    organization_type,
    product_tier,
    is_paid_product_tier,
    umau_value,
    configure_stage,
    create_stage,
    manage_stage,
    monitor_stage,
    package_stage,
    plan_stage,
    protect_stage,
    release_stage,
    secure_stage,
    verify_stage,
    active_stage_count
from wk_self_managed_spo

union

select
    reporting_month,
    organization_id,
    delivery,
    organization_type,
    product_tier,
    is_paid_product_tier,
    umau_value,
    configure_stage,
    create_stage,
    manage_stage,
    monitor_stage,
    package_stage,
    plan_stage,
    protect_stage,
    release_stage,
    secure_stage,
    verify_stage,
    active_stage_count
from wk_saas_spo
