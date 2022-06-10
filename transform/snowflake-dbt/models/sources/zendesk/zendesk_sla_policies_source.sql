with
    source as (select * from {{ source("zendesk", "sla_policies") }}),
    renamed as (

        select
            id::varchar as zendesk_sla_policy_id,
            title::varchar as zendesk_sla_title,
            description::varchar as zendesk_sla_description,
            filter_all.value['field']::varchar as filter_all_field,
            filter_all.value['operator']::varchar as filter_all_operator,
            filter_all.value['value']::varchar as filter_all_value,
            filter_any.value['field']::varchar as filter_any_field,
            filter_any.value['operator']::varchar as filter_any_operator,
            filter_any.value['value']::varchar as filter_any_value,
            policy_metrics.value[
                'business_hours'
            ]::varchar as policy_metrics_business_hours,
            policy_metrics.value['metric']::varchar as policy_metrics_metric,
            policy_metrics.value['priority']::varchar as policy_metrics_priority,
            policy_metrics.value['target']::varchar as policy_metrics_target
        from
            source,
            lateral flatten(input => parse_json(filter__all), outer => true) filter_all,
            lateral flatten(input => parse_json(filter__any), outer => true) filter_any,
            lateral flatten(
                input => parse_json(policy_metrics), outer => true
            ) policy_metrics

    ),
    keyed as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "zendesk_sla_policy_id",
                        "filter_all_field",
                        "filter_all_operator",
                        "filter_all_value",
                        "filter_any_field",
                        "filter_any_operator",
                        "filter_any_value",
                        "policy_metrics_business_hours",
                        "policy_metrics_metric",
                        "policy_metrics_priority",
                        "policy_metrics_target",
                    ]
                )
            }} as zendesk_sla_surrogate_key, *
        from renamed

    )

select *
from keyed
