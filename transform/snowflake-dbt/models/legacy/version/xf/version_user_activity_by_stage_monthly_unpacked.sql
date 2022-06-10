with
    usage_data as (select * from {{ ref("version_usage_data_with_metadata") }}),
    unpacked_stage_json as (

        select usage_data.*, f.key as stage_name, f.value as stage_activity_count_json

        from
            usage_data,
            lateral flatten(input => usage_data.usage_activity_by_stage_monthly) f
        where
            is_object(f.value) = true
            {% if is_incremental() %}
            and created_at > (select max(created_at) from {{ this }})
            {% endif %}

    ),
    unpacked_other_metrics as (

        select
            usage_data.id,
            usage_data.version,
            usage_data.created_at,
            usage_data.uuid,
            usage_data.edition,
            usage_data.ping_source,
            usage_data.major_version,
            usage_data.main_edition,
            usage_data.edition_type,
            usage_data.license_plan_code,
            usage_data.company,
            usage_data.zuora_subscription_id,
            usage_data.zuora_subscription_status,
            usage_data.zuora_crm_id,
            null as stage_name,
            dateadd('days', -28, usage_data.created_at) as period_start,
            usage_data.created_at as period_end,
            f.key as usage_action_name,
            iff(f.value = -1, 0, f.value) as usage_action_count

        from usage_data, lateral flatten(input => usage_data.analytics_unique_visits) f
        where
            is_real(f.value) = true
            {% if is_incremental() %}
            and created_at >= (select max(created_at) from {{ this }})
            {% endif %}

        UNION

        select
            usage_data.id,
            usage_data.version,
            usage_data.created_at,
            usage_data.uuid,
            usage_data.edition,
            usage_data.ping_source,
            usage_data.major_version,
            usage_data.main_edition,
            usage_data.edition_type,
            usage_data.license_plan_code,
            usage_data.company,
            usage_data.zuora_subscription_id,
            usage_data.zuora_subscription_status,
            usage_data.zuora_crm_id,
            'manage' as stage_name,
            dateadd('days', -28, usage_data.created_at) as period_start,
            usage_data.created_at as period_end,
            f.key as usage_action_name,
            iff(f.value = -1, 0, f.value) as usage_action_count

        from
            usage_data,
            lateral flatten(
                input => usage_data.raw_usage_data_payload,
                recursive => true,
                path => 'redis_hll_counters'
            ) f
        where
            is_real(f.value) = true
            {% if is_incremental() %}
            and created_at >= (select max(created_at) from {{ this }})
            {% endif %}


    ),
    unpacked_stage_metrics as (

        select
            unpacked_stage_json.id,
            unpacked_stage_json.version,
            unpacked_stage_json.created_at,
            unpacked_stage_json.uuid,
            unpacked_stage_json.edition,
            unpacked_stage_json.ping_source,
            unpacked_stage_json.major_version,
            unpacked_stage_json.main_edition,
            unpacked_stage_json.edition_type,
            unpacked_stage_json.license_plan_code,
            unpacked_stage_json.company,
            unpacked_stage_json.zuora_subscription_id,
            unpacked_stage_json.zuora_subscription_status,
            unpacked_stage_json.zuora_crm_id,
            unpacked_stage_json.stage_name,
            dateadd('days', -28, unpacked_stage_json.created_at) as period_start,
            unpacked_stage_json.created_at as period_end,
            f.key as usage_action_name,
            iff(f.value = -1, 0, f.value) as usage_action_count
        from
            unpacked_stage_json,
            lateral flatten(input => unpacked_stage_json.stage_activity_count_json) f

    )

    ,
    final as (

        select * from unpacked_stage_metrics UNION select * from unpacked_other_metrics

    )
select *
from final
