{%- macro map_ci_pipeline_failure_reason(failure_reason_id) -%}

case
    when {{ failure_reason_id }}::number = 0
    then 'unknown_failure'
    when {{ failure_reason_id }}::number = 1
    then 'config_error'
    when {{ failure_reason_id }}::number = 2
    then 'external_validation_failure'
    when {{ failure_reason_id }}::number = 3
    then 'user_not_verified'
    when {{ failure_reason_id }}::number = 20
    then 'activity_limit_exceeded'
    when {{ failure_reason_id }}::number = 21
    then 'size_limit_exceeded'
    when {{ failure_reason_id }}::number = 22
    then 'job_activity_limit_exceeded'
    when {{ failure_reason_id }}::number = 23
    then 'deployments_limit_exceeded'
    when {{ failure_reason_id }}::number = 24
    then 'user_blocked'
    when {{ failure_reason_id }}::number = 25
    then 'project_deleted'
    else null
end

{%- endmacro -%}
