{{
    simple_cte(
        [
            ("saas_usage_ping_namespace", "saas_usage_ping_namespace"),
            ("dim_date", "dim_date"),
        ]
    )
}}

,
transformed as (

    select
        saas_usage_ping_gitlab_dotcom_namespace_id,
        namespace_ultimate_parent_id as dim_namespace_id,
        ping_name as ping_name,  -- potentially renamed
        ping_date,  -- currently wrong date input in the airflow run
        to_date(_uploaded_at) as run_date,
        counter_value
    from saas_usage_ping_namespace
    where error = 'Success'

),
joined as (

    select
        saas_usage_ping_gitlab_dotcom_namespace_id,
        dim_namespace_id,
        ping_name,
        ping_date,
        counter_value
    from transformed
    inner join dim_date on ping_date = date_day

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@mpeychet_",
        updated_by="@ischweickartDD",
        created_date="2021-03-22",
        updated_date="2021-04-05",
    )
}}
