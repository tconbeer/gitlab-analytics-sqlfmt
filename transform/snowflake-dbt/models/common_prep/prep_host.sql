{{ simple_cte([("version_hosts_source", "version_hosts_source")]) }}

,
renamed as (

    select host_id as dim_host_id, host_url as host_name from version_hosts_source

)

{{
    dbt_audit(
        cte_ref="renamed",
        created_by="@mpeychet_",
        updated_by="@chrissharp",
        created_date="2021-05-20",
        updated_date="2022-03-03",
    )
}}
