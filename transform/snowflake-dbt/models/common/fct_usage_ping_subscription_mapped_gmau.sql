{{ config(tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("subscriptions", "bdg_subscription_product_rate_plan"),
            ("dates", "dim_date"),
            ("gmau_metrics", "prep_usage_ping_subscription_mapped_gmau"),
        ]
    )
}}

{%- set gmau_metrics = dbt_utils.get_query_results_as_dict(
    "SELECT DISTINCT        group_name || '_' || sql_friendly_name   AS name,        sql_friendly_path                        AS path     FROM "
    ~ ref("dim_key_xmau_metric")
    ~ " WHERE is_gmau       OR is_paid_gmau     ORDER BY name"
) -%}

,
sm_subscriptions as (

    select distinct
        dim_subscription_id,
        dim_subscription_id_original,
        dim_billing_account_id,
        first_day_of_month as snapshot_month
    from subscriptions
    -- first month Usage Ping was collected
    inner join dates on date_actual between '2017-04-01' and current_date
    where product_delivery_type = 'Self-Managed'

),
gmau_monthly as (

    select *
    from gmau_metrics
    where dim_subscription_id is not null and ping_source = 'Self-Managed'
    qualify
        row_number() OVER (
            partition by dim_subscription_id, uuid, hostname, ping_created_at_month
            order by ping_created_at desc
        )
        = 1

),
joined as (

    select
        sm_subscriptions.dim_subscription_id,
        sm_subscriptions.snapshot_month,
        {{ get_date_id("sm_subscriptions.snapshot_month") }} as snapshot_date_id,
        sm_subscriptions.dim_subscription_id_original,
        sm_subscriptions.dim_billing_account_id,
        gmau_monthly.dim_crm_account_id,
        gmau_monthly.dim_parent_crm_account_id,
        gmau_monthly.dim_usage_ping_id,
        gmau_monthly.uuid,
        gmau_monthly.hostname,
        gmau_monthly.dim_license_id,
        gmau_monthly.license_md5,
        gmau_monthly.cleaned_version,
        gmau_monthly.ping_created_at,
        {{ get_date_id("gmau_monthly.ping_created_at") }} as ping_created_date_id,
        gmau_monthly.dim_location_country_id,

        {%- for metric in gmau_metrics.NAME %}
        {{ metric }} as {{ gmau_metrics.NAME[loop.index0] }}
        {%- if not loop.last %},{% endif -%}
        {% endfor %},

        iff(
            row_number() OVER (
                partition by
                    gmau_monthly.dim_subscription_id,
                    gmau_monthly.uuid,
                    gmau_monthly.hostname
                order by gmau_monthly.ping_created_at desc
            )
            = 1,
            true,
            false
        ) as is_latest_gmau_reported
    from sm_subscriptions
    left join
        gmau_monthly
        on sm_subscriptions.dim_subscription_id = gmau_monthly.dim_subscription_id
        and sm_subscriptions.snapshot_month = gmau_monthly.ping_created_at_month

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@ischweickartDD",
        updated_by="@chrissharp",
        created_date="2021-03-15",
        updated_date="2022-02-23",
    )
}}
