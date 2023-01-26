-- fields with corresponding columns in sfdc_opportunity_xf
{% set fields_to_use = [
    "amount",
    "closedate",
    "forecastcategoryname",
    "incremental_acv_2__c",
    "leadsource",
    "renewal_acv__c",
    "renewal_amount__c",
    "sales_accepted_date__c",
    "sales_qualified_date__c",
    "sales_segmentation_employees_o__c",
    "sql_source__c",
    "stagename",
    "swing_deal__c",
    "type",
    "ultimate_parent_sales_segment_emp_o__c",
    "ultimate_parent_sales_segment_o__c",
    "upside_iacv__c",
    "recurring_amount__c",
    "true_up_amount__c",
    "proserv_amount__c",
    "other_non_recurring_amount__c",
    "arr_net__c",
    "arr_basis__c",
    "arr__c",
    "start_date__c",
    "end_date__c",
] %}

with
    date_spine as (

        select distinct date_trunc('day', date_day) as date_actual
        from {{ ref("date_details") }}
        where date_day >= '2019-02-01'::date and date_day <= '2019-10-01'::date

    ),
    net_arr_net_iacv_conversion_factors as (

        select * from {{ ref("sheetload_net_arr_net_iacv_conversion_factors_source") }}

    ),
    first_snapshot as (

        select
            id as opportunity_id,
            valid_to,
            {% for field in fields_to_use %}
            {{ field }}::varchar as {{ field }},
            {% endfor %}
            createddate as created_at,
            valid_from
        from {{ ref("sfdc_opportunity_snapshots_base") }}
        where date_actual = '2019-10-01'::date and isdeleted = false

    ),
    base as (

        select
            field_history.opportunity_id,
            field_modified_at as valid_to,
            opportunity_field,
            coalesce(old_value, 'true null') as old_value  -- retain record of fields that transitioned from NULL to another state
        from {{ ref("sfdc_opportunity_field_history") }} field_history
        inner join
            first_snapshot
            on field_history.field_modified_at <= first_snapshot.valid_from
            and field_history.opportunity_id = first_snapshot.opportunity_id
        where opportunity_field in ('{{ fields_to_use | join ("', '") }}')

    ),
    unioned as (

        select *
        from first_snapshot

        union

        select *, null::timestamp_tz as created_at, null::timestamp_tz as valid_from
        from
            base pivot (
                max(old_value) for opportunity_field
                in ('{{ fields_to_use | join ("', '") }}')
            )

    ),
    filled as (

        select
            opportunity_id,
            {% for field in fields_to_use %}
            first_value({{ field }}) ignore nulls over (
                partition by opportunity_id
                order by valid_to
                rows between current row and unbounded following
            ) as {{ field }},
            {% endfor %}
            first_value(created_at) ignore nulls over (
                partition by opportunity_id order by valid_to
            ) as created_date,
            coalesce(
                lag(valid_to) over (partition by opportunity_id order by valid_to),
                created_date
            ) as valid_from,
            valid_to
        from unioned

    ),
    cleaned as (

        select
            opportunity_id,
            {% for field in fields_to_use %}
            iff({{ field }} = 'true null', null, {{ field }}) as {{ field }},
            {% endfor %}
            created_date,
            valid_from,
            coalesce(
                lead(valid_from) over (partition by opportunity_id order by valid_from),
                valid_to
            ) as valid_to
        from filled
        qualify
            row_number() over (
                partition by opportunity_id, date_trunc('day', valid_from)
                order by valid_from desc
            )
            = 1

    ),
    joined as (

        select
            date_actual,
            valid_from,
            valid_to,
            iff(valid_to is null, true, false) as is_currently_valid,
            cleaned.opportunity_id,
            closedate::date as close_date,
            created_date::date as created_date,
            sql_source__c as generated_source,
            leadsource as lead_source,
            coalesce(
                {{ sales_segment_cleaning("ultimate_parent_sales_segment_emp_o__c") }},
                {{ sales_segment_cleaning("ultimate_parent_sales_segment_o__c") }}
            ) as parent_segment,
            sales_accepted_date__c::date as sales_accepted_date,
            sales_qualified_date__c::date as sales_qualified_date,
            start_date__c::date as subscription_start_date,
            end_date__c::date as subscription_end_date,
            coalesce(
                {{ sales_segment_cleaning("sales_segmentation_employees_o__c") }},
                'Unknown'
            ) as sales_segment,
            type as sales_type,
            {{ sfdc_source_buckets("leadsource") }}
            stagename as stage_name,
            {{ sfdc_deal_size("incremental_acv_2__c::FLOAT", "deal_size") }},
            forecastcategoryname as forecast_category_name,
            incremental_acv_2__c::float as forecasted_iacv,
            swing_deal__c as is_swing_deal,
            renewal_acv__c::float as renewal_acv,
            renewal_amount__c::float as renewal_amount,
            amount::float as total_contract_value,
            amount::float as amount,
            upside_iacv__c::float as upside_iacv,
            case
                when stagename in ('8-Closed Lost', 'Closed Lost') and type = 'Renewal'
                then renewal_acv * -1
                when stagename in ('Closed Won')
                then forecasted_iacv
                else 0
            end as net_iacv,
            arr_net__c as net_arr,
            case
                when closedate::date >= '2018-02-01'
                then coalesce((net_iacv * ratio_net_iacv_to_net_arr), net_iacv)
                else null
            end as net_arr_converted,
            case
                when closedate::date <= '2021-01-31' then net_arr_converted else net_arr
            end as net_arr_final,
            arr_basis__c as arr_basis,
            arr__c as arr,
            recurring_amount__c as recurring_amount,
            true_up_amount__c as true_up_amount,
            proserv_amount__c as proserv_amount,
            other_non_recurring_amount__c as other_non_recurring_amount
        from cleaned
        inner join
            date_spine
            on cleaned.valid_from::date <= date_spine.date_actual
            and (
                cleaned.valid_to::date > date_spine.date_actual
                or cleaned.valid_to is null
            )
        left join
            net_arr_net_iacv_conversion_factors
            on cleaned.opportunity_id
            = net_arr_net_iacv_conversion_factors.opportunity_id

    )

select *
from joined
order by 1, 2
