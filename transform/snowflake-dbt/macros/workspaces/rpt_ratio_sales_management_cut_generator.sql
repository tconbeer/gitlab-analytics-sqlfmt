{% macro rpt_ratio_sales_management_cut_generator(
    select_columns, is_new_logo_calc, extra_where_clause="TRUE"
) %}

-- Metrics to compute
{%- set metrics = [
    "MQLs / A",
    "MQLs / TQ",
    "MQLs / QTD",
    "Trials / A",
    "Trials / TQ",
    "Trials / QTD",
    "First Order ARR / A",
    "First Order ARR / TQ",
    "First Order ARR / QTD",
    "New Logos / A",
    "New Logos / TQ",
    "New Logos / QTD",
    "SAOs / A",
    "SAOs / TQ",
    "SAOs / QTD",
    "Won Opps / A",
    "Won Opps / TQ",
    "Won Opps / QTD",
    "Total Opps / A",
    "Total Opps / TQ",
    "Total Opps / QTD",
] -%}

-- Regions inside Segment Region Grouped that will be part of Large Subtotal
{% set large_segment_region_grouped = [
    "APAC",
    "EMEA",
    "Large Other",
    "PubSec",
    "US East",
    "US West",
    "Pubsec",
    "Global",
    "Large MQLs & Trials",
] %}

-- For the rpt_models align the column names and how Missing values are encoded with
-- the null_or_missing macro.
-- Also, add the variable extra_where_clause to filter the base data
{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            (
                "rpt_crm_opportunity_closed_period",
                "rpt_crm_opportunity_closed_period",
            ),
            (
                "rpt_crm_opportunity_accepted_period",
                "rpt_crm_opportunity_accepted_period",
            ),
            ("rpt_crm_person_mql", "rpt_crm_person_mql"),
            ("rpt_sales_funnel_target", "rpt_sales_funnel_target"),
            ("rpt_sales_funnel_target_daily", "rpt_sales_funnel_target_daily"),
        ]
    )
}},
crm_person as (

    select
        {{
            dbt_utils.star(
                from=ref("rpt_crm_person_mql"), except=["SALES_SEGMENT_GROUPED"]
            )
        }},
        {{ null_or_missing("sales_segment_name", "sales_segment") }},
        {{ null_or_missing("sales_segment_region_mapped", "segment_region_grouped") }},
        {{ null_or_missing("sales_qualified_source_name", "sales_qualified_source") }},
        {{ null_or_missing("sales_segment_grouped", "sales_segment_grouped") }}
    from rpt_crm_person_mql
    where
        order_type_grouped != '3) Consumption / PS / Other'
        and order_type_grouped not like '%Missing%'
        and {{ extra_where_clause }}

),
crm_opportunity_closed_period as (

    select
        rpt_crm_opportunity_closed_period.*,
        {{ null_or_missing("crm_opp_owner_sales_segment_stamped", "sales_segment") }},
        {{
            null_or_missing(
                "crm_opp_owner_sales_segment_region_stamped_grouped",
                "segment_region_grouped",
            )
        }},
        {{ null_or_missing("sales_qualified_source_name", "sales_qualified_source") }},
        {{
            null_or_missing(
                "crm_opp_owner_sales_segment_stamped_grouped", "sales_segment_grouped"
            )
        }}
    from rpt_crm_opportunity_closed_period
    where
        order_type_grouped != '3) Consumption / PS / Other'
        and order_type_grouped not like '%Missing%'
        and {{ extra_where_clause }}

),
crm_opportunity_accepted_period as (

    select
        rpt_crm_opportunity_accepted_period.*,
        {{ null_or_missing("crm_user_sales_segment", "sales_segment") }},
        {{
            null_or_missing(
                "crm_user_sales_segment_region_grouped", "segment_region_grouped"
            )
        }},
        {{ null_or_missing("sales_qualified_source_name", "sales_qualified_source") }},
        {{ null_or_missing("crm_user_sales_segment_grouped", "sales_segment_grouped") }}
    from rpt_crm_opportunity_accepted_period
    where
        order_type_grouped != '3) Consumption / PS / Other'
        and order_type_grouped not like '%Missing%'
        and {{ extra_where_clause }}

),
sales_funnel_target as (

    select
        {{
            dbt_utils.star(
                from=ref("rpt_sales_funnel_target"), except=["ORDER_TYPE_GROUPED"]
            )
        }},
        {{ null_or_missing("order_type_grouped", "order_type_grouped") }},
        {{ null_or_missing("crm_user_sales_segment", "sales_segment") }},
        {{
            null_or_missing(
                "crm_user_sales_segment_region_grouped", "segment_region_grouped"
            )
        }},
        {{ null_or_missing("sales_qualified_source_name", "sales_qualified_source") }},
        {{ null_or_missing("crm_user_sales_segment_grouped", "sales_segment_grouped") }}
    from rpt_sales_funnel_target
    where
        order_type_grouped != '3) Consumption / PS / Other'
        and order_type_grouped not like '%Missing%'
        and {{ extra_where_clause }}

),
sales_funnel_target_daily as (

    select
        {{
            dbt_utils.star(
                from=ref("rpt_sales_funnel_target_daily"),
                except=["ORDER_TYPE_GROUPED"],
            )
        }},
        {{ null_or_missing("order_type_grouped", "order_type_grouped") }},
        {{ null_or_missing("crm_user_sales_segment", "sales_segment") }},
        {{
            null_or_missing(
                "crm_user_sales_segment_region_grouped", "segment_region_grouped"
            )
        }},
        {{ null_or_missing("sales_qualified_source_name", "sales_qualified_source") }},
        {{ null_or_missing("crm_user_sales_segment_grouped", "sales_segment_grouped") }}
    from rpt_sales_funnel_target_daily
    where
        order_type_grouped != '3) Consumption / PS / Other'
        and order_type_grouped not like '%Missing%'
        and {{ extra_where_clause }}

),
current_fiscal_quarter as (

    select distinct fiscal_quarter_name_fy as current_fiscal_quarter
    from dim_date
    where date_actual = current_date

),
factor_to_date as (

    select distinct
        'date_range_quarter' as _type,
        fiscal_quarter_name_fy::varchar as _date,
        iff(
            fiscal_quarter_name_fy < current_fiscal_quarter.current_fiscal_quarter,
            true,
            false
        ) as is_selected_quarter_lower_than_current_quarter,
        last_day_of_fiscal_quarter
    from dim_date
    left join current_fiscal_quarter
    -- WHERE [fiscal_quarter_name_fy=bc_fiscal_quarter]
    where
        fiscal_year between extract(year from current_date) - 1 and extract(
            year from current_date
        )
        + 1

-- Union all the data sources columns to create a base list that can be used to join
-- all the metrics too
),
prep_base_list as (

    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        {{ select_column }} {% if not loop.last %},{% endif %}
        {% endfor %}
    from crm_opportunity_closed_period

    union

    -- SAOs
    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        {{ select_column }} {% if not loop.last %},{% endif %}
        {% endfor %}
    from crm_opportunity_accepted_period

    union

    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        {{ select_column }} {% if not loop.last %},{% endif %}
        {% endfor %}
    from crm_person

    union

    -- Targets
    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        {{ select_column }} {% if not loop.last %},{% endif %}
        {% endfor %}
    from sales_funnel_target

    union

    -- Targets MQL
    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        {{ select_column }} {% if not loop.last %},{% endif %}
        {% endfor %}
    from sales_funnel_target
    where kpi_name in ('MQL', 'Trials')

-- To the above base list add support for subtotals and a total column that will be
-- joined later
),
base_list as (

    select *
    from prep_base_list

    {% if select_columns | count > 1 %}

    {% for __ in select_columns %}

    union

    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        {% if loop.first %} {{ select_column }}
        {% else %} 'Total'
        {% endif %}
        {% if not loop.last %},{% endif %}

        {% endfor %}
    from prep_base_list

    {% endfor %}

    {% endif %}

    union

    select
        fiscal_quarter_name_fy,
        {% for __ in select_columns %}
        'Total' {% if not loop.last %},{% endif %}
        {% endfor %}
    from prep_base_list

-- Calculate the metrics in each CTE. Uses Group by ROLLUP to also calculate subtotal
-- and total columns
-- The IFNULL in the select_column is used because the Subtotal and Total columns
-- calculated by the rollup come back as NULL
),
new_logos_actual as (

    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        ifnull({{ select_column }}, 'Total') as {{ select_column }},
        {% endfor %}
        count(distinct dim_crm_opportunity_id) as "New Logos / A"
    from crm_opportunity_closed_period
    where
        is_won = 'TRUE'
        and is_closed = 'TRUE'
        and is_edu_oss = 0
        -- AND IFF([new_logos] = FALSE, TRUE, order_type = '1. New - First Order')
        and iff(
            {{ is_new_logo_calc }} = false, true, order_type = '1. New - First Order'
        )
    group by
        rollup (
            1,
            {% for select_column in select_columns %}
            {{ select_column }} {% if not loop.last %},{% endif %}
            {% endfor %}
        )

),
sao_count as (

    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        ifnull({{ select_column }}, 'Total') as {{ select_column }},
        {% endfor %}
        count(*) as "SAOs / A"
    from crm_opportunity_accepted_period
    where is_sao = true
    group by
        rollup (
            1,
            {% for select_column in select_columns %}
            {{ select_column }} {% if not loop.last %},{% endif %}
            {% endfor %}
        )

),
first_oder_arr_closed_won as (

    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        ifnull({{ select_column }}, 'Total') as {{ select_column }},
        {% endfor %}
        sum(net_arr) as "First Order ARR / A"
    from crm_opportunity_closed_period
    where
        is_won = 'TRUE'
        and is_closed = 'TRUE'
        and is_edu_oss = 0
        -- AND order_type = '1. New - First Order'
        and iff(
            {{ is_new_logo_calc }} = false, true, order_type = '1. New - First Order'
        )
    group by
        rollup (
            1,
            {% for select_column in select_columns %}
            {{ select_column }} {% if not loop.last %},{% endif %}
            {% endfor %}
        )

),
win_rate as (

    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        ifnull({{ select_column }}, 'Total') as {{ select_column }},
        {% endfor %}
        count(case when is_won then 1 end) as "Won Opps / A",
        count(is_won) as "Total Opps / A"
    from crm_opportunity_closed_period
    where is_win_rate_calc = true
    group by
        rollup (
            1,
            {% for select_column in select_columns %}
            {{ select_column }} {% if not loop.last %},{% endif %}
            {% endfor %}
        )

),
mql_trial_count as (

    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        ifnull({{ select_column }}, 'Total') as {{ select_column }},
        {% endfor %}
        count(distinct email_hash) as "MQLs / A",
        count(
            distinct iff(is_lead_source_trial, email_hash || lead_source, null)
        ) as "Trials / A"
    from crm_person
    group by
        rollup (
            1,
            {% for select_column in select_columns %}
            {{ select_column }} {% if not loop.last %},{% endif %}
            {% endfor %}
        )

),
targets as (

    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        ifnull({{ select_column }}, 'Total') as {{ select_column }},
        {% endfor %}
        sum(iff(kpi_name = 'Trials', qtd_allocated_target, 0)) as "Trials / QTD",
        sum(
            iff(
                iff(
                    {{ is_new_logo_calc }} = false,
                    true,
                    order_type_name = '1. New - First Order'
                )
                and kpi_name = 'Deals',
                qtd_allocated_target,
                0
            )
        ) as "New Logos / QTD",
        sum(
            iff(
                iff(
                    {{ is_new_logo_calc }} = false,
                    true,
                    order_type_name = '1. New - First Order'
                )
                and kpi_name = 'Net ARR',
                qtd_allocated_target,
                0
            )
        ) as "First Order ARR / QTD",
        sum(
            iff(kpi_name = 'Stage 1 Opportunities', qtd_allocated_target, 0)
        ) as "SAOs / QTD",
        sum(iff(kpi_name = 'MQL', qtd_allocated_target, 0)) as "MQLs / QTD",
        sum(iff(kpi_name = 'Deals', qtd_allocated_target, 0)) as "Won Opps / QTD",
        sum(
            iff(kpi_name = 'Total Closed', qtd_allocated_target, 0)
        ) as "Total Opps / QTD"

    from sales_funnel_target_daily
    left join factor_to_date
    where
        iff(
            factor_to_date.is_selected_quarter_lower_than_current_quarter,
            target_date = factor_to_date.last_day_of_fiscal_quarter,
            report_target_date = current_date
        )
        -- AND [fiscal_quarter_name_fy=bc_fiscal_quarter]
        and fiscal_quarter_name_fy = 'FY22-Q1'
    group by
        rollup (
            1,
            {% for select_column in select_columns %}
            {{ select_column }} {% if not loop.last %},{% endif %}
            {% endfor %}
        )

),
targets_full as (

    select
        fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        ifnull({{ select_column }}, 'Total') as {{ select_column }},
        {% endfor %}
        sum(iff(kpi_name = 'Trials', allocated_target, 0)) as "Trials / TQ",
        sum(
            iff(
                iff(
                    {{ is_new_logo_calc }} = false,
                    true,
                    order_type_name = '1. New - First Order'
                )
                and kpi_name = 'Deals',
                allocated_target,
                0
            )
        ) as "New Logos / TQ",
        sum(
            iff(
                iff(
                    {{ is_new_logo_calc }} = false,
                    true,
                    order_type_name = '1. New - First Order'
                )
                and kpi_name = 'Net ARR',
                allocated_target,
                0
            )
        ) as "First Order ARR / TQ",
        sum(
            iff(kpi_name = 'Stage 1 Opportunities', allocated_target, 0)
        ) as "SAOs / TQ",
        sum(iff(kpi_name = 'MQL', allocated_target, 0)) as "MQLs / TQ",
        sum(iff(kpi_name = 'Deals', allocated_target, 0)) as "Won Opps / TQ",
        sum(iff(kpi_name = 'Total Closed', allocated_target, 0)) as "Total Opps / TQ"

    from sales_funnel_target
    group by
        rollup (
            1,
            {% for select_column in select_columns %}
            {{ select_column }} {% if not loop.last %},{% endif %}
            {% endfor %}
        )

),
agg as (

    select
        base_list.fiscal_quarter_name_fy,
        {% for select_column in select_columns %}
        base_list.{{ select_column }},
        {% endfor %}

        {% for metric in metrics %}
        nullif("{{metric}}", 0) as "{{metric}}" {% if not loop.last %},{% endif %}
        {% endfor %}

    from base_list

    inner join
        factor_to_date
        on base_list.fiscal_quarter_name_fy::varchar = factor_to_date._date::varchar

    left join
        new_logos_actual
        on base_list.fiscal_quarter_name_fy = new_logos_actual.fiscal_quarter_name_fy
        {% for select_column in select_columns %}
        and base_list.{{ select_column }} = new_logos_actual.{{ select_column }}
        {% endfor %}

    left join
        first_oder_arr_closed_won
        on base_list.fiscal_quarter_name_fy
        = first_oder_arr_closed_won.fiscal_quarter_name_fy
        {% for select_column in select_columns %}
        and base_list.{{ select_column }}
        = first_oder_arr_closed_won.{{ select_column }}
        {% endfor %}

    left join
        mql_trial_count
        on base_list.fiscal_quarter_name_fy = mql_trial_count.fiscal_quarter_name_fy
        {% for select_column in select_columns %}
        and base_list.{{ select_column }} = mql_trial_count.{{ select_column }}
        {% endfor %}

    left join
        sao_count on base_list.fiscal_quarter_name_fy = sao_count.fiscal_quarter_name_fy
        {% for select_column in select_columns %}
        and base_list.{{ select_column }} = sao_count.{{ select_column }}
        {% endfor %}

    left join
        win_rate on base_list.fiscal_quarter_name_fy = win_rate.fiscal_quarter_name_fy
        {% for select_column in select_columns %}
        and base_list.{{ select_column }} = win_rate.{{ select_column }}
        {% endfor %}

    left join
        targets on base_list.fiscal_quarter_name_fy = targets.fiscal_quarter_name_fy
        {% for select_column in select_columns %}
        and base_list.{{ select_column }} = targets.{{ select_column }}
        {% endfor %}
    left join
        targets_full
        on base_list.fiscal_quarter_name_fy = targets_full.fiscal_quarter_name_fy
        {% for select_column in select_columns %}
        and base_list.{{ select_column }} = targets_full.{{ select_column }}
        {% endfor %}

    where
        not (
            "First Order ARR / A" is null
            and "First Order ARR / TQ" is null
            and "New Logos / A" is null
            and "New Logos / TQ" is null
            and "MQLs / A" is null
            and "MQLs / TQ" is null
            and "SAOs / A" is null
            and "SAOs / TQ" is null
            and "Won Opps / A" is null
            and "Won Opps / TQ" is null
            and "Total Opps / A" is null
            and "Total Opps / TQ" is null
            and "Trials / A" is null
            and "Trials / TQ" is null
        )

)

-- The following code handles the large subtotal calculation
-- This is only calculated if the segment_region_grouped column is selected
-- In case the segment_region_grouped is the first column in the cut and there are
-- other columns in the cut.
-- Additionally to Large-Total, a Large-Total | Total column (a subtotal for
-- Large-Total) is calculated using the GROUP BY ROLLUP function.
{% for select_column in select_columns %}
{% if select_column == "segment_region_grouped" %}

,
large_subtotal_no_mql_trial_new_logo as (

    select
        fiscal_quarter_name_fy,
        {% for column in select_columns %}
        {% if column == "segment_region_grouped" %} 'Large-Total' as {{ column }},
        {% else %} ifnull({{ column }}, 'Total') as {{ column }},
        {% endif %}
        {% endfor %}
        {% for metric in metrics %}
        sum("{{metric}}") as "{{metric}}" {% if not loop.last %},{% endif %}
        {% endfor %}
    from agg
    where
        segment_region_grouped in (
            {% for large_region in large_segment_region_grouped %}
            '{{large_region}}' {% if not loop.last %}, {% endif %}
            {% endfor %}
        )
        {% for select_column in select_columns %}
        and agg.{{ select_column }} != 'Total'
        {% endfor %}

    group by
        rollup (
            1,
            {% for column in select_columns %}
            {% if column == "segment_region_grouped" %} 'Large-Total'
            {% else %} {{ column }}
            {% endif %}
            {% if not loop.last %},{% endif %}
            {% endfor %}
        )

),
large_subtotal_new_logo as (

    select
        fiscal_quarter_name_fy,
        {% for column in select_columns %}
        {% if column == "segment_region_grouped" %} 'Large-Total' as {{ column }},
        {% else %} ifnull({{ column }}, 'Total') as {{ column }},
        {% endif %}
        {% endfor %}
        count(distinct dim_crm_opportunity_id) as "New Logos / A"
    from crm_opportunity_closed_period
    where
        is_won = 'TRUE'
        and is_closed = 'TRUE'
        and is_edu_oss = 0
        -- AND IFF([new_logos] = FALSE, TRUE, order_type = '1. New - First Order')
        and iff(
            {{ is_new_logo_calc }} = false, true, order_type = '1. New - First Order'
        )
        and segment_region_grouped in (
            {% for large_region in large_segment_region_grouped %}
            '{{large_region}}' {% if not loop.last %}, {% endif %}
            {% endfor %}
        )

    group by
        rollup (
            1,
            {% for column in select_columns %}
            {% if column == "segment_region_grouped" %} 'Large-Total'
            {% else %} {{ column }}
            {% endif %}
            {% if not loop.last %},{% endif %}
            {% endfor %}
        )

),
large_subtotal_mql_trial as (

    select
        fiscal_quarter_name_fy,
        {% for column in select_columns %}
        {% if column == "segment_region_grouped" %} 'Large-Total' as {{ column }},
        {% else %} ifnull({{ column }}, 'Total') as {{ column }},
        {% endif %}
        {% endfor %}
        count(distinct email_hash) as "MQLs / A",
        count(
            distinct iff(is_lead_source_trial, email_hash || lead_source, null)
        ) as "Trials / A"
    from crm_person
    where
        segment_region_grouped in (
            {% for large_region in large_segment_region_grouped %}
            '{{large_region}}' {% if not loop.last %}, {% endif %}
            {% endfor %}
        )
    group by
        rollup (
            1,
            {% for column in select_columns %}
            {% if column == "segment_region_grouped" %} 'Large-Total'
            {% else %} {{ column }}
            {% endif %}
            {% if not loop.last %},{% endif %}
            {% endfor %}
        )

),
large_subtotal as (

    select
        large_subtotal_no_mql_trial_new_logo.fiscal_quarter_name_fy,
        {% for column in select_columns %}
        large_subtotal_no_mql_trial_new_logo.{{ column }},
        {% endfor %}
        {% for metric in metrics %}
        {% if metric == "MQLs / A" %}
        large_subtotal_mql_trial."{{metric}}" as "{{metric}}"
        {% elif metric == "Trials / A" %}
        large_subtotal_mql_trial."{{metric}}" as "{{metric}}"
        {% elif metric == "New Logos / A" %}
        large_subtotal_new_logo."{{metric}}" as "{{metric}}"
        {% else %} large_subtotal_no_mql_trial_new_logo."{{metric}}"
        {% endif %}
        {% if not loop.last %},{% endif %}
        {% endfor %}
    from large_subtotal_no_mql_trial_new_logo
    left join
        large_subtotal_mql_trial
        on large_subtotal_mql_trial.fiscal_quarter_name_fy
        = large_subtotal_no_mql_trial_new_logo.fiscal_quarter_name_fy
        {% for select_column in select_columns %}
        {% if select_column == "segment_region_grouped" %}
        and iff(
            large_subtotal_mql_trial.segment_region_grouped like 'Large%',
            'Large-Total',
            null
        )
        = large_subtotal_no_mql_trial_new_logo.segment_region_grouped
        {% else %}
        and large_subtotal_mql_trial.{{ select_column }}
        = large_subtotal_no_mql_trial_new_logo.{{ select_column }}
        {% endif %}
        {% endfor %}
        and large_subtotal_mql_trial.segment_region_grouped like 'Large%'
    left join
        large_subtotal_new_logo
        on large_subtotal_new_logo.fiscal_quarter_name_fy
        = large_subtotal_no_mql_trial_new_logo.fiscal_quarter_name_fy
        {% for select_column in select_columns %}
        and large_subtotal_new_logo.{{ select_column }}
        = large_subtotal_no_mql_trial_new_logo.{{ select_column }}
        {% endfor %}

)
{% endif %}
{% endfor %},
final as (

    select *
    from agg

    {% for select_column in select_columns %}
    {% if select_column == "segment_region_grouped" %}

    union

    select *
    from large_subtotal

    {% endif %}
    {% endfor %}

)

select
    fiscal_quarter_name_fy,
    {% for select_column in select_columns %} {{ select_column }}, {% endfor %}
    "MQLs / A",
    "MQLs / QTD",
    "MQLs / A" / "MQLs / QTD" as "MQLs / %QTD",
    "MQLs / TQ",
    "MQLs / A" / "MQLs / TQ" as "MQLs / %TQ",

    "Trials / A",
    "Trials / QTD",
    "Trials / A" / "Trials / QTD" as "Trials / %QTD",
    "Trials / TQ",
    "Trials / A" / "Trials / TQ" as "Trials / %TQ",

    "First Order ARR / A",
    "New Logos / A",

    "First Order ARR / A" / "New Logos / A" as "ASP / A",
    "First Order ARR / TQ" / "New Logos / TQ" as "ASP / TQ",
    "ASP / A" / "ASP / TQ" as "ASP / %TQ",

    "SAOs / A" / "MQLs / A" as "MQLs to SAOs / A",
    "SAOs / TQ" / "MQLs / TQ" as "MQLs to SAOs / TQ",
    "MQLs to SAOs / A" / "MQLs to SAOs / TQ" as "MQLs to SAOs / %TQ",

    "Won Opps / A" / "Total Opps / A" as "Win Rate / A",
    "Won Opps / TQ" / "Total Opps / TQ" as "Win Rate / TQ",
    "Win Rate / A" / "Win Rate / TQ" as "Win Rate / %TQ"

from final
where
    fiscal_quarter_name_fy is not null
    {% for select_column in select_columns %}
    and {{ select_column }} is not null
    {% endfor %}

{%- endmacro -%}
