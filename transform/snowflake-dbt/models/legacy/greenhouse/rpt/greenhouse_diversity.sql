{% set repeated_column_metrics = "COUNT(CASE WHEN capture_month = 'application_month' THEN application_id ELSE NULL END) AS total_candidates_applied,     SUM(CASE WHEN capture_month = 'application_month' THEN accepted_offer ELSE NULL END)    AS total_offers_based_on_application_month,     SUM(CASE WHEN capture_month='offer_sent_month' THEN 1 ELSE NULL END)                    AS total_sent_offers,     SUM(CASE WHEN capture_month='offer_sent_month' THEN accepted_offer ELSE NULL END)       AS offers_accepted_based_on_sent_month,     SUM(CASE WHEN capture_month = 'accepted_month' THEN accepted_offer ELSE NULL END)       AS offers_accepted,     SUM(CASE WHEN capture_month='accepted_month' THEN hired_sourced_candidate          ELSE NULL END)                                                                      AS offers_accepted_sourced_candidate,     AVG(CASE WHEN capture_month = 'accepted_month' THEN time_to_offer ELSE NULL END)        AS time_to_offer_average,      MEDIAN(CASE WHEN capture_month = 'accepted_month' THEN time_to_offer ELSE NULL END)     AS time_to_offer_median     " %}

{% set repeated_column_names_ratio_to_report = "(PARTITION BY month_date, breakout_type, department_name, division, eeoc_field_name ORDER BY month_date)      " %}

with
    greenhouse_diversity_intermediate as (

        select * from {{ ref("greenhouse_diversity_intermediate") }}


    ),
    breakout as (

        select
            month_date,
            'kpi_level_breakout' as breakout_type,
            null as department_name,
            null as division,
            'no_eeoc' as eeoc_field_name,
            null as eeoc_values,
            {{ repeated_column_metrics }}
        from greenhouse_diversity_intermediate
        where lower(eeoc_field_name) = 'candidate_status' {{ dbt_utils.group_by(n=6) }}

        UNION ALL

        select
            month_date,
            'all_attributes_breakout' as breakout_type,
            department_name,
            division_modified as division,
            eeoc_field_name,
            eeoc_values,
            {{ repeated_column_metrics }}
        from greenhouse_diversity_intermediate {{ dbt_utils.group_by(n=6) }}

        UNION ALL

        select
            month_date,
            'department_division_breakout' as breakout_type,
            department_name,
            division_modified as division,
            'no_eeoc' as eeoc_field_name,
            null as eeoc_values,
            {{ repeated_column_metrics }}
        from greenhouse_diversity_intermediate {{ dbt_utils.group_by(n=6) }}

        UNION ALL

        select
            month_date,
            'division_breakout' as breakout_type,
            null as department_name,
            division_modified as division,
            eeoc_field_name,
            eeoc_values,
            {{ repeated_column_metrics }}
        from greenhouse_diversity_intermediate {{ dbt_utils.group_by(n=6) }}

        UNION ALL

        select
            month_date,
            'eeoc_only_breakout' as breakout_type,
            'NA' as department_name,
            'NA' as division,
            eeoc_field_name,
            eeoc_values,
            {{ repeated_column_metrics }}
        from greenhouse_diversity_intermediate {{ dbt_utils.group_by(n=6) }}

    ),
    aggregated as (

        select
            breakout.*,
            case
                when total_candidates_applied = 0
                then null
                when total_candidates_applied is null
                then null
                else total_offers_based_on_application_month / total_candidates_applied
            end as application_to_offer_percent,
            case
                when total_sent_offers = 0
                then null
                when total_sent_offers is null
                then null
                else offers_accepted_based_on_sent_month / total_sent_offers
            end as offer_acceptance_rate_based_on_offer_month,
            case
                when offers_accepted = 0
                then null
                when offers_accepted is null
                then null
                else offers_accepted_sourced_candidate / offers_accepted
            end as percent_of_hires_sourced,
            iff(
                total_candidates_applied = 0,
                null,
                ratio_to_report(
                    total_candidates_applied
                ) over {{ repeated_column_names_ratio_to_report }}
            ) as percent_of_applicants,
            iff(
                total_sent_offers = 0,
                null,
                ratio_to_report(
                    total_sent_offers
                ) over {{ repeated_column_names_ratio_to_report }}
            ) as percent_of_offers_sent,
            iff(
                offers_accepted = 0,
                null,
                ratio_to_report(
                    offers_accepted
                ) over {{ repeated_column_names_ratio_to_report }}
            ) as percent_of_offers_accepted,
            min(
                total_candidates_applied
            ) over {{ repeated_column_names_ratio_to_report }}
            as min_applicants_breakout,
            min(total_sent_offers) over {{ repeated_column_names_ratio_to_report }}
            as min_sent_offers_for_breakout,
            min(offers_accepted) over {{ repeated_column_names_ratio_to_report }}
            as min_total_offers_accepted_for_breakout
        from breakout

    ),
    final as (

        select
            month_date,
            breakout_type,
            department_name,
            division,
            lower(eeoc_field_name) as eeoc_field_name,
            eeoc_values,
            -- --Applicant Level----
            iff(
                min_applicants_breakout < 3, null, total_candidates_applied
            ) as total_candidates_applied,
            iff(
                breakout_type in ('kpi_level_breakout', 'eeoc_only_breakout'),
                application_to_offer_percent,
                null
            ) as application_to_offer_percent,
            iff(
                min_applicants_breakout < 3, null, percent_of_applicants
            ) as percent_of_applicants,

            -- -Offers Sent Level ---
            iff(
                min_sent_offers_for_breakout < 3, null, total_sent_offers
            ) as total_sent_offers,
            iff(
                min_sent_offers_for_breakout < 3, null, percent_of_offers_sent
            ) as percent_of_offers_sent,
            min_sent_offers_for_breakout,

            -- Offers Accepted Level ---
            iff(
                min_total_offers_accepted_for_breakout < 3, null, offers_accepted
            ) as offers_accepted,
            iff(
                min_total_offers_accepted_for_breakout < 3,
                null,
                percent_of_offers_accepted
            ) as percent_of_offers_accepted,
            iff(
                offers_accepted < 3, null, time_to_offer_average
            ) as time_to_offer_average,
            iff(
                offers_accepted < 3, null, time_to_offer_median
            ) as time_to_offer_median,
            iff(
                offers_accepted < 3, null, offer_acceptance_rate_based_on_offer_month
            ) as offer_acceptance_rate_based_on_offer_month,
            percent_of_hires_sourced
        from aggregated
        where month_date <= dateadd(month, -1, current_date())

    )

select *
from final
