{% set repeated_column_names = "greenhouse_recruiting_xf.application_id,       department_name::VARCHAR(100)                                                 AS department_name,       division::VARCHAR(100)                                                        AS division,       division_modified::VARCHAR(100)                                               AS division_modified,       source_type::VARCHAR(100)                                                     AS source_type,       CASE WHEN eeoc_values in ('I don''t wish to answer','Decline To Self Identify')              THEN 'did not identify'            WHEN eeoc_values = 'No, I don''t have a disability'              THEN 'No'              ELSE COALESCE(lower(eeoc_values), 'did not identify') end                AS eeoc_values " %}

with
    date_details as (

        select date_actual as month_date, 'join' as join_field
        from {{ ref("date_details") }}
        where
            date_actual <= {{ max_date_in_bamboo_analyses() }}
            and day_of_month = 1
            and date_actual >= '2018-08-12'  -- 1st date we started capturing eeoc data

    ),
    greenhouse_recruiting_xf as (select * from {{ ref("greenhouse_recruiting_xf") }}),
    eeoc as (

        {{
            dbt_utils.unpivot(
                relation=ref("greenhouse_eeoc_responses_source"),
                cast_to="varchar",
                exclude=["application_id"],
                remove=["eeoc_response_submitted_at"],
                field_name="eeoc_field_name",
                value_name="eeoc_values",
            )
        }}

    ),
    eeoc_fields as (

        select distinct
            lower(eeoc_field_name)::varchar(100) as eeoc_field_name,
            'join' as join_field
        from eeoc

    ),
    base as (

        select month_date, eeoc_field_name
        from date_details
        left join eeoc_fields on eeoc_fields.join_field = date_details.join_field

        union all

        select month_date, 'no_eeoc' as eeoc_field_name
        from date_details

    ),
    applications as (

        select
            base.*,
            'application_month' as capture_month,
            {{ repeated_column_names }},
            iff(offer_status = 'accepted', 1, 0) as accepted_offer,
            null as time_to_offer,
            iff(sourced_candidate = true, 1, 0) as sourced_candidate,
            iff(
                sourced_candidate = true and offer_status = 'accepted', 1, 0
            ) as hired_sourced_candidate
        from base
        left join
            greenhouse_recruiting_xf
            on date_trunc('month', greenhouse_recruiting_xf.application_date)
            = base.month_date
        left join
            eeoc
            on greenhouse_recruiting_xf.application_id = eeoc.application_id
            and lower(eeoc.eeoc_field_name) = base.eeoc_field_name

    ),
    offers as (

        select
            base.*,
            'offer_sent_month' as capture_month,
            {{ repeated_column_names }},
            iff(offer_status = 'accepted', 1, 0) as accepted_offer,
            null as time_to_offer,
            iff(sourced_candidate = true, 1, 0) as sourced_candidate,
            iff(
                sourced_candidate = true and offer_status = 'accepted', 1, 0
            ) as hired_sourced_candidate
        from base
        left join
            greenhouse_recruiting_xf
            on date_trunc('month', greenhouse_recruiting_xf.offer_sent_date)
            = base.month_date
        left join
            eeoc
            on greenhouse_recruiting_xf.application_id = eeoc.application_id
            and lower(eeoc.eeoc_field_name) = base.eeoc_field_name
        where offer_status is not null

    ),
    accepted as (

        select
            base.*,
            'accepted_month' as capture_month,
            {{ repeated_column_names }},
            iff(offer_status = 'accepted', 1, 0) as accepted_offer,
            time_to_offer,
            iff(sourced_candidate = true, 1, 0) as sourced_candidate,
            iff(
                sourced_candidate = true and offer_status = 'accepted', 1, 0
            ) as hired_sourced_candidate
        from base
        left join
            greenhouse_recruiting_xf
            on date_trunc('month', greenhouse_recruiting_xf.offer_resolved_date)
            = base.month_date
        left join
            eeoc
            on greenhouse_recruiting_xf.application_id = eeoc.application_id
            and lower(eeoc.eeoc_field_name) = base.eeoc_field_name
        -- 1st date we started capturing eeoc data
        where base.month_date >= '2018-09-01' and offer_status = 'accepted'

    ),
    final as (

        select *
        from applications

        union all

        select *
        from offers

        union all

        select *
        from accepted

    )

select *
from final
