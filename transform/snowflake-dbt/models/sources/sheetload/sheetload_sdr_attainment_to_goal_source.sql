with
    source as (select * from {{ source("sheetload", "sdr_attainment_to_goal") }}),
    renamed as (

        select
            current_month::date as current_month,
            name::varchar as name,
            sdr_sfdc_name::varchar as sdr_sfdc_name,
            role::varchar as role,
            status::varchar as status,
            region::varchar as region,
            segment::varchar as segment,
            type::varchar as type,
            total_leads_accepted::number as total_leads_accepted,
            accepted_leads::number as accepted_leads,
            accepted_leads_qualifying::number as accepted_leads_qualifying,
            accepted_leads_completed::number as accepted_leads_completed,
            leads_worked::number as leads_worked,
            qualified_leads::number as qualified_leads,
            unqualified_leads::number as unqualified_leads,
            accepted_leads_inbound::number as accepted_leads_inbound,
            accepted_leads_outbound::number as accepted_leads_outbound,
            inbound_leads_worked::number as inbound_leads_worked,
            outbound_leads_worked::number as outbound_leads_worked,
            inbound_leads_accepted::number as inbound_leads_accepted,
            outbound_leads_accepted::number as outbound_leads_accepted,
            inbound_leads_qualifying::number as inbound_leads_qualifying,
            outbound_leads_qualifying::number as outbound_leads_qualifying,
            iqm::number as iqm,
            average_working_day_calls::number as average_working_day_calls,
            average_working_day_emails::number as average_working_day_emails,
            average_working_day_other::number as average_working_day_other,
            saos::number as saos,
            quarterly_sao_target::number as quarterly_sao_target,
            quarterly_sao_variance::number as quarterly_sao_variance,
            average_time::number as average_time,
            end_of_month::date as end_of_month
        from source
    )

select *
from renamed
