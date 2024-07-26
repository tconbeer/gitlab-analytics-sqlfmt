with
    source as (select * from {{ source("sheetload", "linkedin_recruiter") }}),
    renamed as (

        select
            seat_holder::varchar as sourcer,
            zeroifnull(nullif("MESSAGES_SENT", ''))::number as messages_sent,
            zeroifnull(nullif("RESPONSES_RECEIVED", ''))::number as responses_received,
            zeroifnull(nullif("ACCEPTANCES", ''))::number as acceptances,
            zeroifnull(nullif("DECLINES", ''))::number as declines,
            zeroifnull(nullif("NO_RESPONSE", ''))::number as no_response,
            zeroifnull(nullif("RESPONSES_RATE", ''))::number as responses_rate,
            zeroifnull(nullif("ACCEPT_RATE", ''))::number as accept_rate,
            month::date as data_downloaded_month
        from source

    )

select *
from renamed
where data_downloaded_month is not null and sourcer is not null
