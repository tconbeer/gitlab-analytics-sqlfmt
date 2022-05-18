{{ config(tags=["mnpi"]) }}

with
    source as (select * from {{ source("salesforce", "zqu_quote_amendment") }}),
    renamed as (

        select
            id::varchar as zqu_quote_amendment_id,

            charge_summary_sub_total__c::float as charge_summary_sub_total,
            license_amount__c::float as license_amount,
            name::varchar as zqu_quote_amendment_name,
            professional_services_amount__c::float as professional_services_amount,
            true_up_amount__c::float as true_up_amount,
            zqu__autorenew__c::varchar as zqu__auto_renew,
            zqu__cancellationpolicy__c::varchar as zqu__cancellation_policy,
            zqu__deltamrr__c::float as zqu__delta_mrr,
            zqu__deltatcv__c::float as zqu__delta_tcv,
            zqu__initialtermperiodtype__c::varchar as zqu__initial_term_period_type,
            zqu__initialterm__c::float as zqu__initial_term,
            zqu__quoteamendmentzuoraid__c::varchar as zqu__quote_amendment_zuora_id,
            zqu__renewaltermperiodtype__c::varchar as zqu__renewal_term_period_type,
            zqu__renewalterm__c::float as zqu__renewal_term,
            zqu__status__c::varchar as zqu__status,
            zqu__totalamount__c::float as zqu__total_amount,
            zqu__quote__c::varchar as zqu__quote,
            zqu__type__c::varchar as zqu__type,
            zqu__description__c::varchar as zqu__description,
            zqu__termstartdate__c::timestamp_tz as zqu__term_start_date,


            -- metadata
            createdbyid::varchar as created_by_id,
            createddate::timestamp_tz as created_date,
            isdeleted::boolean as is_deleted,
            lastmodifiedbyid::varchar as last_modified_by_id,
            lastmodifieddate::timestamp_tz as last_modified_date,
            _sdc_received_at::timestamp_tz as sdc_received_at,
            _sdc_extracted_at::timestamp_tz as sdc_extracted_at,
            _sdc_table_version::number as sdc_table_version,
            _sdc_batched_at::timestamp_tz as sdc_batched_at,
            _sdc_sequence::number as sdc_sequence,
            systemmodstamp::timestamp_tz as system_mod_stamp

        from source

    )

select *
from renamed
