WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_fill_out_linkedin_lead_gen_form') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      lead_gen_campaign_name::TEXT              AS lead_gen_campaign_name,
      lead_gen_creative_id::NUMBER              AS lead_gen_creative_id,
      lead_gen_account_name::TEXT               AS lead_gen_account_name

    FROM source

)

SELECT *
FROM renamed
