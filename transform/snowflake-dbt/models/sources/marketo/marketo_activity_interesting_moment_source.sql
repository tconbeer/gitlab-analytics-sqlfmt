WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_interesting_moment') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      source::TEXT                              AS source,
      description::TEXT                         AS description,
      date::TIMESTAMP_NTZ                       AS date

    FROM source

)

SELECT *
FROM renamed
