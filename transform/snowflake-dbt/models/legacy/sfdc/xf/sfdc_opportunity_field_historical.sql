-- fields with corresponding columns in sfdc_opportunity_xf
{% set fields_to_use = [
    'amount','closedate','forecastcategoryname','incremental_acv_2__c',
    'leadsource','renewal_acv__c','renewal_amount__c','sales_accepted_date__c',
    'sales_qualified_date__c','sales_segmentation_employees_o__c',
    'sales_segmentation_o__c','sql_source__c','stagename','swing_deal__c',
    'type','ultimate_parent_sales_segment_emp_o__c','ultimate_parent_sales_segment_o__c',
    'upside_iacv__c', 'recurring_amount__c', 'true_up_amount__c', 'proserv_amount__c',
    'other_non_recurring_amount__c', 'arr_net__c', 'arr_basis__c', 'arr__c', 'start_date__c',
    'end_date__c'
] %}

WITH date_spine AS (

    SELECT DISTINCT
      DATE_TRUNC('day', date_day) AS date_actual
    FROM {{ref("date_details")}}
    WHERE date_day >= '2019-02-01'::DATE
      AND date_day <= '2019-10-01'::DATE

), net_arr_net_iacv_conversion_factors AS (

    SELECT *
    FROM {{ref('sheetload_net_arr_net_iacv_conversion_factors_source')}}

), first_snapshot AS (

    SELECT
      id                   AS opportunity_id,
      valid_to,
      {% for field in fields_to_use %}
        {{field}}::VARCHAR AS {{field}},
      {% endfor %}
      createddate          AS created_at,
      valid_from
    FROM {{ref('sfdc_opportunity_snapshots_base')}}
    WHERE date_actual = '2019-10-01'::DATE
      AND isdeleted = FALSE

), base AS (

    SELECT
      field_history.opportunity_id,
      field_modified_at                AS valid_to,
      opportunity_field,
      COALESCE(old_value, 'true null') AS old_value --retain record of fields that transitioned from NULL to another state
    FROM {{ref('sfdc_opportunity_field_history')}} field_history
    INNER JOIN first_snapshot
      ON field_history.field_modified_at <= first_snapshot.valid_from
     AND field_history.opportunity_id = first_snapshot.opportunity_id
    WHERE opportunity_field IN ('{{ fields_to_use | join ("', '") }}')

), unioned AS (

    SELECT *
    FROM first_snapshot

    UNION

    SELECT
      *,
      NULL::TIMESTAMP_TZ AS created_at,
      NULL::TIMESTAMP_TZ AS valid_from
    FROM base
      PIVOT(MAX(old_value) FOR opportunity_field IN ('{{ fields_to_use | join ("', '") }}'))

), filled AS (

    SELECT
      opportunity_id,
      {% for field in fields_to_use %}
        FIRST_VALUE({{field}}) IGNORE NULLS
          OVER (
                 PARTITION BY opportunity_id
                 ORDER BY valid_to
                 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
               )                                             AS {{field}},
      {% endfor %}
      FIRST_VALUE(created_at) IGNORE NULLS
        OVER (PARTITION BY opportunity_id ORDER BY valid_to) AS created_date,
      COALESCE(
        LAG(valid_to) OVER (PARTITION BY opportunity_id ORDER BY valid_to),
        created_date
        )                                                    AS valid_from,
      valid_to
    FROM unioned

), cleaned AS (

    SELECT
      opportunity_id,
      {% for field in fields_to_use %}
        IFF({{field}} = 'true null', NULL, {{field}}) AS {{field}},
      {% endfor %}
      created_date,
      valid_from,
      COALESCE(
        LEAD(valid_from) OVER (PARTITION BY opportunity_id ORDER BY valid_from),
        valid_to
        )                                             AS valid_to
    FROM filled
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY opportunity_id, DATE_TRUNC('day', valid_from)
        ORDER BY valid_from DESC
      ) = 1

), joined AS (

    SELECT
      date_actual,
      valid_from,
      valid_to,
      IFF(valid_to IS NULL, TRUE, FALSE) AS is_currently_valid,
      cleaned.opportunity_id,
      closedate::DATE                    AS close_date,
      created_date::DATE                 AS created_date,
      sql_source__c                      AS generated_source,
      leadsource                         AS lead_source,
      COALESCE({{ sales_segment_cleaning('ultimate_parent_sales_segment_emp_o__c') }},
               {{ sales_segment_cleaning('ultimate_parent_sales_segment_o__c') }})
                                         AS parent_segment,
      sales_accepted_date__c::DATE       AS sales_accepted_date,
      sales_qualified_date__c::DATE      AS sales_qualified_date,
      start_date__c::DATE                AS subscription_start_date,
      end_date__c::DATE                  AS subscription_end_date,
      COALESCE({{ sales_segment_cleaning('sales_segmentation_employees_o__c') }},
               {{ sales_segment_cleaning('sales_segmentation_o__c') }}, 'Unknown')
                                         AS sales_segment,
      type                               AS sales_type,
      {{  sfdc_source_buckets('leadsource') }}
      stagename                          AS stage_name,
      {{sfdc_deal_size('incremental_acv_2__c::FLOAT', 'deal_size')}},
      forecastcategoryname               AS forecast_category_name,
      incremental_acv_2__c::FLOAT        AS forecasted_iacv,
      swing_deal__c                      AS is_swing_deal,
      renewal_acv__c::FLOAT              AS renewal_acv,
      renewal_amount__c::FLOAT           AS renewal_amount,
      sales_segmentation_o__c            AS segment,
      amount::FLOAT                      AS total_contract_value,
      amount::FLOAT                      AS amount,
      upside_iacv__c::FLOAT              AS upside_iacv,
      CASE
        WHEN stagename IN ('8-Closed Lost', 'Closed Lost') AND type = 'Renewal' THEN renewal_acv * -1
        WHEN stagename IN ('Closed Won')                                        THEN forecasted_iacv
        ELSE 0
      END							     AS net_iacv,
      arr_net__c                         AS net_arr,
      CASE
        WHEN closedate::DATE >= '2018-02-01' THEN COALESCE((net_iacv * ratio_net_iacv_to_net_arr), net_iacv)
        ELSE 99999999999999
      END                                AS net_arr_converted,
      arr_basis__c                       AS arr_basis,
      arr__c                             AS arr,
      recurring_amount__c                AS recurring_amount,
      true_up_amount__c                  AS true_up_amount,
      proserv_amount__c                  AS proserv_amount,
      other_non_recurring_amount__c      AS other_non_recurring_amount
    FROM cleaned
    INNER JOIN date_spine
      ON cleaned.valid_from::DATE <= date_spine.date_actual
      AND (cleaned.valid_to::DATE > date_spine.date_actual OR cleaned.valid_to IS NULL)
    LEFT JOIN net_arr_net_iacv_conversion_factors
      ON cleaned.opportunity_id = net_arr_net_iacv_conversion_factors.opportunity_id

)

SELECT *
FROM joined
ORDER BY 1,2
