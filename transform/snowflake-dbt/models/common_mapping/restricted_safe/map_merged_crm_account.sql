WITH RECURSIVE sfdc_account_source AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}

), recursive_cte(account_id, master_record_id, is_deleted, lineage) AS (

    SELECT
      account_id,
      master_record_id,
      is_deleted,
      TO_ARRAY(account_id) AS lineage
    FROM sfdc_account_source
    WHERE master_record_id IS NULL

    UNION ALL

    SELECT
      iter.account_id,
      iter.master_record_id,
      iter.is_deleted,
      ARRAY_INSERT(anchor.lineage, 0, iter.account_id)  AS lineage
    FROM recursive_cte AS anchor
    INNER JOIN sfdc_account_source AS iter
      ON iter.master_record_id = anchor.account_id

), final AS (

    SELECT
      account_id                                         AS sfdc_account_id,
      lineage[ARRAY_SIZE(lineage) - 1]::VARCHAR          AS merged_account_id,
      is_deleted,
      IFF(merged_account_id != account_id, TRUE, FALSE)  AS is_merged,
      IFF(is_deleted AND NOT is_merged, TRUE, FALSE)     AS deleted_not_merged,
      --return final common dimension mapping,
      IFF(deleted_not_merged, '-1', merged_account_id)   AS dim_crm_account_id
    FROM recursive_cte

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-11-23",
    updated_date="2020-11-23",
) }}
