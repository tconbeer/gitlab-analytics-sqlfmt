{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}


{{ simple_cte([
    ('licenses', 'customers_db_licenses_source'),
    ('zuora_subscriptions', 'zuora_subscription'),
    ('zuora_accounts', 'zuora_account'),
    ('version_releases', 'version_releases')
]) }}

, usage_data AS (

    SELECT {{ dbt_utils.star(from=ref('version_usage_data'), except=["LICENSE_STARTS_AT", "LICENSE_EXPIRES_AT"]) }}
    FROM {{ ref('version_usage_data') }}
  
), joined AS (

    SELECT
      usage_data.*,
      licenses.license_id,
      licenses.zuora_subscription_id,
      licenses.company,
      licenses.plan_code                                                  AS license_plan_code,
      licenses.license_start_date                                         AS license_starts_at,
      licenses.license_expire_date                                        AS license_expires_at,
      zuora_subscriptions.subscription_status                             AS zuora_subscription_status,
      zuora_accounts.crm_id                                               AS zuora_crm_id,
      DATEDIFF('days', ping_version.release_date, usage_data.created_at)  AS days_after_version_release_date,
      latest_version.major_minor_version                                  AS latest_version_available_at_ping_creation,
      latest_version.version_row_number - ping_version.version_row_number AS versions_behind_latest

    FROM usage_data
      LEFT JOIN licenses
        ON usage_data.license_md5 = licenses.license_md5
      LEFT JOIN zuora_subscriptions
        ON licenses.zuora_subscription_id = zuora_subscriptions.subscription_id
      LEFT JOIN zuora_accounts
        ON zuora_subscriptions.account_id = zuora_accounts.account_id
      LEFT JOIN version_releases AS ping_version -- Join on the version of the ping itself.
        ON usage_data.major_minor_version = ping_version.major_minor_version
      LEFT JOIN version_releases AS latest_version -- Join the latest version released at the time of the ping.
        ON usage_data.created_at BETWEEN latest_version.release_date AND {{ coalesce_to_infinity('latest_version.next_version_release_date') }}
    WHERE
      (
        licenses.email IS NULL
        OR NOT (email LIKE '%@gitlab.com' AND LOWER(company) LIKE '%gitlab%') -- Exclude internal tests licenses.
        OR uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
      )

), renamed AS (

    SELECT
      joined.*,
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' THEN 'SaaS'
        ELSE 'Self-Managed'
      END                                                                  AS ping_source,
      CASE WHEN LOWER(edition) LIKE '%ee%' THEN 'EE'
        ELSE 'CE' END                                                      AS main_edition,
      CASE 
          WHEN edition LIKE '%CE%' THEN 'Core'
          WHEN edition LIKE '%EES%' THEN 'Starter'
          WHEN edition LIKE '%EEP%' THEN 'Premium'
          WHEN edition LIKE '%EEU%' THEN 'Ultimate'
          WHEN edition LIKE '%EE Free%' THEN 'Core'
          WHEN edition LIKE '%EE%' THEN 'Starter'
        ELSE NULL END                                                      AS edition_type,
      usage_activity_by_stage_monthly['manage']['events']                  AS monthly_active_users_last_28_days

    FROM joined

)

SELECT *
FROM renamed
