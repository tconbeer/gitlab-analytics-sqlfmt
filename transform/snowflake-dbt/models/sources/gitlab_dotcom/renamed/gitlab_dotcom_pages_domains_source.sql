    
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_pages_domains_dedupe_source') }}
    
), renamed AS (
  
    SELECT
    
      id::NUMBER                             AS pages_domain_id,
      project_id::NUMBER                     AS project_id,
      verified_at::TIMESTAMP                  AS verified_at,
      verification_code::VARCHAR              AS verification_code,
      enabled_until::TIMESTAMP                AS enabled_until,
      remove_at::TIMESTAMP                    AS remove_at,
      auto_ssl_enabled::BOOLEAN               AS is_auto_ssl_enabled,
      certificate_valid_not_before::TIMESTAMP AS certificate_valid_not_before,
      certificate_valid_not_after::TIMESTAMP  AS certificate_valid_not_after,
      certificate_source::VARCHAR             AS certificate_source
    
    FROM source
      
)

SELECT * 
FROM renamed
