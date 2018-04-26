with usage60 as (
  SELECT * FROM {{ ref('last60usagepings') }}
)

SELECT
  curls.clean_domain                                AS clean_url,
  coalesce(usage60.hostname, usage60.source_ip) AS raw_domain,
  usage60.*
FROM
  usage60
  JOIN public.cleaned_urls AS curls ON coalesce(usage60.hostname, usage60.source_ip) = curls.domain