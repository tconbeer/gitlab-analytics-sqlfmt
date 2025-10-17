with base as (select * from {{ ref("version_versions_source") }}) select * from base
