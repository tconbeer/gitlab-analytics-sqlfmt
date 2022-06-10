{{ config({"materialized": "view"}) }}

with source as (select * from {{ ref("license_db_licenses_source") }})

select *
from source
