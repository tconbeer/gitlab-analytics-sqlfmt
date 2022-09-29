{% snapshot sheetload_employee_location_factor_snapshots %}

{{
    config(
        unique_key='"Employee_ID"',
        strategy="timestamp",
        updated_at="_UPDATED_AT",
        enabled=False,
    )
}}

select *
from {{ source("sheetload", "employee_location_factor") }}
where "Employee_ID" != '' and "Location_Factor" not like '#N/A' escape '#'

{% endsnapshot %}
