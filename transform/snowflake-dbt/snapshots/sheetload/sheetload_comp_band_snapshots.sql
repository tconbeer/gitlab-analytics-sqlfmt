{% snapshot sheetload_comp_band_snapshots %}

{{
    config(
        unique_key="employee_number",
        strategy="timestamp",
        updated_at="updated_at",
    )
}}

select
    employee_number,
    percent_over_top_end_of_band,
    dateadd('sec', _updated_at, '1970-01-01')::timestamp as updated_at
from {{ source("sheetload", "comp_band") }}

{% endsnapshot %}
