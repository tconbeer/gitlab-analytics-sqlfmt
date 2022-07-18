{% snapshot sheetload_kpi_status_snapshots %}

{{
    config(
        unique_key="unique_id",
        strategy="timestamp",
        updated_at="updated_at",
    )
}}

select
    md5(kpi_grouping || kpi_sub_grouping || kpi) as unique_id,
    *,
    _updated_at::number::timestamp as updated_at
from {{ source("sheetload", "kpi_status") }}

{% endsnapshot %}
