{% snapshot netsuite_entity_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="entity_id",
        updated_at="last_modified_date",
    )
}} select * from {{ source("netsuite", "entity") }}

{% endsnapshot %}
