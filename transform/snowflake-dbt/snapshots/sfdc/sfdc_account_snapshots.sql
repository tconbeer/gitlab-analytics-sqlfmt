{% snapshot sfdc_account_snapshots %}

{{
    config(
        unique_key="id",
        strategy="timestamp",
        updated_at="systemmodstamp",
    )
}} select * from {{ source("salesforce", "account") }}

{% endsnapshot %}
