{% snapshot sheetload_employee_location_factor_snapshots %}

    {{
        config(
          target_database='RAW',
          target_schema='snapshots',
          unique_key='"Employee_ID"',
          strategy='timestamp',
          updated_at='_UPDATED_AT',
        )
    }}
    
    SELECT * 
    FROM {{ source('sheetload', 'employee_location_factor') }}
    WHERE "Employee_ID" != ''
    
{% endsnapshot %}