{% snapshot snapshot_stock_summary %}

{{
    config(
      target_schema='SNAPSHOT',
      unique_key='SYMBOL || DT',
      strategy='timestamp',
      updated_at='DT',
      invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('stock_summary') }}

{% endsnapshot %}