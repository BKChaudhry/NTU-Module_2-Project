with source as (
    select * from {{ source('oil_pipeline_raw', 'raw_geo_events') }}
),
renamed as (
    select
        cast(date as date) as date,
        event_type,
        event_description,
        cast(event_severity as int64) as event_severity
    from source
    where date is not null
)
select * from renamed
