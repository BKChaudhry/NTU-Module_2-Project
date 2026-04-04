with source as (
    select * from {{ source('oil_pipeline_raw', 'raw_oil_prices') }}
),
renamed as (
    select
        cast(date as date) as date,
        cast(brent_price as float64) as brent_price,
        cast(wti_price as float64) as wti_price,
        cast(dxy_index as float64) as dxy_index,
        cast(vix as float64) as vix,
        cast(gpr_index as float64) as gpr_index,
        cast(brent_return as float64) as brent_return,
        cast(wti_return as float64) as wti_return
    from source
    where date is not null
)
select * from renamed
