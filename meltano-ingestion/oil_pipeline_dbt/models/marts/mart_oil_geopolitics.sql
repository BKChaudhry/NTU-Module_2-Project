with oil as (
    select * from {{ ref('stg_oil_prices') }}
),
events as (
    select * from {{ ref('stg_geo_events') }}
),
joined as (
    select
        o.date,
        o.brent_price,
        o.wti_price,
        o.dxy_index,
        o.vix,
        o.gpr_index,
        o.brent_return,
        o.wti_return,
        e.event_type,
        e.event_description,
        e.event_severity
    from oil o
    left join events e on o.date = e.date
)
select * from joined
