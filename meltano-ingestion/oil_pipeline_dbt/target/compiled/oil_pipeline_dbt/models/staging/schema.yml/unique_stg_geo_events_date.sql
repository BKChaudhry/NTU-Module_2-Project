
    
    

with dbt_test__target as (

  select date as unique_field
  from `module-2-project-490315`.`oil_pipeline_raw`.`stg_geo_events`
  where date is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


