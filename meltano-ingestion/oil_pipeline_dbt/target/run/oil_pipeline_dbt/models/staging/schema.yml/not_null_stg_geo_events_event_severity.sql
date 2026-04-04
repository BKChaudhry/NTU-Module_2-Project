
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_severity
from `module-2-project-490315`.`oil_pipeline_raw`.`stg_geo_events`
where event_severity is null



  
  
      
    ) dbt_internal_test