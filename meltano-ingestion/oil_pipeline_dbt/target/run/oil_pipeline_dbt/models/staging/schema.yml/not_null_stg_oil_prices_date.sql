
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from `module-2-project-490315`.`oil_pipeline_raw`.`stg_oil_prices`
where date is null



  
  
      
    ) dbt_internal_test