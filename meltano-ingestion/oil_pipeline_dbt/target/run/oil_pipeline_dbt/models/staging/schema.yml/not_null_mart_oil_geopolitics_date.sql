
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from `module-2-project-490315`.`oil_pipeline_raw`.`mart_oil_geopolitics`
where date is null



  
  
      
    ) dbt_internal_test