

----Calculate the number of Orders those with Ship Mode as ‘Second Class’


select SHIP_MODE, count(SHIP_MODE) as orders from consumer_data
where SHIP_MODE = 'Second Class' group by SHIP_MODE;


---List down the most valuable customers Country wise

select CUSTOMER_ID,CUSTOMER_NAME,CITY,SALES from
(select CUSTOMER_ID,CUSTOMER_NAME,CITY,SALES,rank() over(partition by city order by SALES desc) as rank from superstore) 
where rank =1 order by SALES desc ;


-----Total Sales for Category ‘Furniture’

select CATEGORY, count(CATEGORY) as TOTAL_SALES from superstore
where CATEGORY = 'Furniture' group by CATEGORY;

-------Which Product provides the maximum profit 

select PRODUCT_NAME,max(profit) from superstore group by PRODUCT_NAME order by max(profit) desc;

----Calculate the total profit made by each product category country wise 
select city,PRODUCT_NAME,sum(PROFIT) from
(select CUSTOMER_ID,CUSTOMER_NAME,CITY,SALES,PRODUCT_NAME,profit,rank() over(partition by city order by PRODUCT_NAME desc) as rank from superstore)
group by (city,product_name);

--Which region of United States have majority loss

 select region,min(PROFIT) as major_loss from consumer_data group by region
 
 
 
 
 
------------------------------------------------------------------------------------------------------------------------------------- 
 