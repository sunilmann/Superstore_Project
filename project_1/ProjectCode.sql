

--**************** create integration object ***********************************

create or replace storage integration int_object
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::627741535532:role/Tejaswini_Role_Booleandata'
storage_allowed_locations = ('s3://tejaswiniboolean/');

DESC INTEGRATION int_object;


create or replace table superstore(Row_ID int, 
                                   Order_ID varchar, 
                                   Order_Date date, 
                                   Ship_Date date,
                                   Ship_Mode varchar(),
                                   Customer_ID varchar(),
                                   Customer_Name string,
                                   Segment varchar(), 
                                   Country varchar(),
                                   City varchar(),
                                   State varchar(),
                                   Postal_Code int, 
                                   Region varchar(),
                                   Product_ID varchar(),
                                   Category varchar(), 
                                   Sub_Category varchar(),
                                   Product_Name varchar(),
                                   Sales float,
                                   Quantity int,
                                   Discount float,
                                   Profit float);



--************** file format creation ***********************************

create or replace file format my_csv_format
type = csv field_delimiter = ','
field_optionally_enclosed_by ='"' skip_header=1
validate_UTF8=false
null_if=('NULL', 'null')
empty_field_as_null = true;



--****************** stage creation ***************************************

create or replace stage mydb.Public.stage_1
url="s3://tejaswiniboolean/store"
STORAGE_INTEGRATION = int_object
file_format = my_csv_format;


create or replace TASK Task_on_thursday
WAREHOUSE = COMPUTE_WH,
SCHEDULE = 'USING CRON 0 12 * * */4 Asia/Kolkata'
AS
copy into mydb.Public.superstore
from (select t.$1,t.$2 ,t.$3 ,t.$4,t.$5,t.$6 ,t.$7 ,t.$8,t.$9,t.$10 ,t.$11 ,t.$12,t.$13,t.$14 ,t.$15 ,t.$16,$17,t.$18 ,t.$19 ,t.$20 ,t.$21 from @mydb.public.stage_1 t)
file_format=my_csv_format
on_error = 'skip_file';

list @mydb.public.stage_1;

select * from superstore;

Alter task Task_on_thursday resume;
Alter task Task_on_thursday suspend;

truncate table superstore;


---********************* creating stream ****************************************

create or replace stream superstore_stream on table superstore;

select * from superstore_STREAM;

update superstore set  state = 'Texas'  where row_id = 2;
update superstore set state = 'Kentucky' where row_id = 1;
--delete from superstore where row_id = 2;

create or replace table consumer_table
                                     (Row_ID int, 
                                      Order_ID varchar, 
                                      Order_Date date, 
                                      Ship_Date date,
                                      Ship_Mode varchar(),
                                      Customer_ID varchar(),
                                      Customer_Name string,
                                      Segment varchar(), 
                                      Country varchar(),
                                      City varchar(),
                                      State varchar(),
                                      Postal_Code int, 
                                      Region varchar(),
                                      Product_ID varchar(),
                                      Category varchar(), 
                                      Sub_Category varchar(),
                                      Product_Name varchar(),
                                      Sales float,
                                      Quantity int,
                                      Discount float,
                                      Profit float,
                                      stream_type string , 
                                      rec_version number default 0,
                                      REC_DATE TIMESTAMP_LTZ);


create warehouse task_warehouse with warehouse_size = 'XSMALL' auto_suspend = 120;


-- ************************ Create a task to schedule the MERGE statement *********************************

create or replace task superstore_history warehouse = task_warehouse schedule = '1 minute' when system$stream_has_data('superstore_stream')
as   

merge into consumer_table cd
using superstore_stream ss
on cd.Row_ID=ss.Row_Id and (metadata$action='DELETE')
when matched and metadata$isupdate='FALSE' then update set rec_version=9999, stream_type='DELETE'
when matched and metadata$isupdate='TRUE' then update set rec_version=9999, stream_type='DELETE'
when matched and metadata$isupdate='TRUE' then update set rec_version=rec_version-1
when not matched then

insert (Row_ID , Order_ID , Order_Date , Ship_Date ,Ship_Mode ,Customer_ID
,Customer_Name ,Segment , Country ,City,State ,Postal_Code, Region,
Product_ID ,Category, Sub_Category,Product_Name,Sales ,Quantity ,Discount ,Profit,stream_type,rec_version,REC_DATE)

values(Row_ID , Order_ID , Order_Date , Ship_Date ,Ship_Mode ,Customer_ID
,Customer_Name ,Segment , Country ,City,State ,Postal_Code, Region,
Product_ID ,Category, Sub_Category,Product_Name,Sales ,Quantity ,Discount ,Profit
, metadata$action,0,CURRENT_TIMESTAMP() );

show tasks;

select * from superstore_stream;
select * from consumer_table;

--******************** Analyzing the data **********************************


----1.Calculate the number of Orders those with Ship Mode as ‘Second Class’

select SHIP_MODE, count(SHIP_MODE) as orders from superstore
where SHIP_MODE = 'Second Class' group by SHIP_MODE;


---2.List down the most valuable customers Country wise

 select CUSTOMER_ID,CUSTOMER_NAME,COUNTRY,SALES from
(select *,rank() over(partition by COUNTRY order by SALES desc) as rank from superstore)
where rank =1 order by SALES desc ;


-------------------or-----------------------------------------
select CUSTOMER_ID,CUSTOMER_NAME,CITY,SALES from
(select CUSTOMER_ID,CUSTOMER_NAME,CITY,SALES,rank() over(partition by city order by SALES desc) as rank from superstore)
 where rank =1 order by SALES desc ;
 



--- 3.Total Sales for Category ‘Furniture’

select CATEGORY, count(CATEGORY) as Total_sales_furniture from superstore
where CATEGORY = 'Furniture' group by CATEGORY;


---4. Which Product provides the maximum profit 

select PRODUCT_NAME,max(profit) from superstore group by PRODUCT_NAME order by max(profit) desc limit 10;



---5.Calculate the total profit made by each product category country wise

select Country,Category,PRODUCT_NAME,sum(PROFIT) as Total_profit from
(select *,rank() over(partition by Category  order by PRODUCT_NAME desc) as rank from superstore)
group by country,product_name,Category order by country,product_name,Category;

-----------------------or---------------------------------------------

select city,PRODUCT_NAME,sum(PROFIT) as Total_profit from
(select CUSTOMER_ID,CUSTOMER_NAME,CITY,SALES,PRODUCT_NAME,profit,rank() over(partition by city order by PRODUCT_NAME desc) as rank from superstore)
group by (city,product_name);


---6.Which region of United States have majority loss 

select region,min(PROFIT) as major_loss from superstore group by region;



--********************************* ROW LEVEL SECURITY ********************************************************

-- Apply Row-level security using Secure Views

create or replace role South;
create or replace role West;
create or replace role Central;
create or replace role East;

create or replace user abhinav password = 'user123' default_Role = 'South' must_change_password = false;
grant role South to user abhinav;

create or replace user vikas password = 'user123' default_Role = 'West' must_change_password = false;
grant role West to user vikas;

create or replace user sunil password = 'user123' default_Role = 'Central' must_change_password = false;
grant role Central to user sunil;

create or replace user tejaswini password = 'user123' default_Role = 'East' must_change_password = false;
grant role East to user tejaswini;

grant role South to user Tejaswini1;
grant role West to user Tejaswini1;
grant role Central to user Tejaswini1;
grant role East to user Tejaswini1;

grant usage on warehouse compute_Wh to role South;
grant usage on warehouse compute_Wh to role West;
grant usage on warehouse compute_Wh to role Central;
grant usage on warehouse compute_Wh to role East;

grant usage on database mydb to role South;
grant usage on database mydb to role West;
grant usage on database mydb to role Central;
grant usage on database mydb to role East;

grant usage on schema public to role South;
grant usage on schema public to role West;
grant usage on schema public to role Central;
grant usage on schema public to role East;

create or replace secure view vw_manager as
select Row_ID , Order_ID , Order_Date , Ship_Date ,Ship_Mode ,Customer_ID
,Customer_Name ,Segment , Country ,City,State ,Postal_Code, Region,
Product_ID ,Category, Sub_Category,Product_Name,Sales ,Quantity ,Discount ,Profit
from superstore where upper(region)= upper(current_role());

select current_role();

grant select on view vw_manager to role South;
grant select on view vw_manager to role West;
grant select on view vw_manager to role Central;
grant select on view vw_manager to role East;

use role East;
use database mydb;
use schema public;

select * from vw_manager;




