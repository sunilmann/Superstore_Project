

-------------------------------------------------------------------

-- CREATING TABLE 


create or replace table superstore(Row_ID int, Order_ID varchar, Order_Date date, Ship_Date date,Ship_Mode varchar(),Customer_ID
varchar(),Customer_Name string,Segment varchar(), Country varchar(),City varchar(),State varchar(),Postal_Code int, Region varchar(),
Product_ID varchar(),Category varchar(), Sub_Category varchar(),Product_Name varchar(),Sales float,Quantity int,Discount float,Profit float);



---------------------------------------------------------------------------------------
--file format creation

create or replace file format my_csv_format
type = csv field_delimiter = ',' 
field_optionally_enclosed_by ='"' skip_header=1
validate_UTF8=false 
null_if=('NULL', 'null')
empty_field_as_null = true;


-------------------------------------------------------------------------------------------

--stage creation
create or replace stage fdb.PUBLIC.stage
url="s3://sunils3buckett" 
STORAGE_INTEGRATION=s3_int
file_format = my_csv_format;


----------------------------------------------------------------------------------------------

--copy command
create or replace TASK mytask_on_thursday
WAREHOUSE = COMPUTE_WH,
SCHEDULE = 'USING CRON 0 12 * * 4 America/Los_Angeles'
AS
copy into fdb.PUBLIC.superstore 
from (select t.$1,t.$2 ,t.$3 ,t.$4,t.$5,t.$6 ,t.$7 ,t.$8,t.$9,t.$10 ,t.$11 ,t.$12,t.$13,t.$14 ,t.$15 ,t.$16,$17,t.$18 ,t.$19 ,t.$20 ,t.$21 from @fdb.public.stage t)
file_format=my_csv_format
on_error = 'skip_file';

---------------------------------------------------------------------------------------------------------------------------

-- task

create or replace TASK Task_on_thursday
WAREHOUSE = COMPUTE_WH,
SCHEDULE = 'USING CRON 0 12 * * */4 Asia/Kolkata'

Alter task Task_on_thursday resume;
Alter task Task_on_thursday suspend;


----------------------------------------------------------------------------------------------------------------------------


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


