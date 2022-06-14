 
 
 -- Apply Row-level security using Secure Views
-- create a secure view
create or replace role South;
create or replace role West;
create or replace role Central;
create or replace role East;


create or replace user abhinav password = 'temp123' default_Role = 'South' must_change_password = false;
grant role South to user abhinav;

create or replace user vikas password = 'temp123' default_Role = 'West' must_change_password = false;
grant role West to user vikas;

create or replace user sunil password = 'temp123' default_Role = 'Central' must_change_password = false;
grant role Central to user sunil;

create or replace user tejaswini password = 'temp123' default_Role = 'East' must_change_password = false;
grant role East to user tejaswini;

grant role South to user sunilraj;
grant role West to user sunilraj;
grant role Central to user sunilraj;
grant role East to user sunilraj;





grant usage on warehouse compute_Wh to role South;
grant usage on warehouse compute_Wh to role West;
grant usage on warehouse compute_Wh to role Central;
grant usage on warehouse compute_Wh to role East;

grant usage on database fdb to role South;
grant usage on database fdb to role West;
grant usage on database fdb to role Central;
grant usage on database fdb to role East;


grant usage on schema public to role South;
grant usage on schema public to role West;
grant usage on schema public to role Central;
grant usage on schema public to role East;





select * from superstore;

create or replace secure view vw_manager as
select Row_ID , Order_ID , Order_Date , Ship_Date ,Ship_Mode ,Customer_ID
,Customer_Name ,Segment , Country ,City,State ,Postal_Code, Region,
Product_ID ,Category, Sub_Category,Product_Name,Sales ,Quantity ,Discount ,Profit
from superstore where upper(region)= upper(current_role());
 
 
               
select current_role();
               
grant select on view vw_manager  to role South;
grant select on view vw_manager   to role West;
grant select on view vw_manager to role Central;
grant select on view vw_manager to role East;



select current_role();

use role Central;
use database fdb;
use schema public;

select * from vw_manager;


select count(city), city from superstore group by city;


select distinct region from superstore;
