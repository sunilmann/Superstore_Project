

-------creating stream
create or replace  stream superstore_stream on table superstore;

select * from superstore_stream;


------------------------------------------------------------------------------------------------

--CREATING CONSUMER TABLE AND COPYING DATA FROM STREAM



create or replace table consumer_data(Row_ID int, Order_ID varchar, Order_Date date, Ship_Date date,Ship_Mode varchar(),Customer_ID
varchar(),Customer_Name string,Segment varchar(), Country varchar(),City varchar(),State varchar(),Postal_Code int, Region varchar(),
Product_ID varchar(),Category varchar(), Sub_Category varchar(),Product_Name varchar(),Sales float,Quantity int,Discount float,Profit float, 
stream_type string , rec_version number default 0,REC_DATE TIMESTAMP_LTZ);

merge into consumer_data cd
using superstore_stream ss 
on cd.Row_ID=ss.Row_Id and (metadata$action='DELETE')
when matched and metadata$isupdate='FALSE' then update set rec_version=9999, stream_type='DELETE'
when matched and metadata$isupdate='TRUE' then update set rec_version=9999, stream_type='DELETE'
when matched and metadata$isupdate='TRUE' then update set rec_version=rec_version-1
when not matched then
insert  (Row_ID , Order_ID , Order_Date , Ship_Date ,Ship_Mode ,Customer_ID
,Customer_Name ,Segment , Country ,City,State ,Postal_Code, Region,
Product_ID ,Category, Sub_Category,Product_Name,Sales ,Quantity ,Discount ,Profit,stream_type,rec_version,REC_DATE )

values(ss.Row_ID , ss.Order_ID , ss.Order_Date , ss.Ship_Date ,ss.Ship_Mode ,ss.Customer_ID
,ss.Customer_Name ,ss.Segment , ss.Country ,ss.City,ss.State ,ss.Postal_Code, ss.Region,
ss.Product_ID ,ss.Category, ss.Sub_Category,ss.Product_Name,ss.Sales ,ss.Quantity ,ss.Discount ,ss.Profit 
, metadata$action,0,CURRENT_TIMESTAMP() );



select * from superstore_stream;

select * from consumer_data ;
