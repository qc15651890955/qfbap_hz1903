drop table if exists qfbap_dm.dm_user_basic;
create table qfbap_dm.dm_user_basic
(
 user_id bigint,
 user_name varchar,
 user_gender tinyint,
 user_birthday varchar,
 user_age int,
 constellation varchar,
 province varchar,
 city varchar,
 city_level tinyint,
 e_mail varchar,
 op_mail varchar,
 mobile bigint,
 num_seg_mobile int,
 op_Mobile varchar,
 register_time varchar,
 login_ip varchar,
 login_source varchar,
 request_user varchar,
 total_score decimal(18,2),
 used_score decimal(18,2),
 is_blacklist tinyint,
 is_married tinyint,
 education varchar,
 monthly_income decimal(18,2),
 profession varchar,
 is_pregnant_woman tinyint,
 is_have_children tinyint,
 is_have_car tinyint,
 phone_brand varchar,
 phone_brand_level varchar,
 phone_cnt int,
 change_phone_cnt int,
 is_maja tinyint,
 majia_account_cnt int,
 loyal_model varchar,
 shopping_type_model varchar,
 weight int,
 height int
)
location '/qfbap/dm/qfbap_dm.dm_user_basic';

insert into table qfbap_dm.dm_user_basic
select
t1.user_id,
user_name,
t1.user_gender,
user_birthday,
user_age,
constellation,
province,
city,
city_level,
e_mail,
op_mail,
mobile,
num_seg_mobile,
op_Mobile,
register_time,
login_ip,
login_source,
request_user,
total_score,
used_score,
is_blacklist,
is_married,
education,
monthly_income,
profession,
is_pregnant_woman,
is_have_children,
is_have_car,
phone_brand,
phone_brand_level,
phone_cnt,
change_phone_cnt,
is_maja,
majia_account_cnt,
loyal_model,
shopping_type_model,
weight,
height
from qfbap_ods.ods_user t1
left join
qfbap_ods.ods_user_extend t2
on t1.user_id=t2.user_id;



create database sales_ods;
CREATE TABLE sales_ods.customer
(
 customer_number INT,
 customer_name VARCHAR(128),
 customer_street_address VARCHAR(256),
 customer_zip_code INT,
 customer_city VARCHAR(32),
 customer_state VARCHAR(32)  
);
CREATE TABLE sales_ods.product
(
 product_code INT,
 product_name VARCHAR(128),
 product_category VARCHAR(256)  
);
CREATE TABLE sales_ods.sales_order
(
order_number INT,
customer_number INT,
product_code INT,
order_date timestamp,
entry_date timestamp,
order_amount DECIMAL(18,2)  
)
Partitioned by(orderdate string)
row format delimited fields terminated by ',';


create database sales_ods1;
CREATE TABLE sales_ods.customer1
(
 customer_number INT,
 customer_name VARCHAR(128),
 customer_street_address VARCHAR(256),
 customer_zip_code INT,
 customer_city VARCHAR(32),
 customer_state VARCHAR(32)  
);
CREATE TABLE sales_ods.product1
(
 product_code INT,
 product_name VARCHAR(128),
 product_category VARCHAR(256)  
);
CREATE TABLE sales_ods.sales_order1
(
order_number INT,
customer_number INT,
product_code INT,
order_date timestamp,
entry_date timestamp,
order_amount DECIMAL(18,2)  
)
Partitioned by(orderdate string)
row format delimited fields terminated by ',';


