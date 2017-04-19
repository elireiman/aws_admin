create table if not exists sales
(
order_id int not null
, customer_id int
, product_id int
, amount decimal(20,6)
, transaction_date datetime not null
)
ORDER BY order_id, transaction_date
segmented by hash(order_id) all nodes ksafe 1
PARTITION BY date_trunc('month',transaction_date)::date
;

create table  if not exists customer
(
customer_id int not null
, first_name varchar(1111)
, last_name varchar(1111)
, credit_card varchar(1111)
, zip varchar(10)
, phone_number varchar(1111)
, date_joined datetime
)
ORDER BY customer_id
segmented by hash(customer_id) all nodes ksafe 1
;
