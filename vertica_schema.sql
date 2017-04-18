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


-- copy sales from local 'sales_20170412213835' direct;


