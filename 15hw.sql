-- ДЗ
-- Добавить в предыдущее ДЗ следующие колонки:
-- Сколько товаров прошло Переупаковку после Возврата.
-- Сколько товаров попало в другой заказ после Возврата.

-- 01 Схема витрины

drop TABLE if exists report.returned_orders_101_v2;
CREATE TABLE report.returned_orders_101_v2
(
    src_office_id     UInt64,
    dt_date_return	  Date,
    qty_orders		  UInt64,
    arr_order_item	  Array(Tuple(UInt64, UInt64)),
    qty_WPU			  UInt64,
    qty_new_order	  UInt64,
    last_date		  DateTime materialized now()
)
ENGINE = ReplacingMergeTree(last_date)
ORDER BY (src_office_id, dt_date_return)
TTL dt_date_return + toIntervalDay(60)
SETTINGS index_granularity = 8192;

-- 02 Запросы сборки витрины

--02.01.Отбираю возвращенные заказы

------------------------------------------
select quantiles(0.3, 0.6)(position_id)
from history.OrderDetails od 
where status_id = 8 and is_marketplace = 0

[600975005337.7,601033465318.2001]
------------------------------------------

drop table tmp.tmp_return_1_101;
create table tmp.tmp_return_1_101 
(
	dt				DateTime,
	position_id		UInt64,
	status_id		UInt64,
	src_office_id	UInt64,
	item_id			UInt64
)
ENGINE = MergeTree()
order by (position_id)

insert into tmp.tmp_return_1_101
select dt
	, position_id 
	, status_id
	, src_office_id
	, item_id
from history.OrderDetails
where status_id = 8 and is_marketplace = 0 

select count()
from tmp.tmp_return_1_101;

--02.02. Отбираю записи из item_mx по офисам из первой времянки
drop table tmp.tmp_return_2_101;
create table tmp.tmp_return_2_101 
(
	item_id		UInt64,
	dt			DateTime,
	mx			UInt64,
	office_id	UInt64
)
ENGINE = MergeTree()
order by (dt, mx)

-------------------------------------------------------------
select min(dt), max(dt), count() from history.OrderDetails od

select quantiles(0.3, 0.6, 0.8)(item_id)
from history.item_mx 
where toUInt64(item_id) in (select toUInt64(item_id) from tmp.tmp_return_1_101)

[6798209858.2,7398406546.6,7787815346.6]
-------------------------------------------------------------

insert into tmp.tmp_return_2_101
select toUInt64(item_id) item_id
	, dt
	, mx
	, dictGet('dictionary.StoragePlace','office_id', toUInt64(mx)) office_id
from history.item_mx
where item_id < 6798209858 and toUInt64(item_id) in (select toUInt64(item_id) from tmp.tmp_return_1_101)

select * from tmp.tmp_return_2_101 
limit 100

select count() from tmp.tmp_return_2_101 


--02.03.
-------------------------------------------------------------
select quantiles(0.2, 0.4, 0.6, 0.8)(item_id)
from tmp.tmp_return_2_101  

[6407672208.2,7037351509.200001,7405243710.4,7783571344.2]
-------------------------------------------------------------

drop TABLE if exists tmp.tmp_return_3_101;
CREATE TABLE tmp.tmp_return_3_101
(
    src_office_id     UInt64,
    position_id		  UInt64,
    dt_return		  DateTime,
    item_id			  UInt64
)
ENGINE = MergeTree()
ORDER BY (src_office_id, dt_return)

insert into tmp.tmp_return_3_101
select distinct src_office_id
	, position_id
	, dt dt_return
	, l.item_id item_id
from
(
	select dt
		, position_id
		, src_office_id
		, item_id
	from tmp.tmp_return_1_101
	where item_id < 6407672208 --and item_id < 7037351509
) l
asof join
(
	select dt dt_return
		, mx
		, office_id
		, item_id
	from tmp.tmp_return_2_101
	where item_id < 6407672208 --and item_id < 7037351509
) r
on l.src_office_id = r.office_id and l.dt < r.dt_return and l.item_id = r.item_id

select count() from tmp.tmp_return_3_101

select * from tmp.tmp_return_3_101
limit 1000

--------------------------------
--Создаю времянку, чтобы отобрать статусы с переупаковкой
drop table if exists tmp.tmp_return_WPU_101;
create table tmp.tmp_return_WPU_101 ENGINE = MergeTree() ORDER BY (item_id, dt) AS 
select *
from history.ItemState is2 
where state_id = 'WPU' and toUInt64(item_id) in (select toUInt64(item_id) from tmp.tmp_return_3_101)

select count() from tmp.tmp_return_WPU_101

select * from tmp.tmp_return_WPU_101
limit 100
--------------------------------

--02.04.
drop table if exists tmp.tmp_return_4_101;
create table tmp.tmp_return_4_101 
(
	src_office_id     UInt64,
    dt_date_return	  Date,
    qty_WPU			  UInt64
)
ENGINE = MergeTree()
order by (src_office_id, dt_date_return)

insert into tmp.tmp_return_4_101
select src_office_id
	, toDate(dt_return) dt_date_return
	, uniq(item_id) qty_WPU
from
(
	select src_office_id
		, position_id
		, dt_return
		, toUInt64(item_id) item_id
	from tmp.tmp_return_3_101
	--where item_id < 6407672208 --and item_id < 7037351509
) l
asof join
(
	select toUInt64(item_id) item_id_st
		, dt
		, state_id
	from tmp.tmp_return_WPU_101
	--where item_id < 6407672208 --and item_id < 7037351509
) r
on l.item_id = r.item_id_st and l.dt_return < r.dt
group by src_office_id, dt_date_return
order by src_office_id, dt_date_return

select * from tmp.tmp_return_4_101
limit 100

select count() from tmp.tmp_return_4_101

--02.05.
[6407672208.2,7037351509.200001,7405243710.4,7783571344.2]

drop table if exists tmp.tmp_return_5_101;
create table tmp.tmp_return_5_101 
(
	src_office_id		UInt64,
	dt_date				Date,
	item_id				UInt64
)
engine = MergeTree
order by (src_office_id, dt_date)

insert into tmp.tmp_return_5_101
select l.src_office_id src_office_id
	, toDate(l.dt) dt_date
	, l.item_id item_id
from
(
	select dt
		, position_id
		, src_office_id
		, item_id
	from tmp.tmp_return_1_101
	where item_id < 6407672208 --and item_id < 7037351509
) l
asof join
(
	select dt
		, position_id
		, src_office_id
		, item_id
	from history.OrderDetails od 
	where toUInt64(item_id) in (select toUInt64(item_id) from tmp.tmp_return_1_101) and item_id < 6407672208 --and item_id < 7037351509
) r
on l.item_id = r.item_id and l.dt < r.dt 
where l.position_id != r.position_id
order by src_office_id, dt_date

select * from tmp.tmp_return_5_101
limit 10

select count() from tmp.tmp_return_5_101



drop table if exists tmp.tmp_return_6_101;
create table tmp.tmp_return_6_101 
(
	src_office_id		UInt64,
	dt_date				Date,
	qty_new_order		UInt64
)
engine = MergeTree
order by (src_office_id, dt_date)

insert into tmp.tmp_return_6_101
select src_office_id
	, dt_date
	, uniq(item_id) qty_new_order
from tmp.tmp_return_5_101
group by src_office_id, dt_date
order by src_office_id, dt_date

select * from tmp.tmp_return_6_101
limit 100

select count() from tmp.tmp_return_6_101

--02.07.
--------------------------
select * from tmp.tmp_return_3_101
limit 100

select * from tmp.tmp_return_4_101
limit 100

select * from tmp.tmp_return_6_101
limit 100
--------------------------

drop TABLE if exists report.returned_orders_101_v2;
CREATE TABLE report.returned_orders_101_v2
(
    src_office_id     UInt64,
    dt_date_return	  Date,
    qty_orders		  UInt64,
    arr_order_item	  Array(Tuple(UInt64, UInt64)),
    qty_WPU			  UInt64,
    qty_new_order	  UInt64,
    last_date		  DateTime materialized now()
)
ENGINE = ReplacingMergeTree(last_date)
ORDER BY (src_office_id, dt_date_return)
TTL dt_date_return + toIntervalDay(60)
SETTINGS index_granularity = 8192;


insert into report.returned_orders_101_v2
select tmp3.src_office_id src_office_id
	, tmp3.dt_date_return dt_date_return 
	, qty_orders
	, arr_order_item
	, qty_WPU
	, qty_new_order
from
(
	select src_office_id
		, toDate(dt_return) dt_date_return
		, uniq(item_id) qty_orders
		, groupArray(10)((position_id, item_id)) arr_order_item
	from tmp.tmp_return_3_101
	group by src_office_id, dt_date_return
	order by src_office_id, dt_date_return 
) tmp3
join
(
	select src_office_id 
		, dt_date_return 
		, qty_WPU
	from tmp.tmp_return_4_101
) tmp4
on tmp3.src_office_id = tmp4.src_office_id and tmp3.dt_date_return = tmp4.dt_date_return
join
(
	select src_office_id 
		, dt_date 
		, qty_new_order
	from tmp.tmp_return_6_101
) tmp6
on tmp4.src_office_id = tmp6.src_office_id and tmp4.dt_date_return = tmp6.dt_date
order by src_office_id, dt_date_return 

select * from report.returned_orders_101_v2

select count() from report.returned_orders_101_v2

-- 03 Запросы обновления витрины


-- 04 За 5 последних дней по всем Офисам оформления посчитать кол-во уникальных заказов по направлениям.
-- Можно ограничить по дате за последние 5 дней.
-- * Для тех кто сможет, не обязательно:
--   Для заказов, у которых в названии Офиса оформления написано "Склад поставщика",
--     найти первый Офис по МХ и использовать этот офис для вывода инфы в запросе.