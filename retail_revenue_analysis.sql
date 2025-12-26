drop table if exists online_retail.online_retail;

create table online_retail.online_retail (
    "InvoiceNo"   VARCHAR,
    "StockCode"   VARCHAR,
    "Description" TEXT,
    "Quantity"    INTEGER,
    "InvoiceDate" TIMESTAMP,
    "UnitPrice"   NUMERIC,
    "CustomerID"  INTEGER,
    "Country"     VARCHAR
);

alter table online_retail.online_retail
alter column "InvoiceDate" type VARCHAR;

copy online_retail.online_retail
from '/Users/aashrayadutt/Desktop/Online Retail.csv'
delimiter  ','
csv header;

alter table  online_retail.online_retail
alter column "InvoiceDate"
type timestamp
using to_timestamp("InvoiceDate", 'DD/MM/YY HH24:MI');


-- Core Business Health KPIs -- 

-- Total Revenue 

with txns as (
  select
    *,
    case
      when "InvoiceNo" like 'C%' then -abs("Quantity" * "UnitPrice")
      else abs("Quantity" * "UnitPrice")
    end as revenue
  from online_retail.online_retail
)
select sum(revenue) as total_revenue
from txns;

--Total Orders
with clean_orders as (
    select *
    from online_retail.online_retail
    where "CustomerID" is not null
      and "InvoiceNo" not like 'C%'
)
select count(distinct "InvoiceNo") as total_orders
from clean_orders;

-- Total Customers
with clean_orders as (
    select *
    from online_retail.online_retail
    where "CustomerID" is not null
    and "InvoiceNo" not like 'C%'
)
select count(distinct "CustomerID") as total_customers
from clean_orders;


-- Avg Order Value

with txns as (
  select
    "InvoiceNo",
    case
      when "InvoiceNo" like 'C%' then -abs("Quantity" * "UnitPrice")
      else abs("Quantity" * "UnitPrice")
    end as revenue
  from online_retail.online_retail
)
select
  round(sum(revenue) / count(distinct "InvoiceNo"), 2) as avg_order_value
from txns
where "InvoiceNo" not like 'C%';

-- Orders / per Customer -- 
select
  round(
    count(distinct "InvoiceNo")::numeric
    / count(distinct "CustomerID"),
    2
  ) as orders_per_customer
from online_retail.online_retail
where "CustomerID" is not null
  and "InvoiceNo" not like 'C%';

-- Growth Trend KPIs ---

-- Revenue by Country 
select
  "Country",
  round(sum(
    case
      when "InvoiceNo" like 'C%' then -abs("Quantity" * "UnitPrice")
      else abs("Quantity" * "UnitPrice")
    end
  ), 0) as revenue
from online_retail.online_retail
group by "Country"
order by revenue desc;

-- Monthly Revenue Trend 

select
  to_char(date_trunc('month', "InvoiceDate"), 'Mon YYYY') as month,
  round(sum(
    case
      when "InvoiceNo" like 'C%' then -abs("Quantity" * "UnitPrice")
      else abs("Quantity" * "UnitPrice")
    end
  ), 0) as revenue
from online_retail.online_retail
group by month
order by month;


-- Customer Quality KPIs -- 


-- % of Repeat Customers 
 
select 
	to_char(
	round(
		count(case when orders > 1 then 1 end):: numeric
		/ count(*)*100,
	2),
	'FM999.00') 
	|| '%'  as repeat_customer_rate
	from(	
select  "CustomerID",count(distinct "InvoiceNo") as orders
from online_retail.online_retail  
where "CustomerID" is not null
group by "CustomerID"
	);

-- Revenue Concentration (% of total revenue that comes from top 10% customers)
-- Calculate total revenue/customer,  Rank customers by revenue desc, identify top 10%,sum revenue, divide by total company revenue 
with txns as (
  select
    "CustomerID",
    case
      when "InvoiceNo" like 'C%' then -abs("Quantity" * "UnitPrice")
      else abs("Quantity" * "UnitPrice")
    end as revenue
  from online_retail.online_retail
  where "CustomerID" is not null
),
customer_revenue as (
  select "CustomerID", sum(revenue) as total_rev
  from txns
  group by "CustomerID"
),
ranked as (
  select *,
         ntile(10) over (order by total_rev desc) as rev_rank
  from customer_revenue
)
select
  round(
    sum(case when rev_rank = 1 then total_rev end)
    / sum(total_rev) * 100,
    2
  ) as top_10pct_rev_share
from ranked;

-- Customer HHI Risk  
-- Calculate total revenue/ cusotmer, calculate total revenue, customer revenue share, sq. CRS, sum of CRS

with customer_revenue as (
  select
    "CustomerID",
    sum(
      case
        when "InvoiceNo" like 'C%' then -abs("Quantity" * "UnitPrice")
        else abs("Quantity" * "UnitPrice")
      end
    ) as revenue
  from online_retail.online_retail
  where "CustomerID" is not null
  group by "CustomerID"
),
shares as (
  select revenue / sum(revenue) over () as rev_share
  from customer_revenue
)
select round(sum(rev_share * rev_share), 4) as customer_hhi
from shares;
	
-- Revenue Volatility 
with monthly_revenue as (
    select
        date_trunc('month', "InvoiceDate") as month_date,
        sum(
            case
                when "InvoiceNo" like 'C%' then -abs("Quantity" * "UnitPrice")
                else abs("Quantity" * "UnitPrice")
            end
        ) as revenue
    from online_retail.online_retail
    group by month_date
)
select round(
    stddev(revenue) / avg(revenue),
    2
) as revenue_volatility
from monthly_revenue;

-- Month on Month % Volatility  

with monthly_revenue as (
    select
        date_trunc('month', "InvoiceDate") as month_date,
        sum(
            case
                when "InvoiceNo" like 'C%' then -abs("Quantity" * "UnitPrice")
                else abs("Quantity" * "UnitPrice")
            end
        ) as revenue
    from online_retail.online_retail
    group by month_date
),
mom_change as (
    select
        month_date,
        revenue,
        (revenue - lag(revenue) over (order by month_date))
        / lag(revenue) over (order by month_date) * 100 as mom_pct_change
    from monthly_revenue
)
select
    to_char(month_date, 'Mon YYYY') as month,
    round(mom_pct_change, 2)|| '%' as mom_pct
from mom_change
order by month_date;

-- Geography Revenue Share 
with txns as (
    select *,
        case 
            when "InvoiceNo" like 'C%' then -abs("Quantity" * "UnitPrice")
            else abs("Quantity" * "UnitPrice")
        end as revenue
    from online_retail.online_retail
),
country_rev as (
    select "Country",
           sum(revenue) as total_revenue
    from txns
    group by "Country"
)
select "Country",
       total_revenue,
       100 * total_revenue / sum(total_revenue) over() as revenue_share
from country_rev
order by total_revenue desc;

-- UK vs Top 4 vs Rest of the World 
with txns as (
    select *,
        case 
            when "InvoiceNo" like 'C%' then -abs("Quantity" * "UnitPrice")
            else abs("Quantity" * "UnitPrice")
        end as revenue
    from online_retail.online_retail
),
country_rev as (
    select "Country", sum(revenue) as total_revenue
    from txns
    group by "Country"
),
ranked as (
    select *, row_number() over(order by total_revenue desc) as rn
    from country_rev
)
select
    case
        when rn = 1 then 'UK'
        when rn between 2 and 5 then 'Next Top 4'
        else 'Rest of World'
    end as region_group,
    sum(total_revenue) as revenue,
    100* sum(total_revenue)/sum(sum(total_revenue)) over() as revenue_share
from ranked
group by
    case
        when rn = 1 then 'UK'
        when rn between 2 and 5 then 'Next Top 4'
        else 'Rest of World'
    end;

