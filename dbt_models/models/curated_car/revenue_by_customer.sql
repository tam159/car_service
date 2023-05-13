{{ config(materialized="table", tags=["revenue"]) }}

with revenue_by_customer as (
select
  customer_id,
  ifnull(sum(cast(s.revenue as numeric)), 0) as subscription_revenue,
  ifnull(sum(cast(h.revenue as numeric)), 0) as hardware_revenue,
  ifnull(sum(cast(s.revenue as numeric)), 0) + ifnull(sum(cast(h.revenue as numeric)), 0) as total_revenue
from {{ source("raw_car", "customers") }} c
left join {{ ref("vw_subscription_events") }} s using(customer_id)
left join {{ source("raw_car", "hardware_sales") }} h on h.email = c.email
group by customer_id
order by customer_id
)
select r.*, a.status
from revenue_by_customer r
left join {{ ref("customer_subscription_access") }} a using(customer_id)
