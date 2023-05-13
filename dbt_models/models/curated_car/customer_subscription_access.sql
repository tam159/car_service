{{ config(materialized="table", tags=["access"]) }}

with latest_subscription as (
select
  customer_id,
  max(date) as latest_subscription_date
from {{ ref("vw_subscription_events") }}
where event_type <> "subscription_cancelled"
group by 1
),
cancelled_customer as (
select
  customer_id,
  date as cancellation_date
from {{ ref("vw_subscription_events") }}
where event_type = 'subscription_cancelled'
),
end_subscription_date as (
select
  customer_id,
  latest_subscription_date,
  cancellation_date,
  date_add(latest_subscription_date, interval 1 year) as end_date
from latest_subscription
left join cancelled_customer using(customer_id)
),
day_diff as (
select
  customer_id,
  latest_subscription_date,
  end_date,
  cancellation_date,
  date_diff(end_date, cancellation_date, day) as end_cancell_diff_day
from end_subscription_date
)
select
  customer_id,
  latest_subscription_date,
  end_date,
  cancellation_date,
  end_cancell_diff_day,
  case when end_date > '2023-04-20' then 'active'
    when end_date <= '2023-04-20' and end_cancell_diff_day >= 0 then 'inactive'
    else 'error' end as status
from day_diff
