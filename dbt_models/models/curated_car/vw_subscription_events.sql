select
  event_type,
  order_id,
  date(timestamp) as date,
  first_value(customer_id) over(partition by order_id order by timestamp) as customer_id,
  if(event_type = "subscription_cancelled", null, first_value(revenue) over(partition by order_id order by timestamp)) as revenue
from {{ source("raw_car", "subscription_events") }}
