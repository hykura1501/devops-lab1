{{
    config(
        materialized='table'
    )
}}

with customer_orders as (
    select * from {{ ref('int_customer_orders') }}
),

product_sales as (
    select * from {{ ref('int_product_sales') }}
),

customer_order_aggregates as (
    select
        customer_id,
        person_id,
        account_number,
        full_name,
        first_name,
        last_name,
        email_promotion,
        territory_id,
        store_id,
        
        -- Order count metrics
        count(distinct sales_order_id) as total_orders,
        count(distinct order_date) as unique_order_dates,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        datediff(day, min(order_date), max(order_date)) as customer_lifetime_days,
        
        -- Revenue metrics
        sum(total_line_total) as total_lifetime_revenue,
        sum(total_gross_line_total) as total_gross_revenue,
        sum(total_discount_amount) as total_discounts_received,
        avg(total_line_total) as avg_order_value,
        max(total_line_total) as max_order_value,
        min(total_line_total) as min_order_value,
        
        -- Quantity metrics
        sum(total_order_quantity) as total_items_purchased,
        avg(total_order_quantity) as avg_items_per_order,
        
        -- Channel preferences
        sum(case when order_channel = 'Online' then 1 else 0 end) as online_orders,
        sum(case when order_channel = 'Offline' then 1 else 0 end) as offline_orders,
        sum(case when order_channel = 'Online' then total_line_total else 0 end) as online_revenue,
        sum(case when order_channel = 'Offline' then total_line_total else 0 end) as offline_revenue,
        
        -- Status metrics
        sum(case when order_status_name = 'Shipped' then 1 else 0 end) as shipped_orders,
        sum(case when order_status_name = 'Cancelled' then 1 else 0 end) as cancelled_orders,
        sum(case when shipping_performance = 'On Time' then 1 else 0 end) as on_time_orders,
        sum(case when shipping_performance = 'Late' then 1 else 0 end) as late_orders,
        
        -- Discount behavior
        sum(case when has_discount_flag = 1 then 1 else 0 end) as orders_with_discount,
        sum(case when has_discount_flag = 1 then total_discount_amount else 0 end) as total_discounts_used,
        
        -- Recency metrics
        datediff(day, max(order_date), getdate()) as days_since_last_order,
        
        -- Frequency metrics
        case 
            when datediff(day, min(order_date), max(order_date)) > 0
            then cast(count(distinct sales_order_id) as float) / datediff(day, min(order_date), max(order_date)) * 30
            else 0
        end as orders_per_month
        
    from customer_orders
    group by
        customer_id,
        person_id,
        account_number,
        full_name,
        first_name,
        last_name,
        email_promotion,
        territory_id,
        store_id
),

customer_product_metrics as (
    select
        customer_id,
        count(distinct product_id) as unique_products_purchased,
        sum(order_quantity) as total_products_quantity,
        sum(line_profit) as total_profit_generated,
        sum(net_line_profit) as total_net_profit_generated,
        avg(profit_margin_pct) as avg_profit_margin_pct
    from product_sales
    group by customer_id
),

customer_segmentation as (
    select
        c.*,
        p.unique_products_purchased,
        p.total_products_quantity,
        p.total_profit_generated,
        p.total_net_profit_generated,
        p.avg_profit_margin_pct,
        
        -- RFM Segmentation
        case 
            when c.days_since_last_order <= 30 then 5
            when c.days_since_last_order <= 60 then 4
            when c.days_since_last_order <= 90 then 3
            when c.days_since_last_order <= 180 then 2
            else 1
        end as recency_score,
        
        case 
            when c.orders_per_month >= 4 then 5
            when c.orders_per_month >= 2 then 4
            when c.orders_per_month >= 1 then 3
            when c.orders_per_month >= 0.5 then 2
            else 1
        end as frequency_score,
        
        case 
            when c.total_lifetime_revenue >= 10000 then 5
            when c.total_lifetime_revenue >= 5000 then 4
            when c.total_lifetime_revenue >= 2000 then 3
            when c.total_lifetime_revenue >= 1000 then 2
            else 1
        end as monetary_score,
        
        -- Customer segment
        case 
            when c.total_lifetime_revenue >= 10000 and c.orders_per_month >= 2 and c.days_since_last_order <= 30 then 'Champion'
            when c.total_lifetime_revenue >= 5000 and c.orders_per_month >= 1 and c.days_since_last_order <= 60 then 'Loyal Customer'
            when c.days_since_last_order <= 30 and c.total_orders >= 3 then 'Potential Loyalist'
            when c.days_since_last_order > 180 then 'At Risk'
            when c.days_since_last_order > 365 then 'Lost'
            when c.total_orders = 1 then 'New Customer'
            else 'Regular'
        end as customer_segment,
        
        -- Channel preference
        case 
            when c.online_orders > c.offline_orders * 2 then 'Online Preferred'
            when c.offline_orders > c.online_orders * 2 then 'Offline Preferred'
            else 'Mixed'
        end as channel_preference
        
    from customer_order_aggregates c
    left join customer_product_metrics p
        on c.customer_id = p.customer_id
)

select
    -- Customer identifiers
    customer_id,
    person_id,
    account_number,
    full_name,
    first_name,
    last_name,
    email_promotion,
    territory_id,
    store_id,
    
    -- Order metrics
    total_orders,
    unique_order_dates,
    first_order_date,
    last_order_date,
    customer_lifetime_days,
    orders_per_month,
    days_since_last_order,
    
    -- Revenue metrics
    total_lifetime_revenue,
    total_gross_revenue,
    total_discounts_received,
    avg_order_value,
    max_order_value,
    min_order_value,
    
    -- Quantity metrics
    total_items_purchased,
    avg_items_per_order,
    unique_products_purchased,
    total_products_quantity,
    
    -- Profit metrics
    total_profit_generated,
    total_net_profit_generated,
    avg_profit_margin_pct,
    
    -- Channel metrics
    online_orders,
    offline_orders,
    online_revenue,
    offline_revenue,
    channel_preference,
    case 
        when total_orders > 0 
        then cast(online_orders as float) / total_orders * 100 
        else 0 
    end as online_order_pct,
    
    -- Status metrics
    shipped_orders,
    cancelled_orders,
    on_time_orders,
    late_orders,
    case 
        when shipped_orders > 0 
        then cast(on_time_orders as float) / shipped_orders * 100 
        else 0 
    end as on_time_shipment_pct,
    
    -- Discount metrics
    orders_with_discount,
    total_discounts_used,
    case 
        when total_orders > 0 
        then cast(orders_with_discount as float) / total_orders * 100 
        else 0 
    end as discount_usage_pct,
    
    -- RFM Scores
    recency_score,
    frequency_score,
    monetary_score,
    recency_score + frequency_score + monetary_score as rfm_score,
    
    -- Customer segment
    customer_segment
    
from customer_segmentation

