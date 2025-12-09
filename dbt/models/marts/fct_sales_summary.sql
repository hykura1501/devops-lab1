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

daily_sales_summary as (
    select
        -- Date dimension
        order_date,
        year(order_date) as order_year,
        month(order_date) as order_month,
        day(order_date) as order_day,
        datename(month, order_date) as order_month_name,
        datename(weekday, order_date) as order_day_name,
        
        -- Order metrics
        count(distinct sales_order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        count(distinct product_id) as unique_products_sold,
        
        -- Quantity metrics
        sum(order_quantity) as total_quantity_sold,
        avg(order_quantity) as avg_quantity_per_order,
        
        -- Revenue metrics
        sum(line_total) as total_revenue,
        sum(calculated_line_total) as total_revenue_calculated,
        sum(gross_line_total) as total_gross_revenue,
        sum(discount_amount) as total_discount_amount,
        avg(line_total) as avg_line_value,
        sum(line_total) / nullif(count(distinct sales_order_id), 0) as avg_order_value,
        
        -- Profit metrics
        sum(line_profit) as total_profit,
        sum(net_line_profit) as total_net_profit,
        avg(profit_margin_pct) as avg_profit_margin_pct,
        
        -- Channel metrics
        count(distinct case when order_channel = 'Online' then sales_order_id end) as online_orders,
        count(distinct case when order_channel = 'Offline' then sales_order_id end) as offline_orders,
        sum(case when order_channel = 'Online' then line_total else 0 end) as online_revenue,
        sum(case when order_channel = 'Offline' then line_total else 0 end) as offline_revenue,
        
        -- Status metrics
        count(distinct case when order_status_name = 'Shipped' then sales_order_id end) as shipped_orders,
        count(distinct case when order_status_name = 'Cancelled' then sales_order_id end) as cancelled_orders,
        
        -- Discount metrics
        count(distinct case when unit_price_discount > 0 then sales_order_id end) as orders_with_discount,
        sum(discount_amount) as total_discounts_applied,
        avg(case when unit_price_discount > 0 then discount_from_list_pct else 0 end) as avg_discount_pct
        
    from product_sales
    group by
        order_date,
        year(order_date),
        month(order_date),
        day(order_date),
        datename(month, order_date),
        datename(weekday, order_date)
),

monthly_sales_summary as (
    select
        order_year,
        order_month,
        order_month_name,
        count(distinct order_date) as days_with_orders,
        sum(total_orders) as total_orders,
        sum(unique_customers) as unique_customers,
        sum(unique_products_sold) as unique_products_sold,
        sum(total_quantity_sold) as total_quantity_sold,
        sum(total_revenue) as total_revenue,
        sum(total_gross_revenue) as total_gross_revenue,
        sum(total_discount_amount) as total_discount_amount,
        sum(total_profit) as total_profit,
        sum(total_net_profit) as total_net_profit,
        avg(avg_profit_margin_pct) as avg_profit_margin_pct,
        sum(online_orders) as online_orders,
        sum(offline_orders) as offline_orders,
        sum(online_revenue) as online_revenue,
        sum(offline_revenue) as offline_revenue,
        sum(shipped_orders) as shipped_orders,
        sum(cancelled_orders) as cancelled_orders,
        avg(avg_order_value) as avg_order_value,
        sum(orders_with_discount) as orders_with_discount,
        sum(total_discounts_applied) as total_discounts_applied
    from daily_sales_summary
    group by
        order_year,
        order_month,
        order_month_name
)

select
    -- Date dimensions
    d.order_date,
    d.order_year,
    d.order_month,
    d.order_day,
    d.order_month_name,
    d.order_day_name,
    
    -- Daily metrics
    d.total_orders,
    d.unique_customers,
    d.unique_products_sold,
    d.total_quantity_sold,
    d.total_revenue,
    d.total_gross_revenue,
    d.total_discount_amount,
    d.total_profit,
    d.total_net_profit,
    d.avg_profit_margin_pct,
    d.avg_order_value,
    
    -- Channel breakdown
    d.online_orders,
    d.offline_orders,
    d.online_revenue,
    d.offline_revenue,
    case 
        when d.total_orders > 0 
        then cast(d.online_orders as float) / d.total_orders * 100 
        else 0 
    end as online_order_pct,
    
    -- Status breakdown
    d.shipped_orders,
    d.cancelled_orders,
    case 
        when d.total_orders > 0 
        then cast(d.shipped_orders as float) / d.total_orders * 100 
        else 0 
    end as shipment_rate_pct,
    
    -- Discount metrics
    d.orders_with_discount,
    d.total_discounts_applied,
    d.avg_discount_pct,
    case 
        when d.total_orders > 0 
        then cast(d.orders_with_discount as float) / d.total_orders * 100 
        else 0 
    end as discount_order_pct,
    
    -- Monthly context
    m.total_orders as monthly_total_orders,
    m.total_revenue as monthly_total_revenue,
    m.avg_order_value as monthly_avg_order_value,
    case 
        when m.total_revenue > 0 
        then cast(d.total_revenue as float) / m.total_revenue * 100 
        else 0 
    end as daily_revenue_pct_of_month
    
from daily_sales_summary d
left join monthly_sales_summary m
    on d.order_year = m.order_year
    and d.order_month = m.order_month

