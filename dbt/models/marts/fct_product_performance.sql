{{
    config(
        materialized='table'
    )
}}

with product_sales as (
    select * from {{ ref('int_product_sales') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

product_sales_aggregates as (
    select
        product_id,
        product_number,
        product_name,
        product_subcategory_id,
        subcategory_name,
        product_category_id,
        product_model_id,
        color,
        size,
        product_line,
        product_class,
        product_style,
        make_flag,
        finished_goods_flag,
        standard_cost,
        list_price,
        product_lifecycle_status,
        
        -- Sales count metrics
        count(distinct sales_order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        count(distinct order_date) as days_sold,
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date,
        datediff(day, min(order_date), max(order_date)) as sales_period_days,
        
        -- Quantity metrics
        sum(order_quantity) as total_quantity_sold,
        avg(order_quantity) as avg_quantity_per_order,
        max(order_quantity) as max_quantity_per_order,
        
        -- Revenue metrics
        sum(line_total) as total_revenue,
        sum(calculated_line_total) as total_revenue_calculated,
        sum(gross_line_total) as total_gross_revenue,
        avg(line_total) as avg_line_total,
        max(line_total) as max_line_total,
        min(line_total) as min_line_total,
        
        -- Pricing metrics
        avg(sale_unit_price) as avg_sale_price,
        min(sale_unit_price) as min_sale_price,
        max(sale_unit_price) as max_sale_price,
        avg(discount_from_list_pct) as avg_discount_from_list_pct,
        sum(discount_amount) as total_discount_amount,
        
        -- Profit metrics
        sum(line_profit) as total_profit,
        sum(net_line_profit) as total_net_profit,
        avg(unit_profit) as avg_unit_profit,
        avg(profit_margin_pct) as avg_profit_margin_pct,
        sum(line_profit) / nullif(sum(gross_line_total), 0) * 100 as overall_profit_margin_pct,
        
        -- Channel metrics
        sum(case when order_channel = 'Online' then 1 else 0 end) as online_orders,
        sum(case when order_channel = 'Offline' then 1 else 0 end) as offline_orders,
        sum(case when order_channel = 'Online' then line_total else 0 end) as online_revenue,
        sum(case when order_channel = 'Offline' then line_total else 0 end) as offline_revenue,
        
        -- Status metrics
        sum(case when order_status_name = 'Shipped' then 1 else 0 end) as shipped_orders,
        sum(case when order_status_name = 'Cancelled' then 1 else 0 end) as cancelled_orders
        
    from product_sales
    group by
        product_id,
        product_number,
        product_name,
        product_subcategory_id,
        subcategory_name,
        product_category_id,
        product_model_id,
        color,
        size,
        product_line,
        product_class,
        product_style,
        make_flag,
        finished_goods_flag,
        standard_cost,
        list_price,
        product_lifecycle_status
),

product_performance_metrics as (
    select
        psa.*,
        
        -- Sales velocity
        case 
            when psa.sales_period_days > 0
            then cast(psa.total_quantity_sold as float) / psa.sales_period_days
            else 0
        end as units_sold_per_day,
        
        case 
            when psa.sales_period_days > 0
            then cast(psa.total_revenue as float) / psa.sales_period_days
            else 0
        end as revenue_per_day,
        
        -- Customer metrics
        case 
            when psa.total_orders > 0
            then cast(psa.unique_customers as float) / psa.total_orders
            else 0
        end as customers_per_order,
        
        -- Price performance
        case 
            when psa.list_price > 0
            then (psa.avg_sale_price / psa.list_price) * 100
            else 0
        end as avg_price_vs_list_pct,
        
        -- Profitability ratios
        case 
            when psa.total_revenue > 0
            then (psa.total_profit / psa.total_revenue) * 100
            else 0
        end as profit_margin_pct,
        
        case 
            when psa.standard_cost > 0
            then ((psa.avg_sale_price - psa.standard_cost) / psa.standard_cost) * 100
            else 0
        end as markup_pct,
        
        -- Channel distribution
        case 
            when psa.total_orders > 0
            then cast(psa.online_orders as float) / psa.total_orders * 100
            else 0
        end as online_order_pct,
        
        case 
            when psa.total_revenue > 0
            then cast(psa.online_revenue as float) / psa.total_revenue * 100
            else 0
        end as online_revenue_pct,
        
        -- Performance classification
        case 
            when psa.total_revenue >= 50000 and psa.avg_profit_margin_pct >= 30 then 'Star Product'
            when psa.total_revenue >= 20000 and psa.avg_profit_margin_pct >= 20 then 'High Performer'
            when psa.total_revenue >= 10000 then 'Regular Performer'
            when psa.total_revenue >= 1000 then 'Low Performer'
            else 'Underperformer'
        end as performance_category,
        
        -- Inventory efficiency (if available)
        p.safety_stock_level,
        p.reorder_point,
        p.days_to_manufacture
        
    from product_sales_aggregates psa
    left join products p
        on psa.product_id = p.product_id
)

select
    -- Product identifiers
    product_id,
    product_number,
    product_name,
    product_subcategory_id,
    subcategory_name,
    product_category_id,
    product_model_id,
    
    -- Product attributes
    color,
    size,
    product_line,
    product_class,
    product_style,
    make_flag,
    finished_goods_flag,
    product_lifecycle_status,
    
    -- Pricing
    standard_cost,
    list_price,
    avg_sale_price,
    min_sale_price,
    max_sale_price,
    avg_price_vs_list_pct,
    avg_discount_from_list_pct,
    
    -- Sales metrics
    total_orders,
    unique_customers,
    days_sold,
    first_sale_date,
    last_sale_date,
    sales_period_days,
    
    -- Quantity metrics
    total_quantity_sold,
    avg_quantity_per_order,
    max_quantity_per_order,
    units_sold_per_day,
    
    -- Revenue metrics
    total_revenue,
    total_gross_revenue,
    total_discount_amount,
    avg_line_total,
    max_line_total,
    min_line_total,
    revenue_per_day,
    
    -- Profit metrics
    total_profit,
    total_net_profit,
    avg_unit_profit,
    avg_profit_margin_pct,
    overall_profit_margin_pct,
    profit_margin_pct,
    markup_pct,
    
    -- Channel metrics
    online_orders,
    offline_orders,
    online_revenue,
    offline_revenue,
    online_order_pct,
    online_revenue_pct,
    
    -- Status metrics
    shipped_orders,
    cancelled_orders,
    case 
        when total_orders > 0
        then cast(shipped_orders as float) / total_orders * 100
        else 0
    end as shipment_rate_pct,
    
    -- Customer metrics
    customers_per_order,
    
    -- Performance classification
    performance_category,
    
    -- Inventory metrics
    safety_stock_level,
    reorder_point,
    days_to_manufacture
    
from product_performance_metrics

