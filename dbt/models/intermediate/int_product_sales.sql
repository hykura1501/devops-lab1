{{
    config(
        materialized='table',
        indexes=[]
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

sales_orders as (
    select * from {{ ref('stg_sales_orders') }}
),

product_sales_detail as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['so.product_id', 'so.order_date', 'so.sales_order_id']) }} as product_sale_surrogate_key,
        
        -- Product identifiers
        so.product_id,
        p.product_number,
        p.product_name,
        p.product_subcategory_id,
        p.subcategory_name,
        p.product_category_id,
        p.product_model_id,
        
        -- Product attributes
        p.color,
        p.size,
        p.product_line,
        p.product_class,
        p.product_style,
        
        -- Product flags
        p.make_flag,
        p.finished_goods_flag,
        
        -- Product pricing
        p.standard_cost,
        p.list_price,
        so.unit_price as sale_unit_price,
        
        -- Price analysis
        so.unit_price - p.standard_cost as unit_profit,
        (so.unit_price - p.standard_cost) / nullif(p.standard_cost, 0) as profit_margin_pct,
        so.unit_price / nullif(p.list_price, 0) as discount_from_list_pct,
        
        -- Order information
        so.sales_order_id,
        so.sales_order_detail_id,
        so.order_date,
        so.due_date,
        so.ship_date,
        so.customer_id,
        
        -- Normalized order status
        case 
            when so.order_status = 1 then 'In Process'
            when so.order_status = 2 then 'Approved'
            when so.order_status = 3 then 'Backordered'
            when so.order_status = 4 then 'Rejected'
            when so.order_status = 5 then 'Shipped'
            when so.order_status = 6 then 'Cancelled'
            else 'Unknown'
        end as order_status_name,
        so.order_status as order_status_code,
        
        -- Normalized order channel
        case 
            when so.is_online_order = 1 then 'Online'
            else 'Offline'
        end as order_channel,
        
        -- Sales metrics
        so.order_quantity,
        so.unit_price,
        so.unit_price_discount,
        so.line_total,
        so.calculated_line_total,
        
        -- Derived business metrics
        so.order_quantity * so.unit_price as gross_line_total,
        so.order_quantity * so.unit_price * so.unit_price_discount as discount_amount,
        so.order_quantity * (so.unit_price - p.standard_cost) as line_profit,
        so.order_quantity * (so.unit_price - p.standard_cost) * (1 - so.unit_price_discount) as net_line_profit,
        
        -- Product lifecycle
        p.sell_start_date,
        p.sell_end_date,
        p.discontinued_date,
        case 
            when p.discontinued_date is not null then 'Discontinued'
            when p.sell_end_date is not null and p.sell_end_date < getdate() then 'Ended'
            when p.sell_start_date > getdate() then 'Not Started'
            else 'Active'
        end as product_lifecycle_status,
        
        -- Inventory metrics
        p.safety_stock_level,
        p.reorder_point,
        p.days_to_manufacture,
        
        -- Territory and sales person
        so.territory_id,
        so.sales_person_id,
        
        -- Timestamps
        so.detail_modified_date,
        p.modified_date as product_modified_date
        
    from sales_orders so
    inner join products p
        on so.product_id = p.product_id
    where p.product_id is not null
        and so.product_id is not null
)

select * from product_sales_detail

