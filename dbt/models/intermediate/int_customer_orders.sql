{{
    config(
        materialized='table',
        indexes=[]
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

sales_orders as (
    select * from {{ ref('stg_sales_orders') }}
),

customer_order_summary as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['so.customer_id', 'so.sales_order_id', 'so.order_date']) }} as customer_order_surrogate_key,
        
        -- Customer identifiers
        so.customer_id,
        c.person_id,
        c.account_number,
        
        -- Customer attributes
        c.first_name,
        c.last_name,
        c.middle_name,
        concat(
            trim(coalesce(c.first_name, '')),
            case when c.middle_name is not null and c.middle_name != '' then ' ' + trim(c.middle_name) else '' end,
            ' ',
            trim(coalesce(c.last_name, ''))
        ) as full_name,
        c.email_promotion,
        c.territory_id,
        c.store_id,
        
        -- Order attributes
        so.sales_order_id,
        so.order_date,
        so.due_date,
        so.ship_date,
        
        -- Normalized order status enum
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
        
        -- Normalized order channel enum
        case 
            when so.is_online_order = 1 then 'Online'
            else 'Offline'
        end as order_channel,
        so.is_online_order,
        
        -- Derived date fields
        datediff(day, so.order_date, so.due_date) as days_until_due,
        datediff(day, so.order_date, so.ship_date) as days_to_ship,
        case 
            when so.ship_date is not null and so.ship_date <= so.due_date then 'On Time'
            when so.ship_date is not null and so.ship_date > so.due_date then 'Late'
            else 'Not Shipped'
        end as shipping_performance,
        
        -- Order metrics
        so.order_quantity,
        so.unit_price,
        so.unit_price_discount,
        so.line_total,
        so.calculated_line_total,
        so.order_subtotal,
        so.order_tax_amount,
        so.order_freight,
        so.order_total_due,
        
        -- Derived business metrics
        so.order_quantity * so.unit_price as gross_line_total,
        so.order_quantity * so.unit_price * so.unit_price_discount as discount_amount,
        case 
            when so.unit_price_discount > 0 then 1
            else 0
        end as has_discount_flag,
        
        -- Sales person and territory
        so.sales_person_id,
        so.territory_id as order_territory_id,
        
        -- Timestamps
        so.header_modified_date,
        c.modified_date as customer_modified_date
        
    from sales_orders so
    inner join customers c
        on so.customer_id = c.customer_id
),

aggregated_orders as (
    select
        customer_order_surrogate_key,
        customer_id,
        person_id,
        account_number,
        first_name,
        last_name,
        middle_name,
        full_name,
        email_promotion,
        territory_id,
        store_id,
        sales_order_id,
        order_date,
        due_date,
        ship_date,
        order_status_name,
        order_status_code,
        order_channel,
        is_online_order,
        days_until_due,
        days_to_ship,
        shipping_performance,
        sum(order_quantity) as total_order_quantity,
        sum(line_total) as total_line_total,
        sum(calculated_line_total) as total_calculated_line_total,
        sum(gross_line_total) as total_gross_line_total,
        sum(discount_amount) as total_discount_amount,
        max(has_discount_flag) as has_discount_flag,
        max(order_subtotal) as order_subtotal,
        max(order_tax_amount) as order_tax_amount,
        max(order_freight) as order_freight,
        max(order_total_due) as order_total_due,
        max(sales_person_id) as sales_person_id,
        max(order_territory_id) as order_territory_id,
        max(header_modified_date) as header_modified_date,
        max(customer_modified_date) as customer_modified_date
    from customer_order_summary
    group by
        customer_order_surrogate_key,
        customer_id,
        person_id,
        account_number,
        first_name,
        last_name,
        middle_name,
        full_name,
        email_promotion,
        territory_id,
        store_id,
        sales_order_id,
        order_date,
        due_date,
        ship_date,
        order_status_name,
        order_status_code,
        order_channel,
        is_online_order,
        days_until_due,
        days_to_ship,
        shipping_performance
)

select * from aggregated_orders

