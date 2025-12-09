{{
    config(
        materialized='view'
    )
}}

with sales_order_header_source as (
    select * from {{ source('adventureworks', 'SalesOrderHeader') }}
),

sales_order_detail_source as (
    select * from {{ source('adventureworks', 'SalesOrderDetail') }}
),

staged as (
    select
        -- Primary keys
        cast(h.SalesOrderID as int) as sales_order_id,
        cast(d.SalesOrderDetailID as int) as sales_order_detail_id,
        
        -- Order header attributes
        cast(h.OrderDate as date) as order_date,
        cast(h.DueDate as date) as due_date,
        cast(h.ShipDate as date) as ship_date,
        cast(h.Status as tinyint) as order_status,
        cast(case when h.OnlineOrderFlag = 1 then 1 else 0 end as bit) as is_online_order,
        trim(upper(h.SalesOrderNumber)) as sales_order_number,
        trim(coalesce(h.PurchaseOrderNumber, '')) as purchase_order_number,
        trim(coalesce(h.AccountNumber, '')) as account_number,
        
        -- Customer and sales information
        cast(h.CustomerID as int) as customer_id,
        cast(coalesce(h.SalesPersonID, 0) as int) as sales_person_id,
        cast(h.TerritoryID as int) as territory_id,
        
        -- Shipping information
        cast(h.ShipToAddressID as int) as ship_to_address_id,
        cast(h.BillToAddressID as int) as bill_to_address_id,
        cast(h.ShipMethodID as int) as ship_method_id,
        
        -- Order detail attributes
        cast(d.ProductID as int) as product_id,
        cast(d.OrderQty as smallint) as order_quantity,
        cast(d.UnitPrice as decimal(19,4)) as unit_price,
        cast(d.UnitPriceDiscount as decimal(19,4)) as unit_price_discount,
        cast(d.LineTotal as decimal(38,6)) as line_total,
        
        -- Calculated fields
        cast((d.OrderQty * d.UnitPrice * (1 - d.UnitPriceDiscount)) as decimal(38,6)) as calculated_line_total,
        
        -- Header totals
        cast(h.SubTotal as decimal(19,4)) as order_subtotal,
        cast(h.TaxAmt as decimal(19,4)) as order_tax_amount,
        cast(h.Freight as decimal(19,4)) as order_freight,
        cast(h.TotalDue as decimal(19,4)) as order_total_due,
        
        -- Metadata
        cast(h.ModifiedDate as datetime) as header_modified_date,
        cast(d.ModifiedDate as datetime) as detail_modified_date,
        cast(h.rowguid as varchar(36)) as header_row_guid
        
    from sales_order_header_source h
    inner join sales_order_detail_source d
        on h.SalesOrderID = d.SalesOrderID
    where h.SalesOrderID is not null
        and d.SalesOrderDetailID is not null
        and h.CustomerID is not null
        and d.ProductID is not null
)

select * from staged

