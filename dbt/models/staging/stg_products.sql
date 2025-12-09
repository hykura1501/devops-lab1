{{
    config(
        materialized='view'
    )
}}

with product_source as (
    select * from {{ source('adventureworks_production', 'Product') }}
),

subcategory_source as (
    select * from {{ source('adventureworks_production', 'ProductSubcategory') }}
),

staged as (
    select
        -- Primary keys
        cast(p.ProductID as int) as product_id,
        cast(p.ProductSubcategoryID as int) as product_subcategory_id,
        cast(p.ProductModelID as int) as product_model_id,
        
        -- Product identifiers
        trim(upper(p.ProductNumber)) as product_number,
        trim(p.Name) as product_name,
        
        -- Product flags
        cast(case when p.MakeFlag = 1 then 1 else 0 end as bit) as make_flag,
        cast(case when p.FinishedGoodsFlag = 1 then 1 else 0 end as bit) as finished_goods_flag,
        
        -- Product attributes
        trim(coalesce(p.Color, 'Unknown')) as color,
        trim(coalesce(p.Size, '')) as size,
        trim(coalesce(p.SizeUnitMeasureCode, '')) as size_unit_measure_code,
        trim(coalesce(p.WeightUnitMeasureCode, '')) as weight_unit_measure_code,
        cast(coalesce(p.Weight, 0) as decimal(8,2)) as weight,
        trim(coalesce(p.ProductLine, '')) as product_line,
        trim(coalesce(p.Class, '')) as product_class,
        trim(coalesce(p.Style, '')) as product_style,
        
        -- Inventory levels
        cast(p.SafetyStockLevel as int) as safety_stock_level,
        cast(p.ReorderPoint as int) as reorder_point,
        cast(p.DaysToManufacture as int) as days_to_manufacture,
        
        -- Pricing
        cast(p.StandardCost as decimal(19,4)) as standard_cost,
        cast(p.ListPrice as decimal(19,4)) as list_price,
        
        -- Dates
        cast(p.SellStartDate as date) as sell_start_date,
        cast(p.SellEndDate as date) as sell_end_date,
        cast(p.DiscontinuedDate as date) as discontinued_date,
        
        -- Subcategory information
        trim(coalesce(ps.Name, '')) as subcategory_name,
        cast(coalesce(ps.ProductCategoryID, 0) as int) as product_category_id,
        
        -- Metadata
        cast(p.ModifiedDate as datetime) as modified_date,
        cast(p.rowguid as varchar(36)) as row_guid
        
    from product_source p
    left join subcategory_source ps
        on p.ProductSubcategoryID = ps.ProductSubcategoryID
    where p.ProductID is not null
)

select * from staged

