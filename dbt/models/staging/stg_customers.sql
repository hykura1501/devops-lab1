{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('adventureworks', 'Customer') }}
),

person_source as (
    select * from {{ source('adventureworks_person', 'Person') }}
),

staged as (
    select
        -- Primary keys
        cast(c.CustomerID as int) as customer_id,
        cast(c.PersonID as int) as person_id,
        
        -- Customer attributes
        trim(upper(c.AccountNumber)) as account_number,
        cast(c.StoreID as int) as store_id,
        cast(c.TerritoryID as int) as territory_id,
        
        -- Person attributes (cleaned)
        trim(coalesce(p.FirstName, '')) as first_name,
        trim(coalesce(p.LastName, '')) as last_name,
        trim(coalesce(p.MiddleName, '')) as middle_name,
        cast(coalesce(p.EmailPromotion, 0) as int) as email_promotion,
        
        -- Timestamps
        cast(c.ModifiedDate as datetime) as modified_date,
        cast(c.rowguid as varchar(36)) as row_guid
        
    from source c
    left join person_source p
        on c.PersonID = p.BusinessEntityID
    where c.CustomerID is not null
        and c.PersonID is not null
)

select * from staged

