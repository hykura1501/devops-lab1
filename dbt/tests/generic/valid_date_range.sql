{% test valid_date_range(model, start_date_column, end_date_column) %}

    select *
    from {{ model }}
    where {{ start_date_column }} is not null
        and {{ end_date_column }} is not null
        and cast({{ start_date_column }} as date) > cast({{ end_date_column }} as date)

{% endtest %}

