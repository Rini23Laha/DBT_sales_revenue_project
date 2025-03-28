{{

    config (tag = 'contains_pii'
    )
}}

select * from 
{{source('landing','users')}}