with income as (
    select 
        season,
        from_club_id as club_id,
        sum(transfer_fee) as total_transfers_income
    from 
        {{ ref('stg_transfers') }}
    group by
        season, from_club_id
),
spending as (
    select
        season,
        to_club_id as club_id,
        sum(transfer_fee) as total_transfers_spending
    from 
        {{ ref('stg_transfers') }}
    group by
        season, to_club_id
)
select 
    income.season,
    income.club_id,
    total_transfers_income,
    total_transfers_spending,
    (total_transfers_income - total_transfers_spending) as net_transfer_balance
from 
    income 
join 
    spending
on
    income.club_id = spending.club_id 
and 
    income.season=spending.season