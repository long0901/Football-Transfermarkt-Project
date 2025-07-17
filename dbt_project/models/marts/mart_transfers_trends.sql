select 
    season,
    player_id as most_expensive_transfer_player_id,
    player_name as most_expensive_transfer_player_name,
    CAST(transfer_fee AS INT64) as most_expensive_transfer_fee
from 
    {{ ref('stg_transfers') }} as t1
where 
    transfer_fee = (
        select
            max(transfer_fee)
        from
            {{ ref('stg_transfers') }} as t2
        where
            t1.season = t2.season
        group by
            season
    )
and transfer_fee != 0.0
and season < 2025
order by season desc