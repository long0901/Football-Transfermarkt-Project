with cte2 as (
    select
        player_id,
        extract(year from date) as season,
        club_id,  
        game_id,
        type
    from 
        {{ ref('stg_game_lineups') }}
),
cte3 as (
    select 
        club_id,
        game_id,
        is_win
    from 
        {{ ref('stg_club_games') }}
)
select
    player_id,
    season,
    count(case when 
        type='substitutes' then 1 end) / count(1) * 100 as substituting_percentage,
    count(case when 
        type='starting_lineup' then 1 end) / count(1) * 100 as starting_percentage,
    sum(case when 
        type='substitutes' then is_win end) / count(case when type='substitutes' then 1 end) * 100 as win_percentage_when_substitute,
    sum(case when 
        type='starting_lineup' then is_win end) / count(case when type='starting_lineup' then 1 end) * 100 as win_percentage_when_starting
from
    cte2 
join 
    cte3
on
    cte2.club_id = cte3.club_id 
and 
    cte2.game_id = cte3.game_id 
group by
    player_id, season
order by 
    player_id, season