with 
stg1 as 
(select 
    round_id
    , count(case when event_name='player_died' then 1 end) as dead_count,
    count(case when event_name='game_roundOver' then 1 end) as round_finish
    from {{ ref('stg_events') }}
    where round_id != 'null'
    group by round_id 
    having count(case when event_name='game_roundStart' then 1 end) > 0)

, stg2 as 
(select 
    dead_count,
    case when round_finish = 0 then 'left' else 'finished' end as round_status,
    count(round_id) as round_count
from stg1
group by 1, 2)

select *,
(round_count / SUM(round_count) OVER ()) AS ratio
from stg2						
order by round_count, round_status ASC