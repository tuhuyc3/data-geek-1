with dts as 
(select 
    min(date) as date
    , EXTRACT(WEEK(FRIDAY) FROM date) as week 
    , game
    , max(case when city is null or city like '%not set%' or city = '' then 'no data' else city end) as city
    , max(case when country is null or country like '%not set%' or country = '' then 'no data' else country end) as country
    , case when country in ('United States', 'Canada') then 'North America (US & CAN)' when continent = 'Americas' then 'LATAM' 
        when continent like '%not set%' then 'no data'  
        else continent end as continent
    , category
    , upper(browser) as browser
    , user_pseudo_id
    , session_id
    , 'Game Distribution' platform_category
    , max(game_version) as game_version
    , datetime_diff(max(date_time),min(date_time),second) as session_time_sec
    , count(case when ads_type = 'VideoReward' and event_name = 'game.ad.start' then event_name else null end) as video_rewards
    , count(case when ads_type = 'Interstitial' and event_name = 'game.ad.start' then event_name else null end) as interstitial
    , count(case when ads_type = 'Banner' and event_name = 'game.ad.start' then event_name else null end) as banner
    , count(distinct case when round_id = 'null' then null else round_id end) as round_count
    , sum(case when event_name = 'game_roundOver' then coin_earning end) coin_earning
    , sum(case when event_name = 'game_roundOver' then round_time end) as play_time
    , sum(case when event_name = 'player_purchase' then item_spent_value end) as item_spent_value
    , (min(case when event_name = 'game_roundStart' then current_fps end) + max(case when event_name = 'game_roundStart' then current_fps end))/2 as game_start_fps
    , (min(case when event_name = 'game_roundOver' then min_fps end) + max(case when event_name = 'game_roundOver' then min_fps end)
    + min(case when event_name = 'game_roundOver' then max_fps end) + max(case when event_name = 'game_roundOver' then max_fps end))/4 as game_over_fps
    , avg(current_fps) as average_fps
    , count(distinct case when event_name = 'add_to_home_complete' then event_name else null end) as add_to_home
    , max(device_model) as device_model
    , count( distinct case when event_name like '%party%' then session_id else null end ) party
    , max(human_count) as human_count
    , min(date_time) as date_time
    , max(deployment_environment) deployment_environment
    , max(operating_system_version) os_version
    , coalesce(max(case when event_name = 'game_techMessage' then checker end),'none') tech_message
    , max(game)  as raw_game_name 
    , max(player_id) as player_id

from {{ ref('staging_events') }}
where city not in ('Pune', 'Da Nang', 'Havant')  

group by 2,3,6,7,8,9,10,11)

select 
    dts.*  
    , row_number()  over(partition by dts.user_pseudo_id order by date_time) as session_order
from dts 
where 
    dts.user_pseudo_id not in 
        (select 
            user_pseudo_id 
        from 
            {{ ref('staging_events') }}
        where   
            (country = 'China' and city = '' and continent = '(not set)')
            )