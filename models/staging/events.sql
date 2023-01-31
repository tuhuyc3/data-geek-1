with event as 
(SELECT 
cast(extract(date from timestamp_micros(event_timestamp)) as date) as date
, cast(extract(datetime from timestamp_micros(event_timestamp)) as datetime) as datetime
  , event_name
   , device.operating_system_version	

, 'HDB' as game
, geo.city
, device.operating_system
, device.web_info.browser
, user_pseudo_id
, (select value.string_value from unnest(user_properties) where key = 'playerId') as player_id
, (select  value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id
, (select value.string_value from unnest(user_properties) where key = 'gameVersion') game_version
    , (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "adsType") AS ads_type
    , (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "pointcutID") AS placement
    , (SELECT  coalesce(coalesce(value.int_value,value.float_value),value.double_value) FROM UNNEST(event_params) WHERE key = "currentFPS") as current_fps
, (SELECT  coalesce(coalesce(value.int_value,value.float_value),value.double_value) FROM UNNEST(event_params) WHERE key = "minFPS") as min_fps
, (SELECT  coalesce(coalesce(value.int_value,value.float_value),value.double_value) FROM UNNEST(event_params) WHERE key = "maxFPS") as max_fps
, (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "pointcutID") as ad_point_cut
, (select value.string_value from unnest(event_params) where key = 'characterID') as character_id
, (select value.string_value from unnest(event_params) where key = 'gameMode') as game_mode
, (select  coalesce(coalesce(value.int_value,value.float_value),value.double_value) from unnest(event_params) where key = 'finalPositionInActionPhase') as game_rank
, (select  coalesce(coalesce(value.int_value,value.float_value),value.double_value) from unnest(event_params) where key = 'coinEarnings') as coin_earning
, (select  coalesce(coalesce(value.int_value,value.float_value),value.double_value) from unnest(event_params) where key = 'crownEarnings') as crown_earning
, (select  coalesce(coalesce(value.int_value,value.float_value),value.double_value) from unnest(event_params) where key = 'killCount') as kill_count
, (select  coalesce(coalesce(value.int_value,value.float_value),value.double_value) from unnest(event_params) where key = 'humanPlayersInGame') as human_count
, (select  coalesce(coalesce(value.int_value,value.float_value),value.double_value) from unnest(event_params) where key = 'itemSpentValue') as item_spent_value
, (select  coalesce(coalesce(value.int_value,value.float_value),value.double_value) from unnest(event_params) where key = 'itemPurchaseValue') as item_purchase_value
, (select value.string_value from unnest(event_params) where key = 'deploymentEnvironment') as deployment_environment
, (select value.string_value from unnest(event_params) where key = 'modeOfPayment') as type_of_spend
, (select value.string_value from unnest(event_params) where key = 'itemPurchaseID') as item_purchase_id
, (select value.string_value from unnest(event_params) where key = 'roundID') as round_id
, (select value.string_value from unnest(event_params) where key = 'id') as login_id
, (select value.string_value from unnest(event_params) where key = 'context') as context
, (select  coalesce(coalesce(value.int_value,value.float_value),value.double_value) from unnest(event_params) where key = 'roundTime') as round_time
, (select  coalesce(coalesce(value.int_value,value.float_value),value.double_value) from unnest(event_params) where key = 'finalPositionInActionPhase') as round_rank
, (select value.string_value from unnest(event_params) where key = 'checker') as checker
, (select value.string_value from unnest(event_params) where key = 'page_referrer') as page_referrer
, (select value.string_value from unnest(event_params) where key = 'page_location') as page_location, (select value.int_value from unnest(event_params) where key = 'isLoaded') as is_loaded
, (select value.int_value from unnest(event_params) where key = 'totalSec') as total_sec
, (select value.int_value from unnest(event_params) where key = 'itemCount') as item_count
, (select value.int_value from unnest(event_params) where key = 'totalRoundCount') as total_round_count
, geo.continent
, device.category
, device.web_info.hostname as host_name
, geo.country
, device.mobile_brand_name || ' ' || coalesce(device.mobile_marketing_name, device.mobile_model_name) as device_model


from 
{{ source('analytics_339895848', 'events_*') }})


select 
* except (game_version)
, max(game_version) over (partition by user_pseudo_id, game, session_id) game_version
from event