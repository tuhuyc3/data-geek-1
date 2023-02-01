with game_event_s4 as
(SELECT 
cast(extract(date from timestamp_micros(event_timestamp)) as date) as date
, cast(extract(datetime from timestamp_micros(event_timestamp)) as datetime) as date_time
  , event_name
   , device.operating_system_version	

, 'MSR' as game
, geo.city
, device.operating_system
, device.web_info.browser
, user_pseudo_id
, (select value.string_value from unnest(event_params) where key = 'playerId') as player_id
, (select  value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id
, coalesce((select value.string_value from unnest(event_params) where key = 'gameVersion'),
(select value.string_value from unnest(user_properties) where key = 'gameVersion')) as game_version
, cast(extract(datetime from timestamp_micros(event_timestamp)) as datetime) as datetime

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
, traffic_source.name
, traffic_source.source 
, traffic_source.medium



 from `dng-imt-live.analytics_305396465.events_intraday_*` 
 
   union all

SELECT 
cast(extract(date from timestamp_micros(event_timestamp)) as date) as date
, cast(extract(datetime from timestamp_micros(event_timestamp)) as datetime) as datetime
  , event_name
   , device.operating_system_version	

, 'FHI' as game
, geo.city
, device.operating_system
, device.web_info.browser
, user_pseudo_id
, (select value.string_value from unnest(event_params) where key = 'playerId') as player_id
, (select  value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id
, (select value.string_value from unnest(event_params) where key = 'gameVersion') as game_version
, cast(extract(datetime from timestamp_micros(event_timestamp)) as datetime) as datetime

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
, traffic_source.name
, traffic_source.source 
, traffic_source.medium



 from `dng-fhio-live-dev.analytics_305405864.events_intraday_*` 
  union all

SELECT 
cast(extract(date from timestamp_micros(event_timestamp)) as date) as date
, cast(extract(datetime from timestamp_micros(event_timestamp)) as datetime) as datetime
  , event_name
   , device.operating_system_version	

, 'TCS' as game
, geo.city
, device.operating_system
, device.web_info.browser
, user_pseudo_id
, (select value.string_value from unnest(event_params) where key = 'playerId') as player_id
, (select  value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id
, (select value.string_value from unnest(event_params) where key = 'gameVersion') as game_version
, cast(extract(datetime from timestamp_micros(event_timestamp)) as datetime) as datetime

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
, traffic_source.name
, traffic_source.source 
, traffic_source.medium



 from `dng-tcest-live.analytics_305256413.events_intraday_*` 
  union all

SELECT 
cast(extract(date from timestamp_micros(event_timestamp)) as date) as date
, cast(extract(datetime from timestamp_micros(event_timestamp)) as datetime) as datetime
  , event_name
   , device.operating_system_version	

, 'HSAH' as game
, geo.city
, device.operating_system
, device.web_info.browser
, user_pseudo_id
, (select value.string_value from unnest(event_params) where key = 'playerId') as player_id
, (select  value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id
, (select value.string_value from unnest(event_params) where key = 'gameVersion') as game_version
, cast(extract(datetime from timestamp_micros(event_timestamp)) as datetime) as datetime

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
, traffic_source.name
, traffic_source.source 
, traffic_source.medium



 from `ubi-dng-hsah-live-prod.analytics_336061606.events_intraday_*` 
 
 union all 
 SELECT 
cast(extract(date from timestamp_micros(event_timestamp)) as date) as date
, cast(extract(datetime from timestamp_micros(event_timestamp)) as datetime) as date_time
  , event_name
   , device.operating_system_version	

, 'HDB' as game
, geo.city
, device.operating_system
, device.web_info.browser
, user_pseudo_id
, (select value.string_value from unnest(event_params) where key = 'playerId') as player_id
, (select  value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id
, coalesce((select value.string_value from unnest(event_params) where key = 'gameVersion'),
(select value.string_value from unnest(user_properties) where key = 'gameVersion')) as game_version
, cast(extract(datetime from timestamp_micros(event_timestamp)) as datetime) as datetime

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
, traffic_source.name
, traffic_source.source 
, traffic_source.medium



 from `ubi-dng-hdb-live-prod.analytics_339895848.events_intraday_*` 
 )


 select 
* except (game_version)
, max(game_version) over (partition by user_pseudo_id, game, session_id) game_version
 from game_event_s4