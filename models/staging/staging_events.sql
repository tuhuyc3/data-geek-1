with 
msr as 
(select * from {{ source('analytics_305396465', 'events_*')}} )
,
fhi as 
(select * from {{ source('analytics_305405864', 'events_*') }})
,
tcs as 
(select * from {{ source('analytics_305256413', 'events_*') }})
,
hsah as 
(select * from {{ source('analytics_336061606', 'events_*') }})
,
hdb as 
(select * from {{ source('analytics_339895848', 'events_*') }})

{% set game_dict = {
    "MSR" :  'msr',
    "FHI" : 'fhi',
    "TCS" : 'tcs',
    "HSAH" : 'hsah',
    "HDB" : 'hdb'
    }-%}

{% set string_params = {
    'adsType': 'ads_type', 
    'pointcutID': 'ad_point_cut',
    'characterID': 'character_id',
    'gameMode': 'game_mode', 
    'deploymentEnvironment': 'deployment_environment',
    'modeOfPayment': 'mode_of_payment',
    'itemPurchaseID': 'item_purchase_id',
    'roundID': 'round_id',
    'context': 'context',
    'checker': 'checker',
    'page_referrer': 'page_referrer',
    'page_location': 'page_location'}
    -%}

{% set num_params = {
    'ga_session_id': 'session_id'
    , 'currentFPS': 'current_fps'
    , 'minFPS': 'min_fps'
    , 'maxFPS': 'max_fps'
    , 'finalPositionInActionPhase': 'game_rank'
    , 'coinEarnings': 'coin_earning'
    , 'crownEarnings': 'crown_earning'
    , 'killCount': 'kill_count'
    , 'humanPlayersInGame': 'human_count'
    , 'itemSpentValue': 'item_spent_value'
    , 'itemPurchaseValue': 'item_purchase_value'
    , 'roundTime': 'round_time'
    , 'finalPositionInActionPhase': 'round_rank'
    , 'isLoaded': 'is_loaded'
    , 'totalSec': 'total_sec'
    , 'itemCount': 'item_count'
    , 'totalRoundCount': 'total_round_count'}
    -%}

{% set user_props = {
    'gameVersion': 'game_version'
    , 'playerId': 'player_id'
    } -%}
, event as (

{% for game, address in game_dict.items() -%}

select
    cast(extract(date from timestamp_micros(event_timestamp)) as date) as date
    , cast(extract(datetime from timestamp_micros(event_timestamp)) as datetime) as date_time
    , event_name
    , device.operating_system_version	
    , {{"'"}}{{game}}{{"'"}} as game
    , geo.city
    , device.operating_system
    , device.web_info.browser
    , user_pseudo_id
    , geo.continent
    , device.category
    , device.web_info.hostname as host_name
    , geo.country
    , device.mobile_brand_name || ' ' || coalesce(device.mobile_marketing_name, device.mobile_model_name) as device_model
    , traffic_source.name
    , traffic_source.source 
    , traffic_source.medium
    {% for param, alias in string_params.items() -%}
    , {{ extract_string_param('"' + param + '"')}} as  {{alias}}
    {% endfor -%}

    {% for param, alias in num_params.items() -%}
    , {{ extract_number_param('"' + param + '"')}} as  {{alias}}
    {% endfor -%}

    {% for param, alias in user_props.items() -%}
    , {{ extract_user_props('"' + param + '"')}} as  {{alias}}
    {% endfor -%}

    
from {{address}}

{% if not loop.last %}
union all
{% endif %}
{% endfor%}

)

select 
* except (game_version)
, max(game_version) over (partition by user_pseudo_id, game, cast(session_id as string)) game_version
 from event