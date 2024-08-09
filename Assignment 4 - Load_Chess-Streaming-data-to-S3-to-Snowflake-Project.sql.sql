/* In this project we want to extract the three JSON objects "list", "stats" and "info" 
located in the S3 data lake "chess-bucket". To facilitate this transfer we have created 
an event trigger which allows the data to be stored in an SQS Queue which we must 
then use to transmit to the Data Warehouse. For this we have created a Snowpipe 
which copies data into a json table on Snowflake. From this json table we have 
created a table by recovering only the necessary columns which are:*/

use warehouse compute_wh;

create or replace schema demo_db.chess_schema;

-- create user in aws with programmatic access and creat external stage using access key and aws key id
create or replace stage demo_db.chess_schema.che_ext_json_a4
url='s3://aws-chess-a4/chess_data_a4/'  
credentials=(aws_key_id='akia2uc3dile54n7xvmk' 
aws_secret_key='r1ap8drgd8k9cxjet0vitrq7nqchjo8idpyw+pys');

desc pipe chess_stats_pipe;

create or replace file format demo_db.chess_schema.che_json_a4_file_format
type = 'json'
compression = 'auto'
strip_outer_array = true;

list @demo_db.chess_schema.che_ext_json_a4;

-- create chess_list table in snowflake 
    create or replace table chess_list (
     username string
    ,is_live boolean );

-- create chess_info table in snowflake     
    create or replace table demo_db.chess_schema.chess_info (
     username string 
    ,followers numeric 
    ,country varchar 
    ,joined date
    ,location varchar 
    ,name varchar 
    ,player_id varchar
    ,status varchar 
    ,title varchar
    ,primary_key numeric);

-- create chess_stats table in snowflake 
    create or replace table demo_db.chess_schema.chess_stats (
    last_blitz numeric 
    ,draw_blitz numeric
    ,loss_blitz numeric
    ,win_blitz numeric 
    ,last_bullet numeric 
    ,draw_bullet numeric 
    ,loss_bullet numeric 
    ,win_bullet numeric 
    ,last_rapid numeric 
    ,draw_rapid numeric
    ,loss_rapid numeric 
    ,win_rapid numeric 
    ,fide numeric
    ,primary_key numeric);
 
 
-- create a snowpipe for list data with auto ingest enabled
create or replace pipe chess_list_pipe 
auto_ingest = true as
copy into chess_list (username,is_live)
  from (
  select
  $1:username::string as username,
 $1:is_live::boolean as is_live
    from @demo_db.chess_schema.che_ext_json_a4/list_file.json)
      file_format = demo_db.chess_schema.che_json_a4_file_format;
      on_error = 'continue';

    
-- create a snowpipe for info data with auto ingest enabled
create or replace pipe chess_info_pipe 
auto_ingest = true as
copy into chess_info(username,followers,country,joined,location,name,player_id,
status,title,primary_key)
  from (
  select
    $1:username::string as username,
    $1:followers::numeric as followers,
    $1:country::varchar as country,
    $1:joined::date as joined,
    $1:location::varchar as location,
    $1:name::varchar as name,
    $1:player_id::varchar as player_id,
    $1:status::varchar as status,
    $1:title::varchar as title,
    $1:primary_key::numeric as primary_key
    from @demo_db.chess_schema.che_ext_json_a4/info_file.json)
      file_format= demo_db.chess_schema.che_json_a4_file_format
      on_error = 'continue';


-- create a snowpipe for stats data with auto ingest enabled
create or replace pipe chess_stats_pipe 
auto_ingest = true as
copy into chess_stats(last_blitz,draw_blitz,loss_blitz,win_blitz,last_bullet,draw_bullet,loss_bullet,win_bullet,last_rapid,draw_rapid,loss_rapid,win_rapid,fide,primary_key)
  from (
  select
    $1:last_blitz::numeric as last_blitz,
    $1:draw_blitz::numeric as draw_blit,
    $1:loss_blitz::numeric as loss_blitz,
    $1:win_blitz::numeric as win_blitz,
    $1:last_bullet::numeric as last_bullet,
    $1:draw_bullet::numeric as draw_bullet,
    $1:loss_bullet::numeric as loss_bullet,
    $1:win_bullet::numeric as win_bullet,
    $1:last_rapid::numeric as last_rapid,
    $1:draw_rapid::numeric as draw_rapid,
    $1:loss_rapidd::numeric as loss_rapid,
    $1:win_rapid::numeric as win_rapid,
    $1:fide::numeric as fide,
    $1:primary_key::numeric as primary_key
     from @demo_db.chess_schema.che_ext_json_a4/stats_table.json)    
      file_format=demo_db.chess_schema.che_json_a4_file_format
      on_error = 'continue';      
    

alter pipe chess_list_pipe refresh;
alter pipe chess_info_pipe refresh;
alter pipe chess_stats_pipe refresh;

select * from chess_list;
select * from chess_info;
select * from chess_stats;

-- • username of the best player by category (blitz, chess, bullet)
with rnk_players as (
    select 
        username,
        row_number() over (order by win_blitz desc) as blitz_rank,
        row_number() over (order by win_bullet desc) as bullet_rank,
        row_number() over (order by win_rapid desc) as rapid_rank
    from demo_db.chess_schema.chess_stats
)
select 
    username,
    case 
        when blitz_rank = 1 then 'best blitz player'
        when bullet_rank = 1 then 'best bullet player'
        when rapid_rank = 1 then 'best rapid player'
    end as category
from rnk_players
where blitz_rank = 1 or bullet_rank = 1 or rapid_rank = 1;



-- • full name (or username if null) of the best player and his fide elo
select coalesce(c.username) as best_player, s.fide
from demo_db.chess_schema.chess_info c
join demo_db.chess_schema.chess_stats s
on c.primary_key = s.primary_key
order by s.fide desc
limit 1;

-- • average elo of premium, staff and basic players
select i.status, avg(i.fide) as average_elo
from demo_db.chess_schema.chess_info c
join demo_db.chess_schema.chess_stats s
on c.primary_key = s.primary_key
where i.status in ('premium', 'staff', 'basic')
group by i.status;

-- • number of professional players and their elo
select count(*) as num_professional_players, 
       avg(i.fide) as average_elo
from demo_db.chess_schema.chess_info c
join demo_db.chess_schema.chess_stats s
on c.primary_key = s.primary_key;

-- • average fide elo by their professional fide elo
select title, avg(fide) as average_elo
from demo_db.chess_schema.chess_info c
join demo_db.chess_schema.chess_stats s
on c.primary_key = s.primary_key
group by title;

-- • best player currently on live
select c.username, s.fide
from demo_db.chess_schema.chess_info c
join demo_db.chess_schema.chess_stats s
on c.primary_key = s.primary_key
join chess_list l
on c.username = l.username
where l.is_live = true
order by s.fide desc
limit 1;







