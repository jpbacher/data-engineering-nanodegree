CREATE TABLE IF NOT EXISTS public.artists (
    artistid varchar(256) NOT NULL,
    name varchar(256),
    location varchar(256),
    latitude numeric(18,0),
    longitude numeric(18,0)
);

CREATE TABLE IF NOT EXISTS public.songplays (
    playid varchar(32) NOT NULL,
    start_time timestamp NOT NULL,
    userid int4 NOT NULL,
    'level' varchar(256),
    songid varchar(256),
    artistid varchar(256),
    sessionid int4,
    location varchar(256),
    user_agent varchar(256),
    CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);
