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

CREATE TABLE IF NOT EXISTS public.songs (
    songid varchar(256) NOT NULL,
    title varchar(256),
    artistid varchar(256),
    'year' int4,
    duration numeric(18,0),
    CONSTRAINT song_primkey PRIMARY KEY (songid)
);

CREATE TABLE IF NOT EXISTS public.staging_events (
    artist varchar(256),
    auth varchar(256),
    firstname varchar(256),
    gender varchar(256),
    iteminsession int4,
    lastname varchar(256),
    length numeric(18,0),
    'level' varchar(256),
    location varchar(256),
    'method' varchar(256),
    page varchar(256),
    registration numeric(18,0),
    sessionid int4,
    song varchar(256),
    status int4,
    ts int8,
    useragent varchar(256),
    userid int4
);
