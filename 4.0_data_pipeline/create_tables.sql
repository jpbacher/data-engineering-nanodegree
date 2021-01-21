CREATE TABLE IF NOT EXISTS public.artists (
    artistid varchar(256) NOT NULL,
    name varchar(256),
    location varchar(256),
    latitude numeric(18,0),
    longitude numeric(18,0)
);
