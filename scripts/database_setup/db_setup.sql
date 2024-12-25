create type content_type as enum ('movie',
'series');

create table titles(
id SERIAL primary key,
netflix_id bigint UNIQUE,
title varchar(256),
content_type content_type,
release_year int,
runtime int
);

create table availability(
id SERIAL primary key,
netflix_id bigint references titles(netflix_id),
redirected_netflix_id bigint references titles(netflix_id),
country char(2),
available boolean,
checked_at timestamp
);

-- WARNING: I loaded movies first, then ran the below to set content_type = 'movie',
update
	titles
set
	content_type = 'movie'
where
	content_type is null;

-- then I loaded series and ran the block below
update
	titles
set
	content_type = 'series'
where
	content_type is null;

alter table titles add column metadata jsonb;

create table ratings(
	id SERIAL primary key,
	netflix_id bigint references titles(netflix_id), 
	vendor varchar(32), 
	url text, 
	rating smallint, 
	ratings_count integer, 
	checked_at timestamp
);

ALTER TABLE ratings ADD CONSTRAINT unique_vendor_and_netflix_id UNIQUE (vendor, netflix_id);