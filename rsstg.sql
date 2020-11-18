create table rsstg_updates (owner integer, update jsonb);

create unique index rsstg_updates__id on rsstg_updates(update->>'update_id');

-- create table rsstg_users (id integer);
create table rsstg_channel (
	channel_id bigint primary key,
	username text not null);
create unique index rsstg_channel__username on rsstg_channel(username);

create table rsstg_source (
	source_id serial,
	channel_id integer not null,
	url text not null,
	last_scrape not null timestamptz default now(),
	enabled boolean not null default false,
	iv_hash text,
	owner bigint not null);
create unique index rsstg_source__source_id on rsstg_source(source_id);
create unique index rsstg_source__channel_id__owner on rsstg_source(channel_id, owner);
create index rsstg_source__owner on rsstg_source(owner);

create table rsstg_post (
	source_id integer not null,
	date int not null,
	url text not null,
	hour smallint not null generated always as (extract('hour' from  posted)) stored,
	FOREIGN KEY (source_id) REFERENCES rsstg_source(source_id)
);
create unique index rsstg_post__url on rsstg_post(url);
create index rsstg_post__hour on rsstg_post(hour);
create index rsstg_post__posted_hour on rsstg_post(posted,hour);

create or replace view rsstg_order as
	select source_id, coalesce(last_scrape + make_interval(0,0,0,0,0,(60 / (coalesce(activity, 1)/7 + 1) )::integer), now() - interval '1 minute') as next_fetch
		from rsstg_source natural left join
		(select source_id, count(*) as activity
			from rsstg_post where 
				hour = extract('hour' from now())
				and posted > now() - interval '7 days'
			group by source_id) as act
		where enabled
		order by next_fetch;
