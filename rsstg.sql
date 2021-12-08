create table rsstg_updates (owner integer, update jsonb);

create unique index rsstg_updates__id on rsstg_updates(update->>'update_id');

create table rsstg_source (
	source_id serial,
	channel text not null,
	channel_id integer not null,
	url text not null,
	last_scrape not null timestamptz default now(),
	enabled boolean not null default true,
	iv_hash text,
	owner bigint not null);
create unique index rsstg_source__source_id on rsstg_source(source_id);
create unique index rsstg_source__channel_id__owner on rsstg_source(channel_id, owner);
create index rsstg_source__owner on rsstg_source(owner);

create table rsstg_post (
	source_id integer not null,
	posted timestamptz not null,
	url text not null,
	hour smallint not null generated always as (extract('hour' from posted at time zone 'utc')) stored,
	hxm smallint not null generated always as (hxm(posted)) stored,
	FOREIGN KEY (source_id) REFERENCES rsstg_source(source_id) on delete cascade,
);
create unique index rsstg_post__url on rsstg_post(url);
create index rsstg_post__hour on rsstg_post(hour);
create index rsstg_post__posted_hour on rsstg_post(posted,hour);
create index rsstg_post__hxm on rsstg_post(hxm);
create index rsstg_post__posted_hxm on rsstg_post(posted,hxm);

create or replace view rsstg_order_old as
	select source_id, coalesce(last_scrape + make_interval(0,0,0,0,0,(60 / (coalesce(activity, 1)/7 + 1) )::integer), now() - interval '1 minute') as next_fetch, owner
		from rsstg_source natural left join
		(select source_id, count(*) as activity
			from rsstg_post where 
				hour = extract('hour' from now())::smallint
				and posted > now() - interval '7 days'
				and posted < now() - interval '1 hour'
			group by source_id) as act
		where enabled
		order by next_fetch;

create or replace function hxm(timestamptz) returns smallint
	as $$ select(extract('hour' from $1) * extract('minute' from $1)); $$
	language sql immutable returns null on null input;

create or replace view rsstg_order as
	select source_id, coalesce(last_scrape + make_interval(0,0,0,0,0,(60 / (coalesce(activity, 1)/7 + 1) )::integer), now() - interval '1 minute') as next_fetch, owner
		from rsstg_source natural left join
		(select source_id, count(*) as activity
			from rsstg_post where 
			  (
					(hxm > hxm(now()) - 30 and hxm < hxm(now()) + 30)
					or (hxm < 30 and hxm < hxm(now()) + 30 - 1440)
					or (hxm > 1410 and hxm > 1440 + hxm(now()) - 30)
			  )
				and posted < now() - interval '1 hour'
				and posted > now() - interval '7 days'
			group by source_id) as act
		where enabled
		order by next_fetch;
