/*
 * This is an easy change - if there was no redirect but there's some mismatch in title/content type,
 * just trust the metadata collected directly from the title page.
 *
 * We may not want to do the same for redirected titles because e.g. for shows, the
 * titles.title might be the name of the season/episode rather than the
 * name of the show. Messing with that model could just exacerbate quality issues.
 */
update titles
set
    title = t2.extracted_title,
    content_type = t2.extracted_content_type::public.content_type
from (
    select
        t1.*,
        t1.extracted_title = t1.title as title_eq,
        t1.content_type::text = t1.extracted_content_type as content_type_eq
    from (
        select
            *,
            metadata -> 0 -> 'data' ->> 'title' as extracted_title,
            replace(
                json_extract_element_from_metadata(metadata, 'moreDetails') -> 'data' ->> 'type',
                'show',
                'series'
            ) as extracted_content_type
        from titles
        where metadata is not null
    ) as t1
) as t2
left join availability as a
    on
        t2.netflix_id = a.netflix_id
        and a.country = 'US'
where
    titles.id = t2.id
    and a.redirected_netflix_id is null
    and (
        not title_eq
        or not content_type_eq
    );

/*
    Titles that were reachable via a redirect whose redirected title ID is not present in titles
    should be inserted into titles to make life a little easier. This is especially necessary
    for when I'm storing ratings data since I have a FK constraint I would like to obey.

    NOTE `title` and `content_type` were seeded, hence the need to parse out the "updated" fields from metadata
    whereas `release_year` and `runtime` were backfilled via script and already went through this parsing process.
*/
with insertable as (
    select
        a.redirected_netflix_id as netflix_id,
        t1.metadata -> 0 -> 'data' ->> 'title' as title,
        replace(
            json_extract_element_from_metadata(
                t1.metadata,
                'moreDetails'
            )
            -> 'data'
            ->> 'type',
            'show',
            'series'
        )::public.content_type as content_type,
        min(t1.release_year) as release_year,
        min(t1.runtime) as runtime
    from availability as a
    inner join titles as t1
        on a.netflix_id = t1.netflix_id
    left join titles as t2
        on a.redirected_netflix_id = t2.netflix_id
    where
        -- Reachable titles with redirects where the redirect ID is not present in the titles table
        a.redirected_netflix_id is not null
        and a.titlepage_reachable
        and t2.id is null
    group by 1, 2, 3
)

insert into titles (netflix_id, title, content_type, release_year, runtime)
select
    netflix_id,
    title,
    content_type,
    release_year,
    runtime
from insertable;
