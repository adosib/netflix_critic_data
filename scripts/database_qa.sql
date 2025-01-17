/*
    Titles should roughly match, and content type should probably definitely match
    TODO need to work in redirect logic bc e.g.
    the title for 80181965 is 'First Cup' which is fair since that's the name of the season
    with season ID 80181965. So, at least for shows, I can't really expect titles.title == metadata.title
    since metadata is gotten from the redirected title page and titles.title is from the seed file.
    ...I really wish I didn't have episodes or seasons in the titles table.
*/
select
    t2.id,
    t2.netflix_id,
    a.redirected_netflix_id,
    t2.title,
    t2.extracted_title,
    t2.title_eq,
    t2.content_type,
    t2.extracted_content_type,
    t2.content_type_eq
from (
    select
        t1.*,
        t1.extracted_title = t1.title as title_eq,
        t1.content_type::text = (case t1.extracted_content_type when 'show' then 'tv series' else t1.extracted_content_type end) as content_type_eq
    from (
        select
            *,
            metadata -> 0 -> 'data' ->> 'title' as extracted_title,
            json_extract_element_from_metadata(metadata, 'moreDetails') -> 'data' ->> 'type' as extracted_content_type
        from titles
        where metadata is not null
    ) as t1
) as t2
left join availability as a
    on
        t2.netflix_id = a.netflix_id
        and a.country = 'US'
where not t2.title_eq or not t2.content_type_eq;

/*
    EXPECT NO RECORDS IN RESULT SET:
    If the title is available, that must mean the titlepage is reachable
*/
select count(*)
from availability
where available = true and coalesce(titlepage_reachable, false) = false;

/*
    EXPECT NO RECORDS IN RESULT SET:
    Available titles should have metadata
*/
select count(*)
from titles
inner join availability as a
    on
        titles.netflix_id = a.netflix_id
        and a.country = 'US'
where
    a.available = true
    and titles.metadata is null;

/*
    EXPECT HANDFUL OF RECORDS IN RESULT SET:
    Unreachable titles *with* metadata (likely removed from Netflix between first and latest availability check)
*/
select count(*)
from titles
inner join availability as a
    on
        titles.netflix_id = a.netflix_id
        and a.country = 'US'
where
    a.titlepage_reachable = false
    and titles.metadata is not null;


/*
    EXPECT HANDFUL OF RECORDS IN RESULT SET:
    TODO These probably need to be reviewed
*/
select count(*) from titles
where title = 'SUPPLEMENTAL';


/*
    EXPECT HANDFUL OF RECORDS IN RESULT SET:
    Title pages not reachable but redirected from original ID:
    these titles are probably reachable with an authenticated session,
    but they're almost certainly not available
*/
select count(*) from availability
where
    redirected_netflix_id is not null
    and titlepage_reachable = false;

/*
    TODO in addition to verifying `content_type`, `release_year` and `runtime` should be checked against `metadata`, too.
    I did notice an issue for release_year where there are a few cases of 0 because the logic in _get_release_year
    in backfill_titles.py
*/
select *
from (
    select
        netflix_id,
        title,
        content_type,
        release_year,
        coalesce(
            (
                json_extract_element_from_metadata(metadata, 'seasonsAndEpisodes')
                -> 'data'
                -> 'seasons'
                -> 0
                -> 'episodes'
                -> 0
                ->> 'year'
            )::int,
            release_year
        ) as first_ep_release_year
    from titles
) as t
where release_year > first_ep_release_year;


/*
    EXPECT NO RECORDS IN RESULT SET:
    The goal with the below is validating a query for populate_ratings.py
    I don't want to waste SERP results on bad searches.
    The first step is removing duplicates - titles that redirect to some "parent" title.
    There could be 30 titles redirecting to one parent title e.g.

        select *
        from titles t
        join availability a
            on t.netflix_id = a.netflix_id
        where a.redirected_netflix_id = 80049872

    In the end, netflix_id should be UNIQUE. No duplicates.
    Titles, too, since we're looking up ratings by title.
 */
select
    netflix_id,
    count(*) as ct_duplicates
from (
    -- This subquery gets used in populate_ratings.py
    select distinct
        replace(
            json_extract_element_from_metadata(
                coalesce(t2.metadata, t.metadata),
                'moreDetails'
            )
            -> 'data'
            ->> 'type',
            'show',
            'tv series'
        )::public.content_type as content_type,
        coalesce(a.redirected_netflix_id, t.netflix_id) as netflix_id,
        coalesce(t2.metadata, t.metadata) -> 0 -> 'data' ->> 'title' as title,
        coalesce(t2.release_year, t.release_year) as release_year
    from availability as a
    inner join titles as t
        on a.netflix_id = t.netflix_id
    left join titles as t2
        on a.redirected_netflix_id = t2.netflix_id
    where
        a.country = 'US'
        and a.available = true
        and coalesce(
            coalesce(t2.metadata, t.metadata)
            -> 0
            -> 'data'
            -> 'details'
            -> 0
            -> 'data'
            -> 'coreGenre'
            ->> 'genreName', ''
        ) <> 'Special Interest'
) as dataset_for_ratings_search
group by 1
having count(*) > 1;

/*
    EXPECT HANDFUL OF RECORDS IN RESULT SET:
    Available titles for which a 'Google user' rating could not be obtained.

    The number of records ought to represent <5% of available titles.
*/
select count(*)
from titles
inner join availability as a
    on
        titles.netflix_id = a.netflix_id
        and a.country = 'US'
left join ratings
    on
        ratings.netflix_id = coalesce(a.redirected_netflix_id, a.netflix_id)
        and ratings.vendor = 'Google users'
where
    a.available = true
    and coalesce(
        titles.metadata
        -> 0
        -> 'data'
        -> 'details'
        -> 0
        -> 'data'
        -> 'coreGenre'
        ->> 'genreName', ''
    ) not in ('', 'Special Interest')
    and ratings.id is null
    and titles.metadata is not null;
