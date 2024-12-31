-- View records for titles that became available
select
    audit.*,
    titles.*,
    a.redirected_netflix_id
from audit
inner join availability as a
    on audit.record_id = a.id
inner join titles
    on a.netflix_id = titles.netflix_id
where
    audit.field = 'available'
    and coalesce(audit.old_value, 'false') = 'false'
    and audit.new_value = 'true'
order by audit.changed_at desc;

-- 'Special Interest' genre - seems to be mostly for internal tests. Surprised this is publicly reachable.
select *
from (
    select
        *,
        metadata -> 0 -> 'data' -> 'details' -> 0 -> 'data' -> 'coreGenre' ->> 'genreName' as genre
    from titles
    where metadata is not null
) as t
where genre = 'Special Interest'
