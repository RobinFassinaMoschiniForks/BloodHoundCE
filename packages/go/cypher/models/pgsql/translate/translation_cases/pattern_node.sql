-- case: match (s) return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0)
select n0.n0 as s
from n0;

-- case: match (n:NodeKind1), (e) where n.name = e.name return n
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.kind_ids operator (pg_catalog.&&) array [1]::int2[]),
     n1 as (select n0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
            from n0,
                 node n1
            where (n0.n0).properties -> 'name' = n1.properties -> 'name')
select n1.n0 as n
from n1;

-- case: match (s), (e) where id(s) in e.captured_ids return s, e
--
-- This is a little weird for us since JSONB arrays are basically type any[] which requires a special form of
-- type negotiation in PgSQL
--
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0),
     n1 as (select n0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
            from n0,
                 node n1
            where (n0.n0).id = any ((array (select jsonb_array_elements_text(n1.properties -> 'captured_ids')))::int4[]))
select n1.n0 as s, n1.n1 as e
from n1;

-- case: match (s) where s:NodeKind1 and s:NodeKind2 return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.kind_ids operator (pg_catalog.&&) array [1]::int2[]
              and n0.kind_ids operator (pg_catalog.&&) array [2]::int2[])
select n0.n0 as s
from n0;

-- case: match (s) where s.name = '1234' return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' = '1234')
select n0.n0 as s
from n0;

-- case: match (s:NodeKind1), (e:NodeKind2) where s.selected or s.tid = e.tid and e.enabled return s, e
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.kind_ids operator (pg_catalog.&&) array [1]::int2[]),
     n1 as (select n0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
            from n0,
                 node n1
            where n1.kind_ids operator (pg_catalog.&&) array [2]::int2[] and ((n0.n0).properties -> 'selected')::bool
               or (n0.n0).properties -> 'tid' = n1.properties -> 'tid' and (n1.properties -> 'enabled')::bool)
select n1.n0 as s, n1.n1 as e
from n1;

-- case: match (s) where s.value + 2 / 3 > 10 return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where (n0.properties -> 'value')::int8 + 2 / 3 > 10)
select n0.n0 as s
from n0;

-- case: match (s), (e) where s.name = 'n1' return s, e.name as othername
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' = 'n1'),
     n1 as (select n0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
            from n0,
                 node n1)
select n1.n0 as s, (n1.n1).properties -> 'name' as othername
from n1;

-- case: match (s) where s.name in ['option 1', 'option 2'] return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' = any (array ['option 1', 'option 2']::text[]))
select n0.n0 as s
from n0;

-- case: match (s) where toLower(s.name) = '1234' return distinct s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where lower(n0.properties ->> 'name') = '1234')
select n0.n0 as s
from n0;

-- case: match (s:NodeKind1), (e:NodeKind2) where s.name = e.name return s, e
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.kind_ids operator (pg_catalog.&&) array [1]::int2[]),
     n1 as (select n0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
            from n0,
                 node n1
            where n1.kind_ids operator (pg_catalog.&&) array [2]::int2[]
              and (n0.n0).properties -> 'name' = n1.properties -> 'name')
select n1.n0 as s, n1.n1 as e
from n1;

-- case: match (s), (e) where s.name = '1234' and e.other = 1234 return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' = '1234'),
     n1 as (select n0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
            from n0,
                 node n1
            where (n1.properties -> 'other')::int8 = 1234)
select n1.n0 as s
from n1;

-- case: match (s), (e) where s.name = '1234' or e.other = 1234 return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0),
     n1 as (select n0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
            from n0,
                 node n1
            where (n0.n0).properties ->> 'name' = '1234'
               or (n1.properties -> 'other')::int8 = 1234)
select n1.n0 as s
from n1;

-- case: match (n), (k) where n.name = '1234' and k.name = '1234' match (e) where e.name = n.name return k, e
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' = '1234'),
     n1 as (select n0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
            from n0,
                 node n1
            where n1.properties ->> 'name' = '1234'),
     n2 as (select n1.n0 as n0, n1.n1 as n1, (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n2
            from n1,
                 node n2
            where n2.properties -> 'name' = (n1.n0).properties -> 'name')
select n2.n1 as k, n2.n2 as e
from n2;

-- case: match (n) return n skip 5 limit 10
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0)
select n0.n0 as n
from n0
offset 5 limit 10;

-- case: match (s) return s order by s.name, s.other_prop desc
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0)
select n0.n0 as s
from n0
order by (n0.n0).properties -> 'name', (n0.n0).properties -> 'other_prop' desc;

-- case: match (s) where s.created_at = localtime() return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where (n0.properties ->> 'created_at')::time without time zone = localtime(6))
select n0.n0 as s
from n0;

-- case: match (s) where s.created_at = localtime('12:12:12') return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where (n0.properties ->> 'created_at')::time without time zone = ('12:12:12')::time without time zone)
select n0.n0 as s
from n0;

-- case: match (s) where s.created_at = date() return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where (n0.properties ->> 'created_at')::date = current_date)
select n0.n0 as s
from n0;

-- case: match (s) where s.created_at = date('2023-12-12') return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where (n0.properties ->> 'created_at')::date = ('2023-12-12')::date)
select n0.n0 as s
from n0;

-- case: match (s) where s.created_at = datetime() return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where (n0.properties ->> 'created_at')::timestamp with time zone = now())
select n0.n0 as s
from n0;

-- case: match (s) where s.created_at = datetime('2019-06-01T18:40:32.142+0100') return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where (n0.properties ->> 'created_at')::timestamp with time zone =
                  ('2019-06-01T18:40:32.142+0100')::timestamp with time zone)
select n0.n0 as s
from n0;

-- case: match (s) where s.created_at = localdatetime() return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where (n0.properties ->> 'created_at')::timestamp without time zone = localtimestamp(6))
select n0.n0 as s
from n0;

-- case: match (s) where s.created_at = localdatetime('2019-06-01T18:40:32.142') return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where (n0.properties ->> 'created_at')::timestamp without time zone =
                  ('2019-06-01T18:40:32.142')::timestamp without time zone)
select n0.n0 as s
from n0;

-- case: match (s) where not (s.name = '123') return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where not (n0.properties ->> 'name' = '123'))
select n0.n0 as s
from n0;

-- case: match (s) return s.value + 1
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0)
select ((n0.n0).properties -> 'value')::int8 + 1
from n0;

-- case: match (s) return (s.value + 1) / 3
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0)
select (((n0.n0).properties -> 'value')::int8 + 1) / 3
from n0;

-- case: match (s) where id(s) in [1, 2, 3, 4] return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.id = any (array [1, 2, 3, 4]::int8[]))
select n0.n0 as s
from n0;

-- case: match (s) where s.name in ['option 1', 'option 2'] return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' = any (array ['option 1', 'option 2']::text[]))
select n0.n0 as s
from n0;

-- case: match (s) where s.created_at is null return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where not n0.properties ? 'created_at')
select n0.n0 as s
from n0;

-- case: match (s) where s.created_at is not null return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ? 'created_at')
select n0.n0 as s
from n0;

-- case: match (s) where s.name starts with '123' return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' ~~ '123%')
select n0.n0 as s
from n0;

-- case: match (s) where s.name contains '123' return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' ~~ '%123%')
select n0.n0 as s
from n0;

-- case: match (s) where s.name ends with '123' return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' ~~ '%123')
select n0.n0 as s
from n0;

-- case: match (s) where s.name starts with s.other return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' ~~ ((n0.properties ->> 'other') || '%')::text)
select n0.n0 as s
from n0;

-- case: match (s) where s.name contains s.other return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' ~~ ('%' || (n0.properties ->> 'other') || '%')::text)
select n0.n0 as s
from n0;

-- case: match (s) where s.name ends with s.other return s
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where n0.properties ->> 'name' ~~ ('%' || (n0.properties ->> 'other'))::text)
select n0.n0 as s
from n0;
