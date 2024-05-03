-- case: match ()-[r]->() return r
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.id = e0.start_id
                   join node n1 on n1.id = e0.end_id)
select e0.e0 as r
from e0;

-- case: match (n), ()-[r]->() return n, r
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0),
     e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   n0.n0                                                                     as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1,
                   (n2.id, n2.kind_ids, n2.properties)::nodecomposite                        as n2
            from n0,
                 edge e0
                   join node n1 on n1.id = e0.start_id
                   join node n2 on n2.id = e0.end_id)
select e0.n0 as n, e0.e0 as r
from e0;

-- todo: The cypher parser inserts a `r != e` condition to the final projection, as such, with the
--       basic harness this query in SQL returns 9 results.
-- case: match ()-[r]->(), ()-[e]->() return r, e
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.id = e0.start_id
                   join node n1 on n1.id = e0.end_id),
     e1 as (select e0.e0                                                                     as e0,
                   (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1,
                   e0.n0                                                                     as n0,
                   e0.n1                                                                     as n1,
                   (n2.id, n2.kind_ids, n2.properties)::nodecomposite                        as n2,
                   (n3.id, n3.kind_ids, n3.properties)::nodecomposite                        as n3
            from e0,
                 edge e1
                   join node n2 on n2.id = e1.start_id
                   join node n3 on n3.id = e1.end_id)
select e1.e0 as r, e1.e1 as e
from e1;

-- case: match ()-[r]->() where r.value = 42 return r
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.id = e0.start_id
                   join node n1 on n1.id = e0.end_id
            where (e0.properties -> 'value')::int8 = 42)
select e0.e0 as r
from e0;

-- todo: match (s) where ()-[]->(s) return s
with ex0 as (with n0 as (select n0.* from node n0),
                  e0 as (select e0.*
                         from edge e0,
                              n0
                         where n0.id = e0.start_id),
                  n1 as (select n1.*
                         from node n1,
                              e0
                         where n1.id = e0.end_id)
             select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as r,
                    (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as s
             from e0,
                  n0)
select *
from ex0;

-- todo: match ()-[r]->() where ({name: 'test'})-[r]->() return r
;

-- todo: with 1 as a match (n) where n.name = a with n match (r) where r.tag_id = n.tag return r
;

-- case: match (n)-[r]->() where n.name = '123' return n, r
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.properties ->> 'name' = '123' and n0.id = e0.start_id
                   join node n1 on n1.id = e0.end_id)
select e0.n0 as n, e0.e0 as r
from e0;

-- case: match (s)-[r]->(e) where s.name = '123' and e.name = '321' return s, r, e
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.properties ->> 'name' = '123' and n0.id = e0.start_id
                   join node n1 on n1.properties ->> 'name' = '321' and n1.id = e0.end_id)
select e0.n0 as s, e0.e0 as r, e0.n1 as e
from e0;

-- case: match (f), (s)-[r]->(e) where not f.bool_field and s.name = '123' and e.name = '321' return f, s, r, e
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0
            from node n0
            where not (n0.properties -> 'bool_field')::bool),
     e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   n0.n0                                                                     as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1,
                   (n2.id, n2.kind_ids, n2.properties)::nodecomposite                        as n2
            from n0,
                 edge e0
                   join node n1 on n1.properties ->> 'name' = '123' and n1.id = e0.start_id
                   join node n2 on n2.properties ->> 'name' = '321' and n2.id = e0.end_id)
select e0.n0 as f, e0.n1 as s, e0.e0 as r, e0.n2 as e
from e0;

-- case: match ()-[e0]->(n)<-[e1]-() return e0, n, e1
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.id = e0.start_id
                   join node n1 on n1.id = e0.end_id),
     e1 as (select e0.e0                                                                     as e0,
                   (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1,
                   e0.n0                                                                     as n0,
                   e0.n1                                                                     as n1,
                   (n2.id, n2.kind_ids, n2.properties)::nodecomposite                        as n2
            from e0,
                 edge e1
                   join node n2 on n2.id = e1.start_id
            where (e0.e0).start_id = e1.end_id)
select e1.e0 as e0, e1.n1 as n, e1.e1 as e1
from e1;

-- case: match ()-[e0]->(n)-[e1]->() return e0, n, e1
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.id = e0.start_id
                   join node n1 on n1.id = e0.end_id),
     e1 as (select e0.e0                                                                     as e0,
                   (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1,
                   e0.n0                                                                     as n0,
                   e0.n1                                                                     as n1,
                   (n2.id, n2.kind_ids, n2.properties)::nodecomposite                        as n2
            from e0,
                 edge e1
                   join node n2 on n2.id = e1.end_id
            where (e0.e0).end_id = e1.start_id)
select e1.e0 as e0, e1.n1 as n, e1.e1 as e1
from e1;

-- case: match ()<-[e0]-(n)<-[e1]-() return e0, n, e1
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.id = e0.end_id
                   join node n1 on n1.id = e0.start_id),
     e1 as (select e0.e0                                                                     as e0,
                   (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1,
                   e0.n0                                                                     as n0,
                   e0.n1                                                                     as n1,
                   (n2.id, n2.kind_ids, n2.properties)::nodecomposite                        as n2
            from e0,
                 edge e1
                   join node n2 on n2.id = e1.start_id
            where (e0.e0).start_id = e1.end_id)
select e1.e0 as e0, e1.n1 as n, e1.e1 as e1
from e1;

-- case: match (s)<-[r:EdgeKind1|EdgeKind2]-(e) return s.name, e.name
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.id = e0.end_id
                   join node n1 on n1.id = e0.start_id
            where e0.kind_id = any (array [11, 12]::int2[]))
select (e0.n0).properties -> 'name', (e0.n1).properties -> 'name'
from e0;

-- case: match (s)-[:EdgeKind1|EdgeKind2]->(e)-[:EdgeKind1]->() return s.name as s_name, e.name as e_name
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.id = e0.start_id
                   join node n1 on n1.id = e0.end_id
            where e0.kind_id = any (array [11, 12]::int2[])),
     e1 as (select e0.e0                                                                     as e0,
                   (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1,
                   e0.n0                                                                     as n0,
                   e0.n1                                                                     as n1,
                   (n2.id, n2.kind_ids, n2.properties)::nodecomposite                        as n2
            from e0,
                 edge e1
                   join node n2 on n2.id = e1.end_id
            where (e0.e0).end_id = e1.start_id
              and e1.kind_id = any (array [11]::int2[]))
select (e1.n0).properties -> 'name' as s_name, (e1.n1).properties -> 'name' as e_name
from e1;

-- case: match (s:NodeKind1)-[r:EdgeKind1|EdgeKind2]->(e:NodeKind2) return s.name, e.name
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.kind_ids operator (pg_catalog.&&) array [1]::int2[] and n0.id = e0.start_id
                   join node n1 on n1.kind_ids operator (pg_catalog.&&) array [2]::int2[] and n1.id = e0.end_id
            where e0.kind_id = any (array [11, 12]::int2[]))
select (e0.n0).properties -> 'name', (e0.n1).properties -> 'name'
from e0;
