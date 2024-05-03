-- case: match (n)-[*..]->(e) return n, e
with ex0 as (with recursive ex0(root_id, next_id, depth, satisfied, is_cycle, path) as (select e0.start_id,
                                                                                               e0.end_id,
                                                                                               1,
                                                                                               false,
                                                                                               e0.start_id = e0.end_id,
                                                                                               array [e0.id]
                                                                                        from edge e0
                                                                                               join node n0 on n0.id = e0.start_id
                                                                                               join node n1 on n1.id = e0.end_id
                                                                                        union
                                                                                        select ex0.root_id,
                                                                                               e0.end_id,
                                                                                               ex0.depth + 1,
                                                                                               false,
                                                                                               e0.id = any (ex0.path),
                                                                                               ex0.path || e0.id
                                                                                        from ex0
                                                                                               join edge e0 on e0.start_id = ex0.next_id
                                                                                               join node n1 on n1.id = e0.end_id)
             select (select array_agg((e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite)
                     from edge e0
                     where e0.id = any (ex0.path))                     as e0,
                    (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0,
                    (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
             from ex0
                    join edge e0 on e0.id = any (ex0.path)
                    join node n0 on n0.id = ex0.root_id
                    join node n1 on e0.id = ex0.path[array_length(ex0.path, 1)] and n1.id = e0.end_id)
select ex0.n0 as n, ex0.n1 as e
from ex0;

-- case: match (n)-[*..]->(e:NodeKind1) where n.name = '1234' return e
with ex0 as (with recursive ex0(root_id, next_id, depth, satisfied, is_cycle, path) as (select e0.start_id,
                                                                                               e0.end_id,
                                                                                               1,
                                                                                               false,
                                                                                               e0.start_id = e0.end_id,
                                                                                               array [e0.id]
                                                                                        from edge e0
                                                                                               join node n0 on n0.properties ->> 'name' = '1234' and n0.id = e0.start_id
                                                                                               join node n1 on
                                                                                          n1.kind_ids operator (pg_catalog.&&)
                                                                                          array [1]::int2[] and
                                                                                          n1.id = e0.end_id
                                                                                        union
                                                                                        select ex0.root_id,
                                                                                               e0.end_id,
                                                                                               ex0.depth + 1,
                                                                                               n1.kind_ids operator (pg_catalog.&&) array [1]::int2[],
                                                                                               e0.id = any (ex0.path),
                                                                                               ex0.path || e0.id
                                                                                        from ex0
                                                                                               join edge e0 on e0.start_id = ex0.next_id
                                                                                               join node n1 on
                                                                                          n1.kind_ids operator (pg_catalog.&&)
                                                                                          array [1]::int2[] and
                                                                                          n1.id = e0.end_id)
             select (select array_agg((e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite)
                     from edge e0
                     where e0.id = any (ex0.path))                     as e0,
                    (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0,
                    (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
             from ex0
                    join edge e0 on e0.id = any (ex0.path)
                    join node n0 on n0.id = ex0.root_id
                    join node n1 on e0.id = ex0.path[array_length(ex0.path, 1)] and n1.id = e0.end_id)
select ex0.n1 as e
from ex0;

-- todo: cypher expects the right hand binding for `r` to already be a list of relationships which seems strange
-- case: match (n)-[r*..]->(e:NodeKind1) where n.name = 'n1' and r.prop = 'a' return e
with ex0 as (with recursive ex0(root_id, next_id, depth, satisfied, is_cycle, path) as (select e0.start_id,
                                                                                               e0.end_id,
                                                                                               1,
                                                                                               false,
                                                                                               e0.start_id = e0.end_id,
                                                                                               array [e0.id]
                                                                                        from edge e0
                                                                                               join node n0 on n0.properties ->> 'name' = 'n1' and n0.id = e0.start_id
                                                                                               join node n1 on
                                                                                          n1.kind_ids operator (pg_catalog.&&)
                                                                                          array [1]::int2[] and
                                                                                          n1.id = e0.end_id
                                                                                        where e0.properties ->> 'prop' = 'a'
                                                                                        union
                                                                                        select ex0.root_id,
                                                                                               e0.end_id,
                                                                                               ex0.depth + 1,
                                                                                               n1.kind_ids operator (pg_catalog.&&) array [1]::int2[],
                                                                                               e0.id = any (ex0.path),
                                                                                               ex0.path || e0.id
                                                                                        from ex0
                                                                                               join edge e0 on e0.start_id = ex0.next_id
                                                                                               join node n1 on
                                                                                          n1.kind_ids operator (pg_catalog.&&)
                                                                                          array [1]::int2[] and
                                                                                          n1.id = e0.end_id
                                                                                        where e0.properties ->> 'prop' = 'a')
             select (select array_agg((e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite)
                     from edge e0
                     where e0.id = any (ex0.path))                     as e0,
                    (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0,
                    (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
             from ex0
                    join edge e0 on e0.id = any (ex0.path)
                    join node n0 on n0.id = ex0.root_id
                    join node n1 on e0.id = ex0.path[array_length(ex0.path, 1)] and n1.id = e0.end_id)
select ex0.n1 as e
from ex0;

-- case: match (n)-[*..]->(e:NodeKind1) where n.name = 'n2' return n
with ex0 as (with recursive ex0(root_id, next_id, depth, satisfied, is_cycle, path) as (select e0.start_id,
                                                                                               e0.end_id,
                                                                                               1,
                                                                                               false,
                                                                                               e0.start_id = e0.end_id,
                                                                                               array [e0.id]
                                                                                        from edge e0
                                                                                               join node n0 on n0.properties ->> 'name' = 'n2' and n0.id = e0.start_id
                                                                                               join node n1 on
                                                                                          n1.kind_ids operator (pg_catalog.&&)
                                                                                          array [1]::int2[] and
                                                                                          n1.id = e0.end_id
                                                                                        union
                                                                                        select ex0.root_id,
                                                                                               e0.end_id,
                                                                                               ex0.depth + 1,
                                                                                               n1.kind_ids operator (pg_catalog.&&) array [1]::int2[],
                                                                                               e0.id = any (ex0.path),
                                                                                               ex0.path || e0.id
                                                                                        from ex0
                                                                                               join edge e0 on e0.start_id = ex0.next_id
                                                                                               join node n1 on
                                                                                          n1.kind_ids operator (pg_catalog.&&)
                                                                                          array [1]::int2[] and
                                                                                          n1.id = e0.end_id)
             select (select array_agg((e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite)
                     from edge e0
                     where e0.id = any (ex0.path))                     as e0,
                    (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0,
                    (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
             from ex0
                    join edge e0 on e0.id = any (ex0.path)
                    join node n0 on n0.id = ex0.root_id
                    join node n1 on e0.id = ex0.path[array_length(ex0.path, 1)] and n1.id = e0.end_id)
select ex0.n0 as n
from ex0;

-- case: match (n)-[*..]->(e:NodeKind1)-[]->(l) where n.name = 'n1' return l
with ex0 as (with recursive ex0(root_id, next_id, depth, satisfied, is_cycle, path) as (select e0.start_id,
                                                                                               e0.end_id,
                                                                                               1,
                                                                                               false,
                                                                                               e0.start_id = e0.end_id,
                                                                                               array [e0.id]
                                                                                        from edge e0
                                                                                               join node n0 on n0.properties ->> 'name' = 'n1' and n0.id = e0.start_id
                                                                                               join node n1 on
                                                                                          n1.kind_ids operator (pg_catalog.&&)
                                                                                          array [1]::int2[] and
                                                                                          n1.id = e0.end_id
                                                                                        union
                                                                                        select ex0.root_id,
                                                                                               e0.end_id,
                                                                                               ex0.depth + 1,
                                                                                               n1.kind_ids operator (pg_catalog.&&) array [1]::int2[],
                                                                                               e0.id = any (ex0.path),
                                                                                               ex0.path || e0.id
                                                                                        from ex0
                                                                                               join edge e0 on e0.start_id = ex0.next_id
                                                                                               join node n1 on
                                                                                          n1.kind_ids operator (pg_catalog.&&)
                                                                                          array [1]::int2[] and
                                                                                          n1.id = e0.end_id)
             select (select array_agg((e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite)
                     from edge e0
                     where e0.id = any (ex0.path))                     as e0,
                    (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0,
                    (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
             from ex0
                    join edge e0 on e0.id = any (ex0.path)
                    join node n0 on n0.id = ex0.root_id
                    join node n1 on e0.id = ex0.path[array_length(ex0.path, 1)] and n1.id = e0.end_id),
     e1 as (select ex0.e0                                                                    as e0,
                   (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1,
                   ex0.n0                                                                    as n0,
                   ex0.n1                                                                    as n1,
                   (n2.id, n2.kind_ids, n2.properties)::nodecomposite                        as n2
            from ex0,
                 edge e1
                   join node n2 on n2.id = e1.end_id
            where e0[array_length(e0, 1)].end_id = e1.start_id)
select e1.n2 as l
from e1;

-- case: match (n)-[*..]->(e)-[:EdgeKind1|EdgeKind2]->()-[*..]->(l) where n.name = 'n1' and e.name = 'n2' return l
with ex0 as (with recursive ex0(root_id, next_id, depth, satisfied, is_cycle, path) as (select e0.start_id,
                                                                                               e0.end_id,
                                                                                               1,
                                                                                               false,
                                                                                               e0.start_id = e0.end_id,
                                                                                               array [e0.id]
                                                                                        from edge e0
                                                                                               join node n0 on n0.properties ->> 'name' = 'n1' and n0.id = e0.start_id
                                                                                               join node n1 on n1.properties ->> 'name' = 'n2' and n1.id = e0.end_id
                                                                                        union
                                                                                        select ex0.root_id,
                                                                                               e0.end_id,
                                                                                               ex0.depth + 1,
                                                                                               n1.properties ->> 'name' = 'n2',
                                                                                               e0.id = any (ex0.path),
                                                                                               ex0.path || e0.id
                                                                                        from ex0
                                                                                               join edge e0 on e0.start_id = ex0.next_id
                                                                                               join node n1 on n1.properties ->> 'name' = 'n2' and n1.id = e0.end_id)
             select (select array_agg((e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite)
                     from edge e0
                     where e0.id = any (ex0.path))                     as e0,
                    (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0,
                    (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
             from ex0
                    join edge e0 on e0.id = any (ex0.path)
                    join node n0 on n0.id = ex0.root_id
                    join node n1 on e0.id = ex0.path[array_length(ex0.path, 1)] and n1.id = e0.end_id),
     e1 as (select ex0.e0                                                                    as e0,
                   (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1,
                   ex0.n0                                                                    as n0,
                   ex0.n1                                                                    as n1,
                   (n2.id, n2.kind_ids, n2.properties)::nodecomposite                        as n2
            from ex0,
                 edge e1
                   join node n2 on n2.id = e1.end_id
            where e1.kind_id = any (array [11, 12]::int2[])
              and e0[array_length(e0, 1)].end_id = e1.start_id),
     ex1 as (with recursive ex1(root_id, next_id, depth, satisfied, is_cycle, path) as (select e2.start_id,
                                                                                               e2.end_id,
                                                                                               1,
                                                                                               false,
                                                                                               e2.start_id = e2.end_id,
                                                                                               array [e2.id]
                                                                                        from e1
                                                                                               join edge e2 on (e1.e1).end_id = e2.start_id
                                                                                               join node n3 on n3.id = e2.end_id
                                                                                        union
                                                                                        select ex1.root_id,
                                                                                               e2.end_id,
                                                                                               ex1.depth + 1,
                                                                                               false,
                                                                                               e2.id = any (ex1.path),
                                                                                               ex1.path || e2.id
                                                                                        from ex1
                                                                                               join edge e2 on e2.start_id = ex1.next_id
                                                                                               join node n3 on n3.id = e2.end_id
                                                                                        where not ex1.is_cycle
                                                                                          and not ex1.satisfied)
             select e1.e0                                              as e0,
                    e1.e1                                              as e1,
                    (select array_agg((e2.id, e2.start_id, e2.end_id, e2.kind_id, e2.properties)::edgecomposite)
                     from edge e2
                     where e2.id = any (ex1.path))                     as e2,
                    e1.n0                                              as n0,
                    e1.n1                                              as n1,
                    e1.n2                                              as n2,
                    (n3.id, n3.kind_ids, n3.properties)::nodecomposite as n3
             from e1,
                  ex1
                    join edge e2 on e2.id = any (ex1.path)
                    join node n2 on n2.id = ex1.root_id
                    join node n3 on e2.id = ex1.path[array_length(ex1.path, 1)] and n3.id = e2.end_id)
select ex1.n3 as l
from ex1;
