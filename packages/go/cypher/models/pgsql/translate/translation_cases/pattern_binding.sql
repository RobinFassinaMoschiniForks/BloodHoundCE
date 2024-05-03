-- case: match p = ()-[]->() return p
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.id = e0.start_id
                   join node n1 on n1.id = e0.end_id)
select (array [e0.n0, e0.n1], array [e0.e0])::pathcomposite as p
from e0;

-- case: match p = ()-[r1]->()-[r2]->(e) return e
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
select e1.n2 as e
from e1;

-- case: match ()-[r1]->()-[r2]->()-[]->() where r1.name = 'a' and r2.name = 'b' return r1
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.id = e0.start_id
                   join node n1 on n1.id = e0.end_id
            where e0.properties ->> 'name' = 'a'),
     e1 as (select e0.e0                                                                     as e0,
                   (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1,
                   e0.n0                                                                     as n0,
                   e0.n1                                                                     as n1,
                   (n2.id, n2.kind_ids, n2.properties)::nodecomposite                        as n2
            from e0,
                 edge e1
                   join node n2 on n2.id = e1.end_id
            where (e0.e0).end_id = e1.start_id
              and e1.properties ->> 'name' = 'b'),
     e2 as (select e1.e0                                                                     as e0,
                   e1.e1                                                                     as e1,
                   (e2.id, e2.start_id, e2.end_id, e2.kind_id, e2.properties)::edgecomposite as e2,
                   e1.n0                                                                     as n0,
                   e1.n1                                                                     as n1,
                   e1.n2                                                                     as n2,
                   (n3.id, n3.kind_ids, n3.properties)::nodecomposite                        as n3
            from e1,
                 edge e2
                   join node n3 on n3.id = e2.end_id
            where (e1.e1).end_id = e2.start_id)
select e2.e0 as r1
from e2;

-- case: match p = (a)-[]->()<-[]-(f) where a.name = 'value' and f.is_target return p
with e0 as (select (e0.id, e0.start_id, e0.end_id, e0.kind_id, e0.properties)::edgecomposite as e0,
                   (n0.id, n0.kind_ids, n0.properties)::nodecomposite                        as n0,
                   (n1.id, n1.kind_ids, n1.properties)::nodecomposite                        as n1
            from edge e0
                   join node n0 on n0.properties ->> 'name' = 'value' and n0.id = e0.start_id
                   join node n1 on n1.id = e0.end_id),
     e1 as (select e0.e0                                                                     as e0,
                   (e1.id, e1.start_id, e1.end_id, e1.kind_id, e1.properties)::edgecomposite as e1,
                   e0.n0                                                                     as n0,
                   e0.n1                                                                     as n1,
                   (n2.id, n2.kind_ids, n2.properties)::nodecomposite                        as n2
            from e0,
                 edge e1
                   join node n2 on (n2.properties -> 'is_target')::bool and n2.id = e1.start_id
            where (e0.e0).start_id = e1.end_id)
select (array [e1.n0, e1.n1, e1.n2], array [e1.e0, e1.e1])::pathcomposite as p
from e1;
