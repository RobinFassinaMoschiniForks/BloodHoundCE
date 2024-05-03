-- case: match (n1), (n3) set n1.target = true set n3.target = true
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0),
     n1 as (select n0.n0 as n0, (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n1
            from n0,
                 node n1),
     n2 as (update node n2 set properties =
       jsonb_set(n2.properties, array ['target']::text[], 'true') from n0 where (n0.n0).id = n2.id returning (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n0, n0.n1 as n1),
     n3 as (update node n3 set properties =
       jsonb_set(n3.properties, array ['target']::text[], 'true') from n2 where (n2.n0).id = n3.id returning (n3.id, n3.kind_ids, n3.properties)::nodecomposite as n0, n2.n1 as n1)
select 1;

-- case: match (n) set n.other = 1 set n.prop = '1'
with n0 as (select (n0.id, n0.kind_ids, n0.properties)::nodecomposite as n0 from node n0),
     n1
       as (update node n1 set properties = jsonb_set(n1.properties, array ['other']::text[], '1') from n0 where (n0.n0).id = n1.id returning (n1.id, n1.kind_ids, n1.properties)::nodecomposite as n0),
     n2
       as (update node n2 set properties = jsonb_set(n2.properties, array ['prop']::text[], '1') from n1 where (n1.n0).id = n2.id returning (n2.id, n2.kind_ids, n2.properties)::nodecomposite as n0)
select 1;

-- case: match (n) where n.name = '1234' set n.is_target = true
with n0 as (select n0.* from node n0 where n0.properties ->> 'name' = '1234'),
     un0 as (update node un0 set properties =
       jsonb_set(un0.properties, array ['is_target']::text[], 'true') from n0 where un0.id = n0.id)
select 1;

-- case: match (n) where n.name = '1234' match (e) where e.tag = n.tag_id set e.is_target = true
with n0 as (select n0.* from node n0 where n0.properties ->> 'name' = '1234'),
     n1 as (select n1.* from node n1),
     un0 as (update node un0 set properties =
       jsonb_set(un0.properties, array ['is_target']::text[], 'true') from n0, n1 where n1.properties -> 'tag' = n0.properties -> 'tag_id' and un0.id = n1.id)
select 1;

-- case: match ()-[r]->(:NodeKind1) set r.is_special_outbound = true
with n0 as (select n0.* from node n0),
     n1 as (select n1.* from node n1 where n1.kind_ids operator (pg_catalog.&&) array [1]::int2[]),
     e0 as (select e0.*
            from n0,
                 n1,
                 edge e0
            where n0.id = e0.start_id
              and n1.id = e0.end_id),
     eu0 as (update edge eu0 set properties =
       jsonb_set(eu0.properties, array ['is_special_outbound']::text[], 'true') from e0 where eu0.id = e0.id)
select 1;

-- case: match (a)-[r]->(:NodeKind1) set a.name = '123', r.is_special_outbound = true
with n0 as (select n0.* from node n0),
     n1 as (select n1.* from node n1 where n1.kind_ids operator (pg_catalog.&&) array [1]::int2[]),
     e0 as (select e0.*
            from edge e0,
                 n1,
                 n0
            where n0.id = e0.start_id
              and n1.id = e0.end_id),
     eu0 as (update edge eu0 set properties =
       jsonb_set(eu0.properties, array ['is_special_outbound']::text[], 'true') from e0 where eu0.id = e0.id)
select 1;
