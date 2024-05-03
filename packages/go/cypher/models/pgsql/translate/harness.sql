truncate table edge;
truncate table node;

insert into node (id, graph_id, kind_ids, properties)
values (1, 1, array [1], '{"name": "n1"}'),
       (2, 1, array [1, 2], '{"name": "n2"}'),
       (3, 1, array [1, 2], '{"name": "n3"}'),
       (4, 1, array [2], '{"name": "n4"}');

insert into edge (graph_id, start_id, end_id, kind_id, properties)
values (1, 1, 2, 11, '{"name": "e1", "prop": "a"}'),
       (1, 2, 3, 11, '{"name": "e2", "prop": "a"}'),
       (1, 3, 4, 11, '{"name": "e3"}');
