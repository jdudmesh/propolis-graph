create table nodes (
  id text not null primary key,
  created_at datetime not null,
  updated_at datetime null
);

create table node_attributes (
  id text not null primary key,
  created_at datetime not null,
  updated_at datetime null,
  node_id text not null,
  attr_name text not null,
  attr_value text not null,
  data_type int not null,
  foreign key(node_id) references nodes(id)
);

create index idx_nodes_attributes_attr_name on node_attributes(attr_name);

create table node_labels (
  id text not null primary key,
  created_at datetime not null,
  updated_at datetime null,
  node_id text not null,
  label text not null,
  foreign key(node_id) references nodes(id)
);

create index idx_node_labels_label on node_labels(label);

create table relations (
  id text not null primary key,
  created_at datetime not null,
  updated_at datetime null,
  left_node_id text not null,
  right_node_id text not null,
  direction int not null,
  foreign key(left_node_id) references nodes(id),
  foreign key(right_node_id) references nodes(id)
);

create index idx_relations_direction on relations(direction);

create table relation_attributes (
  id text not null primary key,
  created_at datetime not null,
  updated_at datetime null,
  relation_id text not null,
  attr_name text not null,
  attr_value text not null,
  data_type int not null,
  foreign key(relation_id) references relations(id)
);

create index idx_relation_attributes_attr_name on relation_attributes(attr_name);

create table relation_labels(
  id text not null primary key,
  created_at datetime not null,
  updated_at datetime null,
  relation_id text not null,
  label text not null,
  foreign key(relation_id) references relations(id)
);

create index relation_labels_label on relation_labels(label);