create table nodes (
  id text not null primary key,
  created_at not null datetime
);

create table node_attributes (
  id text not null primary key,
  created_at not null datetime
  node_id text not null,
  foreign key(node_id) references nodes(id)
);

create table node_labels (
  id text not null primary key,
  created_at not null datetime,
  node_id text not null,
  foreign key(node_id) references nodes(id)
);

create table relations (
  id text not null primary key,
  created_at not null datetime
);

create table relation_attributes (
  id text not null primary key,
  created_at not null datetime,
  relation_id text not null,
  foreign key(relation_id) references relations(id)
);

create table relation_labels(
  id text not null primary key,
  created_at not null datetime,
  relation_id text not null,
  foreign key(relation_id) references relations(id)
);
