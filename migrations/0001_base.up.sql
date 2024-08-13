create table seeds (
  remote_addr text not null primary key,
  created_at datetime not null,
  updated_at datetime null
);

create table local_subs (
  spec text not null primary key,
  created_at datetime not null,
  updated_at datetime null
);

create table peers (
  remote_addr text not null primary key,
  created_at datetime not null,
  updated_at datetime null
);

create table subs (
  remote_addr text not null,
  spec text not null,
  created_at datetime not null,
  updated_at datetime null,
  primary key(remote_addr, spec),
  foreign key(remote_addr) references peers(remote_addr)
);

create index idx_subs_peerspec on subs(remote_addr, spec);
create index idx_subs_spec on subs(spec);

create table actions (
  id text not null primary key,
  created_at datetime not null,
  updated_at datetime null,
  action text not null,
  remote_addr text not null
);

create index idx_actions_peer on actions(remote_addr);

create table pending_subs (
  remote_addr text not null,
  spec text not null,
  created_at datetime not null,
  updated_at datetime null,
  primary key(remote_addr, spec),
  foreign key(remote_addr) references peers(remote_addr)
);
