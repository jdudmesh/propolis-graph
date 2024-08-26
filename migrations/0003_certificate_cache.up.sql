create table certificate_cache (
    id text not null primary key,
    created_at datetime not null,
    updated_at datetime null,
    certificate blob not null
);