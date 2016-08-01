create table question (
    qid serial primary key,
    title text not null,
    description_url text,
    description text,
    is_asked boolean,
    where_asked text,
    last_updated_dttm TIMESTAMP
);