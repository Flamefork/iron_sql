CREATE TABLE ordered_items (
    id INTEGER NOT NULL,
    group_id INTEGER NOT NULL,
    order_index INTEGER NOT NULL,
    tags JSONB NOT NULL
);

CREATE TABLE owners (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE resources (
    id INTEGER PRIMARY KEY,
    owner_id INTEGER NOT NULL REFERENCES owners(id),
    rank INTEGER NOT NULL
);

CREATE TABLE members (
    id INTEGER PRIMARY KEY
);

CREATE TABLE sections (
    id INTEGER PRIMARY KEY
);

CREATE TABLE memberships (
    member_id INTEGER NOT NULL REFERENCES members(id),
    section_id INTEGER REFERENCES sections(id)
);

CREATE TABLE event_log (
    id INTEGER NOT NULL,
    stream_id INTEGER NOT NULL,
    payload BYTEA NOT NULL
);
