CREATE TYPE user_status AS ENUM ('active', 'inactive');

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB
);

CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id),
    title TEXT NOT NULL,
    content TEXT,
    published BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS json_payloads (
    id SERIAL PRIMARY KEY,
    payload JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS jsonb_arrays (
    id SERIAL PRIMARY KEY,
    payloads JSONB[] NOT NULL
);
