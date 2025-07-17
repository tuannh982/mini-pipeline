CREATE TABLE pipeline_templates (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT DEFAULT '',
    json_definition TEXT NOT NULL
);