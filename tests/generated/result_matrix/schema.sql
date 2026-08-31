CREATE TYPE result_mood AS ENUM ('happy', 'calm');

        CREATE TABLE result_values (
            id INTEGER PRIMARY KEY,
            integer_required INTEGER NOT NULL,
            integer_optional INTEGER,
            mood_required result_mood NOT NULL,
            mood_optional result_mood,
            numbers_required INTEGER[] NOT NULL,
            numbers_optional INTEGER[],
            payload_required JSONB NOT NULL,
            payload_optional JSONB
        );
