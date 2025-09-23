CREATE TABLE IF NOT EXISTS questions (
    id SERIAL PRIMARY KEY,
    question_text TEXT NOT NULL,
    human_answer TEXT NOT NULL,
    llm_answer TEXT,
    similarity_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cache_metrics (
    id SERIAL PRIMARY KEY,
    cache_policy VARCHAR(50) NOT NULL,
    cache_size INTEGER NOT NULL,
    hit_rate FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_question_text ON questions(question_text);
CREATE INDEX IF NOT EXISTS idx_created_at ON questions(created_at);