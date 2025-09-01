-- Step 1: Enable the pgvector extension 
CREATE EXTENSION IF NOT EXISTS vector;

-- Step 2: Create the table "column_embeddings".
CREATE TABLE public.column_embeddings (
    id serial PRIMARY KEY,
    column_name text NOT NULL,
    description text NOT NULL,
    embedding vector(1536),
    keywords text[]
);

-- Step 3: Create indexes for efficient data retrieval.

-- Index 1: HNSW index on the 'embedding' column for fast similarity search.
-- This is crucial for performance when working with vector embeddings.
CREATE INDEX ON public.column_embeddings
USING hnsw (embedding vector_cosine_ops);

-- Index 2: GIN index on the 'keywords' column.
-- This allows for fast searching within the text array.
CREATE INDEX ON public.column_embeddings
USING gin (keywords);

-- Filters the columns containing keywords in the user query before vector search
SELECT
    column_name,
    (SELECT COUNT(*)
        FROM unnest(keywords) as k
        WHERE k = ANY(%s)) as match_count,
    keywords
FROM
    public.column_embeddings
WHERE
    keywords && %s
ORDER BY
    match_count DESC;

--performs similarity search on all columns
"""
SELECT column_name, description
FROM public.column_embeddings
ORDER BY embedding <=> %s
LIMIT 25;
""", (str(embedding),)

--performs similarity search on only the filtered columns
"""
SELECT column_name, description
FROM public.column_embeddings
WHERE column_name = ANY(%s) -- This is the new filtering clause
ORDER BY embedding <=> %s
LIMIT 25;
""", (prefiltered_columns, str(embedding))