# Keep SQL in one place for reuse and linting.
CREATE_EXAMPLE_OUTPUT = """
CREATE TABLE IF NOT EXISTS public.example_output (
    id INT PRIMARY KEY,
    value TEXT
);
"""

SELECT_EXAMPLE_INPUT = "SELECT id, value FROM public.example_input;"

UPSERT_EXAMPLE_OUTPUT = """
    INSERT INTO public.example_output (id, value)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value
"""