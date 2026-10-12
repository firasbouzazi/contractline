-- Demo CRM source for the CDC pipeline. Synthetic data only.
CREATE TABLE public.customers (
    customer_id  BIGSERIAL PRIMARY KEY,
    email        TEXT        NOT NULL,
    full_name    TEXT,
    birth_date   DATE,
    country_code TEXT        NOT NULL,
    segment      TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Debezium needs the old row values on UPDATE and DELETE to emit full "before" images.
ALTER TABLE public.customers REPLICA IDENTITY FULL;

INSERT INTO public.customers (email, full_name, birth_date, country_code, segment) VALUES
    ('ada@example.com',    'Ada Example',    '1985-04-12', 'FR', 'consumer'),
    ('bob@example.com',    'Bob Example',    '1979-11-30', 'TN', 'business'),
    ('chloe@example.com',  'Chloe Example',  NULL,         'DE', 'partner');
