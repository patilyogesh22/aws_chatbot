import os
import glob
import psycopg2

PG_DSN = (
    f"host={os.getenv('PG_HOST') or os.getenv('DB_HOST')} "
    f"port={os.getenv('PG_PORT', '5432')} "
    f"dbname={os.getenv('PG_DB') or os.getenv('DB_NAME')} "
    f"user={os.getenv('PG_USER') or os.getenv('DB_USER')} "
    f"password={os.getenv('PG_PASSWORD') or os.getenv('DB_PASSWORD')}"
)

conn = psycopg2.connect(PG_DSN)

with conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                migration_name TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        files = sorted(glob.glob("migrations/*.sql"))

        for file in files:
            migration_name = os.path.basename(file)

            cur.execute(
                "SELECT 1 FROM schema_migrations WHERE migration_name = %s",
                (migration_name,)
            )

            if cur.fetchone():
                print(f"Skipping already applied: {migration_name}")
                continue

            print(f"Running migration: {migration_name}")

            with open(file, "r", encoding="utf-8") as f:
                sql = f.read()

            cur.execute(sql)

            cur.execute(
                "INSERT INTO schema_migrations (migration_name) VALUES (%s)",
                (migration_name,)
            )

            print(f"Applied: {migration_name}")

print("All migrations completed")