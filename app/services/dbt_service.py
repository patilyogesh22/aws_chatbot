import subprocess

from app.config import DBT_PROJECT_DIR


def run_dbt_build():
    try:
        result = subprocess.run(
            ["dbt", "build"],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            timeout=120
        )

        output = (result.stdout or "") + (result.stderr or "")

        if result.returncode == 0:
            return "dbt build succeeded"

        return f"dbt failed:\n{output[-800:]}"

    except Exception as e:
        return f"dbt error: {str(e)}"