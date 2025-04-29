import subprocess
import tempfile
import shutil
from pathlib import Path
import pandas as pd

def create_sample_excel(directory, filename="test.xlsx"):
    df = pd.DataFrame({
        "DELIVERY_DATE": ["2024-01-01"],
        "PRODUCT": ["aFRR"],
        "ENERGY_PRICE_[EUR/MWh]": [10.5],
        "ENERGY_PRICE_PAYMENT_DIRECTION": ["TSO to BSP"],
        "ALLOCATED_CAPACITY_[MW]": [100],
        "NOTE": [""]
    })
    file_path = Path(directory) / filename
    df.to_excel(file_path, index=False)
    return file_path

def test_cli_runs_and_loads(monkeypatch):
    # Create a temp directory and Excel file
    with tempfile.TemporaryDirectory() as tmpdir:
        excel_file = create_sample_excel(tmpdir)
        db_path = Path(tmpdir) / "test.duckdb"

        # Build CLI command
        cmd = [
            "python", "-m", "hypermvp.provider.cli",
            "--input-dir", tmpdir,
            "--db-path", str(db_path),
            "--log-level", "INFO"
        ]

        # Run the CLI as a subprocess
        result = subprocess.run(cmd, capture_output=True, text=True)

        # Check CLI output for success
        assert "Starting ETL process" in result.stdout or result.stderr
        assert "ETL Summary" in result.stdout or result.stderr
        assert db_path.exists()

        # Optionally, check DuckDB for loaded data
        import duckdb
        con = duckdb.connect(str(db_path))
        rows = con.execute("SELECT * FROM provider_raw").fetchall()
        assert len(rows) == 1
        # Check that the NOTE column is not present if it was empty in the input
        columns = [desc[0] for desc in con.execute("PRAGMA table_info('provider_raw')").fetchall()]
        assert "NOTE" not in columns
        con.close()