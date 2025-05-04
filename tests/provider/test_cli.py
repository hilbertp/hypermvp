import subprocess
import tempfile
from pathlib import Path
import pandas as pd

def create_sample_excel(directory):
    """
    Create a minimal valid Excel file for provider data in the given directory.
    """
    df = pd.DataFrame({
        "DELIVERY_DATE": ["2024-09-01 00:00:00"],
        "PRODUCT": ["FRR"],
        "ENERGY_PRICE_[EUR/MWh]": [100.0],
        "PAYMENT_DIRECTION": ["PROVIDER_TO_GRID"],
        "ALLOCATED_CAPACITY_[MW]": [5.0],
        "NOTE": ["note1"],
    })
    file_path = Path(directory) / "sample_provider_data.xlsx"
    df.to_excel(file_path, index=False)
    return file_path

def test_cli_runs_and_loads(monkeypatch):
    # Create a temp directory and Excel file
    with tempfile.TemporaryDirectory() as tmpdir:
        excel_file = create_sample_excel(tmpdir)
        db_path = Path(tmpdir) / "test.duckdb"

        # Build CLI command
        cmd = [
            "python", "-m", "src.hypermvp.provider.provider_cli",
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