import pandas as pd

def load_provider_file(filepath):
    """Load a provider file assuming it's always an XLSX file."""
    if not filepath.endswith('.xlsx'):
        raise ValueError(f"Unsupported file format: {filepath}")
    return pd.read_excel(filepath)
