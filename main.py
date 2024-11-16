import pandas as pd
import numpy as np

print("Projekt bereit!")
print("Pandas Version: ", pd.__version__)
print("Numpy Version: ", np.__version__)

file_path="data/beispiel.xlsx"

try:
    df = pd.read_excel(file_path)
    print("DAten erfolgreich geladen.")
    print(df.head())
except FileNotFoundError:
    print (f"Dateo {file_path} nicht gefunden. Bitte überprüfe Pfad.")