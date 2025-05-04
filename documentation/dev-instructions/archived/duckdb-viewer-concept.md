Got it. I’ll prepare an implementation recommendation and a complete, actionable battle plan for developing a human-friendly DuckDB data viewer using VS Code and GitHub Copilot. This will be based on your Python/Polars/DuckDB stack, your role as a solo developer/product owner, and the goal of gaining quick insights into data quality. I’ll recommend the most efficient approach (e.g., Jupyter Notebook or script-based tool), and outline step-by-step how to build and maintain it.

# Implementation Plan for a DuckDB Data Viewer

We recommend a **Jupyter Notebook (or VS Code Python Interactive) approach** as the fastest, lowest-overhead way to build a DuckDB data viewer for interactive inspection.  In practice, this means installing the DuckDB and Polars Python packages and using a notebook with the `%sql` magic (via [JupySQL](https://github.com/ploomber/jupytext)) to run queries against the database.  This allows ad-hoc SQL queries (for filtering and anomaly checks) while viewing results as DataFrames in the notebook.  For example, you can use `%sql SELECT * FROM provider_raw LIMIT 10;` to see sample rows as a Pandas DataFrame ([Jupyter Notebooks – DuckDB](https://duckdb.org/docs/stable/guides/python/jupyter.html#:~:text=Single%20line%20SQL%20queries%20can,displayed%20as%20a%20Pandas%20DataFrame)).  You can also load query results into Polars for faster in-memory analysis (DuckDB can output Polars DataFrames via the `.pl()` method ([Integration with Polars – DuckDB](https://duckdb.org/docs/stable/guides/python/polars.html#:~:text=DuckDB%20can%20output%20results%20as,conversion%20method))).  The notebook approach integrates well with VS Code’s Jupyter support and GitHub Copilot, lets you document each step in Markdown cells, and avoids building a full app or UI.  As alternatives, one could write a small **CLI script** (using argparse/typer + [rich](https://rich.readthedocs.io) for console tables) or a minimal **Streamlit UI** with a text box for SQL and a `st.dataframe` output; these are viable but involve more setup.  Overall, the interactive notebook is easiest for quick data exploration without heavy development overhead.  

## Environment Setup (Python + Libraries)  

- **Python version:** Use Python 3.8 or later.  Create a new virtual environment (venv or conda) for this project.  
- **Install DuckDB and Jupyter tools:**  Run a pip install of DuckDB, Polars, JupySQL (ipython-sql), and related libraries. For example:

  ```bash
  pip install duckdb   # DuckDB database engine (Python API)
  pip install -U 'polars[pyarrow]'  # Polars DataFrame library with Arrow support ([Integration with Polars – DuckDB](https://duckdb.org/docs/stable/guides/python/polars.html#:~:text=pip%20install%20))
  pip install jupyterlab jupytext  # Jupyter environment (if using JupyterLab) 
  pip install jupysql       # Enables %sql magic and data profiling commands ([Jupyter Notebooks – DuckDB](https://duckdb.org/docs/stable/guides/python/jupyter.html#:~:text=pip%20install%20jupysql%20pandas%20matplotlib,engine)) ([Data profiling — Python  documentation](https://jupysql.ploomber.io/en/latest/user-guide/data-profiling.html#:~:text=This%20example%20requires%20duckdb,engine))
  pip install duckdb-engine  # SQLAlchemy connector required for jupysql profiling ([Data profiling — Python  documentation](https://jupysql.ploomber.io/en/latest/user-guide/data-profiling.html#:~:text=This%20example%20requires%20duckdb,engine))
  ```

  JupySQL’s docs recommend installing **duckdb-engine** to enable the `%sql` magic with DuckDB and the `%sqlcmd profile` command ([Data profiling — Python  documentation](https://jupysql.ploomber.io/en/latest/user-guide/data-profiling.html#:~:text=This%20example%20requires%20duckdb,engine)).  (If you prefer the classic `ipython-sql`, it can also be used, but JupySQL is more actively maintained and easier for profiling.)  
- **Optional (CLI or UI):** If exploring a CLI alternative, also install `rich` and `typer`:  

  ```bash
  pip install rich typer
  ```  

  For a Streamlit UI alternative, install Streamlit: `pip install streamlit`.  

Keep these dependencies listed in a `requirements.txt` (or `pyproject.toml`) for reproducibility.

## Project and Code Structure  

1. **Project Layout:** Create a simple directory, e.g.:

   ```
   duckdb_viewer/
     README.md
     requirements.txt
     data_viewer.ipynb    # (or .py) the main notebook or script
     .gitignore
   ```

   In README.md, note usage instructions (see the Documentation section below). Use .gitignore to exclude any large data files.
2. **Database Connection:** In your notebook (or script), connect to the DuckDB database. For example, in Python:

   ```python
   import duckdb
   conn = duckdb.connect('path/to/your.db')   # use your DuckDB file path
   ```

   Alternatively, in a Jupyter cell with JupySQL loaded:

   ```sql
   %sql duckdb:///path/to/your.db
   ```

   This establishes the default connection to the file-based DuckDB.  (You can omit the path for the default in-memory DB.)  See DuckDB docs on [Jupyter usage](https://duckdb.org/docs/stable/guides/python/jupyter.html) ([Jupyter Notebooks – DuckDB](https://duckdb.org/docs/stable/guides/python/jupyter.html#:~:text=Querying%20DuckDB)).
3. **Query Examples:** Use `%sql` (single-line) or `%%sql` (cell) magics to run SQL and display results. For example:

   ```sql
   %sql SELECT * FROM provider_raw LIMIT 5;
   ```

   This outputs a Pandas DataFrame (thanks to JupySQL’s `autopandas=True` setting).  To save a query, do `result_df << %sql SELECT ...;` then work with `result_df` in Python. Alternatively, in pure Python you can do:

   ```python
   df = conn.execute("SELECT * FROM provider_raw LIMIT 5").fetchdf()  # Pandas DF
   display(df.head())
   ```

   Or to get a Polars DataFrame:

   ```python
   import polars as pl
   # DuckDB returns Polars via the .pl() method
   df_pl = duckdb.sql("SELECT * FROM provider_raw").pl()
   print(df_pl.head())
   ```

   DuckDB will convert to Polars efficiently via Arrow integration ([Integration with Polars – DuckDB](https://duckdb.org/docs/stable/guides/python/polars.html#:~:text=format%20arrow,for%20the%20integration%20to%20work)) ([Integration with Polars – DuckDB](https://duckdb.org/docs/stable/guides/python/polars.html#:~:text=DuckDB%20can%20output%20results%20as,conversion%20method)).
4. **Filtering and Inspection:**  Apply SQL filters or Polars filters to find anomalies.  For instance:

   ```sql
   %sql SELECT provider, NOTE FROM provider_raw WHERE NOTE LIKE '%error%' OR NOTE IS NULL;
   ```

   Or in Python:

   ```python
   # Using Polars to filter by string content
   df_pl = duckdb.sql("SELECT NOTE FROM provider_raw").pl()
   filtered = df_pl.filter(pl.col("NOTE").str.contains("some pattern"))
   print(filtered)
   ```

   JupySQL also lets you store query results as Pandas or Polars for further analysis.  For example, `%sql result_pl << SELECT * FROM provider_raw;` then work with `result_pl.pl()`.
5. **Profiling and Statistics:** Use JupySQL’s built-in profiling to quickly summarize columns.  In a Jupyter cell, run:

   ```sql
   %sqlcmd profile --table provider_raw
   ```

   This will print counts, unique values, top values, means, etc., for each column ([Data profiling — Python  documentation](https://jupysql.ploomber.io/en/latest/user-guide/data-profiling.html#:~:text=%25sqlcmd%20profile%20)).  It’s a fast way to spot nulls or outliers (e.g. if the NOTE column has unusually long strings, null frequency, etc.).  Under the hood this uses DuckDB SQL under the hood (EXPLAIN ANALYZE) to compute stats.  
6. **Output Display:**  Results from `%sql` or `duckdb.execute` show as tables in the notebook.  For large tables, display only the head or aggregate summaries.  Use Polars/Pandas `.describe()` or `.summary()` functions if needed.  If using a CLI, use `rich.Table` to pretty-print limited rows; for Streamlit, show tables via `st.dataframe()`.  

By following the above steps, you get a quick interactive “viewer”: run SQL or Python filters on demand, immediately see the table output, and tweak as needed. This is very maintainable (all code lives in one notebook or script) and efficient (DuckDB queries are fast even on large data).

## Alternative Approaches (Brief)

- **CLI Script:** You could write a small Python CLI tool (using [typer](https://typer.tiangolo.com) or argparse) that connects to DuckDB and takes commands. For example, `python viewer.py --table provider_raw --limit 10` would execute `SELECT * FROM provider_raw LIMIT 10`. Use [rich](https://rich.readthedocs.io) to format tables in the terminal. This has minimal UI, just a prompt or arguments, so development is light. But it’s less flexible than a notebook for ad-hoc queries.
- **Minimal Streamlit UI:** Create `viewer.py` with Streamlit to build a tiny web page. For example:

   ```python
   import streamlit as st, duckdb
   conn = duckdb.connect('data.db')
   query = st.text_input("SQL query", "SELECT * FROM provider_raw LIMIT 5")
   if st.button("Run"):
       df = conn.execute(query).fetch_df() 
       st.dataframe(df)
   ```

   This allows filtering via SQL text and shows the DataFrame in a browser. It is still lightweight (just one Python file) but requires running `streamlit run viewer.py`. It’s more user-friendly for non-technical users but a bit more setup than a notebook.  

Either alternative can work, but for a solo dev wanting speed and simplicity, the interactive notebook is usually fastest to implement.

## Detailed Steps and Example Code

1. **Create and activate a Python venv.** For example:

   ```bash
   python3 -m venv venv
   source venv/bin/activate   # (Windows: venv\Scripts\activate)
   pip install --upgrade pip
   ```

2. **Install required packages:** In the venv, run (based on [1] and [20]):

   ```bash
   pip install duckdb jupyterlab jupysql polars[pyarrow] duckdb-engine
   ```

   This installs DuckDB, Jupyter, JupySQL, Polars (with Arrow), and DuckDB SQLAlchemy engine ([Jupyter Notebooks – DuckDB](https://duckdb.org/docs/stable/guides/python/jupyter.html#:~:text=pip%20install%20jupysql%20pandas%20matplotlib,engine)) ([Integration with Polars – DuckDB](https://duckdb.org/docs/stable/guides/python/polars.html#:~:text=pip%20install%20)).  (If you also want plotting, install `matplotlib`.)  
3. **Start a notebook:** In VS Code or JupyterLab, create `data_viewer.ipynb`. At the top cell, load extensions and configure:

   ```python
   %load_ext sql
   %config SqlMagic.autopandas = True
   %config SqlMagic.feedback = False
   %config SqlMagic.displaycon = False
   %sql duckdb:///path/to/your.db
   ```

   This connects the `%sql` magic to your DuckDB. (The DB path can be relative or absolute; omit `:///` for in-memory.)  
4. **Run example queries:** In new cells, do things like:

   ```sql
   %%sql
   SELECT COUNT(*) AS total_rows, COUNT(DISTINCT provider) AS unique_providers
   FROM provider_raw;
   ```

   ```sql
   %sql SELECT provider, NOTE FROM provider_raw LIMIT 10;
   ```

   ```python
   # In Python: get NOTE values into Polars to check lengths
   import polars as pl
   df_notes = duckdb.sql("SELECT NOTE FROM provider_raw").pl()
   st = df_notes.select([
       pl.col("NOTE").str.lengths().alias("length"),
       pl.col("NOTE").is_null().alias("is_null")
   ])
   st.describe()  # show min/mean/max length, null count, etc.
   ```

   This shows if `NOTE` has outliers in length or many nulls.  
5. **Filtering examples:** If you suspect bad entries (e.g. unusual text in `NOTE`), do:

   ```sql
   %sql SELECT NOTE FROM provider_raw WHERE NOTE LIKE '%error%' OR NOTE LIKE '%NaN%' LIMIT 5;
   ```

   Or using Polars:

   ```python
   df_pl = duckdb.sql("SELECT * FROM provider_raw").pl()
   bad = df_pl.filter(pl.col("NOTE").str.contains("Unexpected"))
   bad.head(5).to_pandas()
   ```

   These let you eyeball anomaly cases.  
6. **Data profiling:** Run a profiling command to summarize all columns:

   ```sql
   %sqlcmd profile --table provider_raw
   ```

   This prints stats (count, unique, top, freq, mean, etc.) for each column ([Data profiling — Python  documentation](https://jupysql.ploomber.io/en/latest/user-guide/data-profiling.html#:~:text=%25sqlcmd%20profile%20)), highlighting any issues (e.g. if `NOTE` has an extreme max length or too many nulls).  
7. **Document and comment:** In the notebook, use Markdown cells to describe what each step does (“Check total rows”, “Filter null notes”, etc.). Comment your code so it’s clear.  In a `.py` script, include docstrings and comments for each function or step.  
8. **Testing and usage:** If using a script, test it manually: e.g. `python data_viewer.py --table provider_raw`. Ensure it catches invalid queries and handles empty results.  In a notebook, just run cells interactively.  
9. **Iterate:** As you find issues, you can add more queries (e.g. group by categories, search for outlier values).  Keep everything in this single project so it’s easy to maintain.  

## Key Libraries and Tools

- **duckdb (Python library):** The core database engine. Use its Python API or SQL magic for queries. See official docs ([Jupyter Notebooks – DuckDB](https://duckdb.org/docs/stable/guides/python/jupyter.html#:~:text=Single%20line%20SQL%20queries%20can,displayed%20as%20a%20Pandas%20DataFrame)).  
- **Polars:** Fast DataFrame library. DuckDB can export to Polars with `.pl()` ([Integration with Polars – DuckDB](https://duckdb.org/docs/stable/guides/python/polars.html#:~:text=DuckDB%20can%20output%20results%20as,conversion%20method)). Useful for in-memory filtering (`df.filter(...)`) or analyses not easily done in SQL.  
- **JupySQL (`%sql` magic):** Enables running SQL in Jupyter cells. Allows quick interactive queries and assignment of results to Pandas/Polars.  (JupySQL is recommended over plain `ipython-sql` for better support ([Jupyter Notebooks – DuckDB](https://duckdb.org/docs/stable/guides/python/jupyter.html#:~:text=pip%20install%20jupysql%20pandas%20matplotlib,engine)) ([Data profiling — Python  documentation](https://jupysql.ploomber.io/en/latest/user-guide/data-profiling.html#:~:text=This%20example%20requires%20duckdb,engine)).)  
- **duckdb-engine:** A SQLAlchemy driver that JupySQL uses under the hood for DuckDB. Required to run `%sqlcmd profile` ([Data profiling — Python  documentation](https://jupysql.ploomber.io/en/latest/user-guide/data-profiling.html#:~:text=This%20example%20requires%20duckdb,engine)).  
- **rich / typer (CLI mode):** If building a CLI, use `rich` for pretty tables and `typer` for argument parsing. (Not needed for the notebook.)  
- **Streamlit (UI mode):** If building a UI, use `st.dataframe`, `st.text_input`, etc., to filter. Streamlit apps stay locally by default, so no heavy backend.

## Documentation and Maintenance

- **README:** Clearly list setup steps (Python version, pip install, how to run). Provide examples of usage (e.g. sample queries).  
- **Code comments:** In the notebook, use Markdown headings and comments to explain each block. In scripts, use docstrings. E.g. explain the purpose of each section: connecting to DB, running a query, showing output.  
- **Version control:** Commit the code/notebook to GitHub. Use descriptive commit messages.  
- **Copilot-friendly:** Write clear function and variable names. Copilot will help fill in code, but verify it especially for SQL correctness.  
- **Continuous improvement:** As new data issues arise, add more queries or plots. For instance, you could eventually integrate plotting libraries (like matplotlib or Streamlit charts) if needed for deeper analysis.

By following these steps, you’ll have a **human-friendly, maintainable data viewer**: a notebook or script that can be opened anytime to inspect the DuckDB `provider_raw` table and its `NOTE` column, filter as needed, and quickly spot anomalies. The use of SQL magics and Polars ensures the tool is both powerful and lightweight ([Jupyter Notebooks – DuckDB](https://duckdb.org/docs/stable/guides/python/jupyter.html#:~:text=Single%20line%20SQL%20queries%20can,displayed%20as%20a%20Pandas%20DataFrame)) ([Integration with Polars – DuckDB](https://duckdb.org/docs/stable/guides/python/polars.html#:~:text=format%20arrow,for%20the%20integration%20to%20work)).

**Sources:** DuckDB’s official Python/Jupyter guide ([Jupyter Notebooks – DuckDB](https://duckdb.org/docs/stable/guides/python/jupyter.html#:~:text=Single%20line%20SQL%20queries%20can,displayed%20as%20a%20Pandas%20DataFrame)) ([Data profiling — Python  documentation](https://jupysql.ploomber.io/en/latest/user-guide/data-profiling.html#:~:text=This%20example%20requires%20duckdb,engine)) and documentation on DuckDB–Polars integration ([Integration with Polars – DuckDB](https://duckdb.org/docs/stable/guides/python/polars.html#:~:text=format%20arrow,for%20the%20integration%20to%20work)) ([Integration with Polars – DuckDB](https://duckdb.org/docs/stable/guides/python/polars.html#:~:text=DuckDB%20can%20output%20results%20as,conversion%20method)) (for using `.pl()`) provide best practices. JupySQL’s profiling docs show how `%sqlcmd profile` works ([Data profiling — Python  documentation](https://jupysql.ploomber.io/en/latest/user-guide/data-profiling.html#:~:text=%25sqlcmd%20profile%20)). These guides inform the setup and usage described above.
