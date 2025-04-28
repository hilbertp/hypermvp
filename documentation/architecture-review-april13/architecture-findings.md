# Architecture Findings

based on the plan /home/philly/hypermvp/documentation/architecture-review-april13/Architecture-Review-Plan.md we identified these todos:

## TODO List - Step 1 Follow-up (Data Handling & Tests)

1. **Enhance Provider Update Test (`test_provider_db_cleaner.py`):**
    * Modify the test setup (`setUp` or within `test_update_provider_data_in_db`) to pre-populate the `provider_data` table with sample data covering time periods *both inside and outside* the date range of the test's input (`raw_provider_data`).
    * Add assertions to explicitly verify:
        * Records *outside* the input date range in `provider_data` remain unchanged.
        * Records *inside* the input date range in `provider_data` are deleted and replaced by the new data.
    * Confirm the test uses the current workflow functions (`update_provider_data_in_db` from `provider_db_cleaner.py`).

1. **Locate AFRR Upsert Logic:**
    * Identify the specific Python file(s) and function(s) responsible for:
        * Loading AFRR activation data from CSV files.
        * Performing the `UPSERT` (insert or update) operation into the relevant DuckDB table (e.g., `afrr_data`).

1. **Write AFRR Upsert Unit Test:**
    * Once the AFRR upsert function is located, create or update the relevant test file (e.g., `/home/philly/hypermvp/tests/afrr/test_afrr_db.py`).
    * Implement unit tests using `pytest` or `unittest` to cover:
        * Inserting AFRR data into an empty table.
        * Inserting data for a 15-minute interval that does not yet exist in the table.
        * Inserting data for a 15-minute interval that *already exists*, verifying that the existing record is updated correctly (the upsert behavior).

## Findings - Step 2 (Error Handling & Logging)

* **Review Focus:** Examined `main.py`'s handling of `subprocess` calls and overall logging strategy.
* **Error Handling:**
  * The use of `subprocess.run` and `subprocess.Popen` in `main.py` includes checks for non-zero return codes.
  * Errors (`stderr`) from subprocesses are captured and logged.
  * Failures in subprocesses correctly raise `RuntimeError` in `main.py`, halting the workflow.
  * Top-level `try...except` blocks in `main.py` provide an additional layer of safety.
  * **Conclusion:** Error handling for subprocesses in `main.py` appears robust and suitable for the MVP.
* **Logging:**
  * Basic logging is configured using `logging.basicConfig`.
  * Informative messages log the start/end of major workflow steps.
  * Error messages, including subprocess `stderr`, are logged.
  * Progress reporting (like `tqdm` integration) provides good runtime feedback.
  * **Conclusion:** Logging provides adequate visibility for status and debugging for the MVP.
* **Overall Assessment:** No major gaps identified in error handling or logging within the main orchestration script (`main.py`) for the current MVP scope.

## Findings - Step 3 (Modularity & Visual Overview)

* **Review Focus:** Evaluated the use of `subprocess` in `main.py` for invoking the provider workflow and reviewed the accuracy of the Mermaid diagram in the architecture plan.
* **Modularity (`subprocess`):**
  * `main.py` uses `subprocess` to call `provider_cli.py`.
  * The error handling around these calls (reviewed in Step 2) is robust.
  * **Conclusion:** While direct function imports could be an alternative for tighter integration, the current `subprocess` approach is acceptable and functional for the MVP. Refactoring is not a priority at this stage.
* **Visual Overview (Diagram):**
  * The Mermaid diagram in `architecture-review-plan.md` (Section 4), with minor quoting fixes applied, accurately reflects the components and data flow.
  * It correctly shows `main.py` orchestrating handlers, the data sources (Excel, CSV), the DuckDB database, and the distinct update strategies (Delete/Insert for Provider, Upsert for AFRR).
  * **Conclusion:** The diagram provides a clear and correct high-level view of the application structure.
* **Overall Assessment:** The current modularity approach is sufficient for the MVP, and the visual overview is accurate. No immediate actions required for this step.

## Findings - Step 4 (Scalability Bottlenecks - Data Loading)

* **Review Focus:** Assessed potential performance issues with loading large provider Excel files (~100MB, multi-sheet, ~3M rows/month) and AFRR CSV files.
* **Previous Method (Pandas - deprecated):** User confirmed this was extremely slow (~10 min for <1/3 of data) and incomplete (only loaded the first sheet), validating it as a major bottleneck.
* **Current Method (Direct Excel-to-DuckDB via `openpyxl`):**
  * This approach (`provider_loader.py`, `provider_db_cleaner.py`) is currently under development/refactoring.
  * **User reports initial tests do *not* indicate significantly faster load times compared to the deprecated Pandas method.** This suggests the `openpyxl`-based reading might still be a bottleneck.
  * Performance testing is still **blocked** until the implementation is stable and handles all sheets correctly.
* **AFRR Data:** Considered less critical for performance due to smaller data volumes.
* **Conclusion:** Both the previous Pandas method and potentially the current `openpyxl`-based refactoring appear too slow for efficient loading of large provider Excel files. Performance remains a key concern.
* **TODO (Post-Refactor/Debugging):**
  1. Stabilize the current `provider_loader.py` and `provider_db_cleaner.py` workflow to correctly load *all* sheets from a provider Excel file.
  2. Perform a baseline performance test (time, memory) with a full-size monthly file using the stabilized `openpyxl` method.
  3. **High Priority:** If the baseline performance confirms the slowness (as initial tests suggest), **immediately investigate and implement alternatives**:
      * **Option A:** DuckDB's native `read_excel()` function (likely simpler, potentially good performance).
      * **Option B:** Polars `scan_excel()` (likely most memory-efficient and potentially fastest, adds dependency).
  4. Compare the performance of the chosen alternative against the `openpyxl` baseline.

## Findings - Step 5 (Core Calculation Tests - Marginal Price)

* **Review Focus:** Identify the core marginal price calculation logic and assess the need for unit tests.
* **Code Identified:** The primary calculation logic is within the `calculate_marginal_prices` function in `/home/philly/hypermvp/src/hypermvp/analysis/marginal_price.py`. It uses data from `afrr_data` and `provider_data` tables in DuckDB.
* **Test Coverage:** Currently, there are no dedicated unit tests for `calculate_marginal_prices`.
* **Required Test Scenarios (Based on user feedback):**
    1. **Normal Case:** Verify correct price calculation based on merit order for a typical interval with bids and activation.
    2. **Zero Activation Volume:** Verify `marginal_price` is `None` when activation volume is zero.
    3. **Missing Provider Bids:** Verify `marginal_price` is `None` when activation volume is non-zero but no corresponding provider bids are found for the interval/product.
* **Conclusion:** Unit tests are needed to ensure the correctness of the core marginal price calculation logic.
* **TODO:**
    1. Create test file `/home/philly/hypermvp/tests/analysis/test_marginal_price.py`.
    2. Implement `pytest` unit tests for `calculate_marginal_prices` covering the three scenarios listed above.
    3. Use fixtures to create an in-memory DuckDB instance and populate it with mock `afrr_data` and `provider_data` for each test case.

## Appendix: Visual Overview Diagram (Mermaid Code)

This diagram provides a high-level conceptual overview of the application components and data flow, as discussed in Step 3.

```mermaid
graph TD
    subgraph User/System
        A[Manual Trigger / Scheduler] --> B(main.py Orchestrator);
    end

    subgraph main.py Control Flow
        B -- Triggers Process 1 --> C{Provider Data Handler};
        B -- Triggers Process 2 --> F{AFRR Data Handler};
        B -- Triggers Process 3 --> H{"Data Cleaner (Optional)"}; 
        B -- Triggers Process 4 --> K{Marginal Price Calculator};
        B -- Triggers Process 5 --> L{Dashboard Summarizer};
        B -- Triggers Process 6 --> I{Data Archiver};
    end

    subgraph Data Storage
        E[(DuckDB Database)];
    end

    subgraph Data Sources/Sinks
        D["Excel Files (Provider Bids)"]; 
        G["CSV Files (AFRR Activations)"];
        J[Archive Location];
        M["Summary Output (File/Console)"];
    end

    subgraph Process Interactions
        C -- Reads --> D;
        C -- Updates (Delete/Insert) --> E;
        F -- Reads --> G;
        F -- Updates (Upsert) --> E;
        H -- Reads/Writes --> E;
        K -- Reads --> E;
        K -- Writes Results --> E;
        L -- Reads --> E;
        L -- Writes --> M;
        I -- Reads Config & Moves --> D;
        I -- Reads Config & Moves --> G;
        I -- Moves Files To --> J;
    end

    style E fill:#f9f,stroke:#333,stroke-width:2px
