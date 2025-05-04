# User Story 001: Provider Table Cleaner

As a data analyst,  
I want a function that takes a data table in DuckDB and:

- Removes all rows where the `PRODUCT` column starts with `POS_`.
- For rows where `PAYMENT_DIRECTION` is `PROVIDER_TO_GRID`, multiplies the `ENERGY_PRICE_[EUR/MWh]` by `-1`.
- Sorts the table first by `DELIVERY_DATE` (chronological order), and then by `ENERGY_PRICE_[EUR/MWh]` (ascending order within each delivery date).

So that the resulting data is filtered, price-corrected, and consistently ordered for further analysis.

---

## Acceptance Criteria

- The function can be implemented as a DuckDB SQL query or as a Python function using `pandas` or `polars`.
- The logic must be modular and easy to test.
- The output table or DataFrame contains only the cleaned and sorted data as described above.
- **Sorting must always use `DELIVERY_DATE` as the primary key and `ENERGY_PRICE_[EUR/MWh]` as the secondary key, so that dates are always in chronological order and, within each date, prices are ascending.**

---

## Testing

- Include unit tests with sample/mock data to verify:
  - Rows with `PRODUCT` starting with `POS_` are removed.
  - Price sign is corrected for `PROVIDER_TO_GRID`.
  - Output is sorted as specified, with chronological order always taking precedence over price order.

---

## Plain English Summary

This function or query ensures that only relevant provider bid data is kept, price signs are correct, and the data is sorted so that all entries are in chronological order, and within each time period, prices are sorted ascendingly.
