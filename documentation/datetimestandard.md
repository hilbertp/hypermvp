# Date Format Standardization Plan

Based on your project files, I've identified the scripts that need to be updated to use the centralized date format constants from config.py.

## Files Requiring Updates

### 1. Analysis Module
- marginal_price.py
  - Uses date parsing: `datetime.strptime(start_date, '%Y-%m-%d').date()`
  - Uses date formatting for timestamps and quarter-hour periods

### 2. AFRR Module
- cleaner.py
  - Processes "Datum" in German format (DD.MM.YYYY)
  - Handles "von" and "bis" time columns (HH:MM)

### 3. Provider Module
- loader.py
  - Loads DELIVERY_DATE from various sources
  - Needs standardization for datetime objects

- cleaner.py
  - Processes date columns during cleaning
  - Should standardize to common format

- converter.py (new file)
  - Must align with the standardized formats
  - Uses date conversion when processing CSV files

### 4. Dashboard Module
- Dashboard generation scripts
  - Format dates for display
  - Need to use standardized formatting

### 5. Scrapers
- cli.py
  - Uses date generation and formatting 

## Implementation Approach

For each file, we need to:

1. **Add Import:**
   ```python
   from hypermvp.config import (
       ISO_DATETIME_FORMAT, ISO_DATE_FORMAT, 
       TIME_FORMAT, AFRR_DATE_FORMAT, 
       standardize_date_column
   )
   ```

2. **Replace Hardcoded Formats:**
   - Find all instances of hardcoded date formats like `'%Y-%m-%d'`
   - Replace with the appropriate constant

3. **Use Standardization Helper:**
   - Where date columns are processed, use the `standardize_date_column` helper

## Specific Updates Needed

### 1. In `marginal_price.py`:
Replace:
```python
start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
```

With:
```python
start_date = datetime.strptime(start_date, ISO_DATE_FORMAT).date()
```

### 2. In `afrr/cleaner.py`:
Use standardization helper:
```python
df = standardize_date_column(df, 'Datum', AFRR_DATE_FORMAT)
```

### 3. In `provider/converter.py`:
Ensure it uses the standardized formats when working with CSV files and datetime conversion.

### 4. In all dashboard generation code:
Format dates using the standardized constants:
```python
formatted_date = date_obj.strftime(ISO_DATE_FORMAT)
```

By implementing these changes consistently across all files, you'll ensure that date formats are standardized throughout your application, making maintenance easier and reducing the risk of format-related bugs.