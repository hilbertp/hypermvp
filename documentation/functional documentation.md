### HyperMVP Market Analysis (Revised)

#### Key Concepts:

1. **Marginal Price (Grenzpreis):**

   - The highest activated energy price in a 15-minute interval.
   - Crucial for profit optimization and pricing strategy.
   - Calculated from the last and most expensive offer activated in a given interval.

2. **Negative vs. Positive aFRR:**

   - **Negative aFRR:**
     - Negative aFRR (Automatic Frequency Restoration Reserve) is a balancing service used by the TSO to reduce energy on the grid when there is an oversupply.
     - Mechanisms:
       1. Energy providers reduce their energy generation into the grid, or
       2. Consumers increase consumption to utilize excess energy.
     - Activation Priority:
       - Providers offering negative aFRR services are ranked by bid prices.
       - The TSO activates the lowest-priced bids first (including those with negative prices, where providers effectively pay to be activated) until the required balancing volume is met.
       - This follows the **merit-order principle** for cost efficiency.
     - Why Negative Prices?:
       - Some providers are more eager to be called a lot and may even be willing to pay the grid operator to be called before others that want to be paid by the grid. The reason why can be explained by Philipp but are too complex to be explained in written form here.

   - **Positive aFRR:**
- Represents the injection of balancing energy into the system (the TSO increases generation or reduces consumption).

3. **Payment Direction and Merit Order Principle:**

   - At the start of each 15-minute control window, the TSO stabilizes the grid by engaging providers in a sequence that honors the merit-order principle.
   - Providers with negative prices (Payment Direction PROVIDER_TO_GRID) are called first, followed by providers with zero and then positive prices (Payment Direction GRID_TO_PROVIDER), until the control demand is fully met.

#### Workflow Overview:

**Provider Workflow (Revised):**

- **Data Collection:**  
  Raw XLSX files containing provider bid data are received in a dedicated input folder.

- **Data Preparation:**  
  The system loads and cleans these raw files and groups the data into 4‑hour periods based on the DELIVERY_DATE.
  
- **4‑Hour Interval Update Process:**  
  For each 4‑hour period (e.g., 00:00–04:00, 04:00–08:00, etc.), the system deletes any previously stored data for that period from the database and then inserts the complete new dataset. This ensures that duplicate imports do not occur while still allowing all valid bid entries—including identical ones—to be maintained.

- **Outcome:**  
  Provider bid data is kept current on a 4‑hour interval basis. Corrections or re-imports will entirely replace the older data for a given 4‑hour period.

**AFRR Workflow (Revised):**

- **Data Collection:**  
  Raw CSV files containing AFRR data are received from the designated source.

- **Data Preparation:**  
  The system loads and cleans the AFRR data in memory.

- **15-Minute Interval Update Process:**  
  Every 15-minute period requires a specific adjustment value – a delta between the planned energy and the actual measured energy. This delta, which is always negative for our purposes (we are only analysing negativ AFRR in this project), is used to keep the grid stable. 
  When a value is imported that already exists the script updates the database by overwriting the existing delta for that interval. This ensures that each interval always holds the most current adjustment value while allowing past values to be corrected if necessary.

- **Outcome:**  
  AFRR data is maintained accurately on a 15-minute interval basis, with one current adjustment value (delta) per interval.

#### Algorithm for Marginal Price Calculation:

1. **Data Preparation:**

   - Two datasets are processed:
     - Activated power (negative aFRR) in 15-minute intervals.
     - Provider offers for varying quantities and prices of aFRR.
   - Time columns are formatted appropriately, and any irrelevant positive aFRR data is removed.

2. **Merit-Order Principle:**

   - Offers are sorted by price, accounting for payment direction:
     - PROVIDER_TO_GRID prices are adjusted to negative values.
     - GRID_TO_PROVIDER prices remain positive.

3. **Marginal Price Determination for an Interval:**

   - The system identifies the negative balancing energy demand for the interval.
   - Bids are matched based on their product code.
   - Sorted bids are accumulated until the energy control demand is met.
   - The price of the final (most expensive) activated bid defines the marginal price.

4. **Iterative Summation to Determine Marginal Price:**

   - Initialize variables for cumulative power and marginal price.
   - Iterate through sorted offers:
     - Add offered power to the cumulative total until demand is met.
     - The price of the last activated offer sets the marginal price.

#### Example Calculation:

1. **Input Data:**

   - Negative balancing energy demand: 80.528 MW (50Hertz at 16:00 – 16:15).
   - Offers sorted by price:
     | Offer # | Price (EUR/MWh) | Offered MW | Cumulative MW | Still Needed? |
     | ------- | --------------- | ---------- | ------------- | ------------- |
     | 1       | -10.0           | 30         | 30            | 55.528        |
     | 2       | -5.0            | 20         | 50            | 35.528        |
     | 3       | -3.87           | 40         | 90            | Demand Met    |

2. **Result:**

   - Marginal Price: “-3.87 EUR/MWh” at 80.528 MW.
   - Hypothetical Variation: At 138 MW demand using the same provider pool, the marginal price would have been 14.04 EUR/MWh.

#### Insights:

- **Strategic Pricing:**
  - Marginal prices help providers adjust bidding strategies. For example, historical data showing frequent high prices in specific intervals can inform offer adjustments.
- **Surface-Level Forecasting:**
  - By analyzing trends in marginal prices, providers may identify patterns that help shape future pricing strategies.

#### Structure and Flow Enhancements:

1. **Step-by-Step Process:**

   - Under the merit-order principle, steps such as “(a) Filter by product code and TSO, (b) Sort by price, (c) Accumulate volumes” can be emphasized for clarity.

2. **Practical Explanations:**

   - The use of negative and positive price conventions aligns with real-world balancing scenarios, making the documentation intuitive for all readers.

3. **Example Variation Context:**

   - The example highlights how the marginal price changes under higher demand using the same provider pool. This variation demonstrates the algorithm’s flexibility without introducing new complexities.



