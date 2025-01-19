### HyperMVP Market Analysis (Revised)

#### Key Concepts:

1. **Marginal Price (Grenzpreis):**

   - The highest activated energy price in a 15-minute interval.
   - Crucial for profit optimization and pricing strategy.
   - Calculated from the last and most expensive offer activated in a given interval.

2. **Negative vs. Positive aFRR:**

   - **Negative aFRR:**

     - Negative aFRR (Automatic Frequency Restoration Reserve) is a balancing service used by the Transmission System Operator (TSO) to reduce energy on the grid when there is an oversupply. This service ensures that the grid frequency remains stable at 50 Hz by addressing surplus energy.
     - Mechanisms:
       1. Providers reduce their energy generation into the grid (e. g. turning down power plants or renewable energy sources).
       2. Alternatively, providers increase consumption by taking energy from the grid to utilize it effectively.
     - Activation Priority:
       - Providers offering negative aFRR services are ranked based on their bid prices.
       - The TSO activates the least expensive bids first (including those with negative prices, where providers pay the grid) until the required balancing volume is achieved.
       - This follows the **merit-order principle** to ensure cost efficiency.
     - Why Negative Prices?:
       - Providers may be willing to pay the grid to reduce their energy generation or consume excess energy to save operational costs (e.g., avoiding fuel expenses) or allow for beneficial energy usage.

   - **Positive aFRR:**

     - Balancing energy is injected into the system (TSO increases generation or reduces consumption).

3. **Payment Direction and Merit Order Principle:**

   - At the start of each 15-minute control window, the TSO engages providers based on the merit-order principle to stabilize the grid at 50 Hz during times of surplus energy. Providers with negative prices (PROVIDER\_TO\_GRID) are called first, as they are willing to pay to be activated. Once these are exhausted, the grid activates providers with zero prices, followed by those with positive prices (GRID\_TO\_PROVIDER). This sequence continues until the energy control demand for the period is fully met.

#### Algorithm for Marginal Price Calculation:

1. **Data Preparation:**

   - Two datasets are processed:
     - Activated power (negative aFRR) in 15-minute intervals.
     - Provider offers for varying quantities and prices of aFRR.
   - Time columns are converted into appropriate formats, and irrelevant positive aFRR data is removed.

2. **Merit-Order Principle:**

   - Offers are sorted by price, accounting for payment direction:
     - PROVIDER\_TO\_GRID prices are adjusted to negative values (e.g., “x EUR/MWh” becomes “-x EUR/MWh”).
     - GRID\_TO\_PROVIDER prices remain positive.

3. **Marginal Price Determination for an Interval:**

   - Identify the negative balancing energy demand (e.g., 80.528 MW at 16:00 – 16:15 for 50Hertz).
   - Match offers to the interval using the product code (e.g., `NEG_065`).
   - Filter and sort offers by adjusted price.

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



