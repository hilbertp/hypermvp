# **HyperMVP**

HyperMVP is a Python-based project designed to process, analyze, and clean data related to the German energy control market, particularly focusing on aFRR (automatic frequency restoration reserve) and provider data.
This is the first software step to a great project that will allow us to harness free energy from the power grid to mine Bitcoin or to produce Hydrogen or similar. The transmission grid providers even pay us for the service of balancing the power grid at the same time. Triple win!

## **Features**
- **Data Handling:** Comprehensive data ingestion and processing pipeline, including storage in structured directories for raw, processed, and output data (`data/`).
- **aFRR Data Analysis:** Dedicated modules for parsing, cleaning, and analyzing aFRR market data to identify opportunities for cost-efficient energy usage.
- **Provider Data Management:** Tools for handling and comparing provider lists, ensuring compliance with transmission grid operator standards.
- **Automation:** Python-based scripts within the `src/` folder to streamline repetitive data processing tasks.
- **Integration Testing:** Built-in tests for key functionalities, including loaders and data cleaning scripts, ensuring reliability of the processing pipeline.
- **Energy Market Optimization:** Analytical tools to predict and simulate marginal prices, aiding in decision-making for energy consumption and grid stabilization.
- **Flexible Output:** Processed data is exportable in formats suitable for downstream applications like pricing analysis or energy optimization.

---

## **Getting Started**

### **Prerequisites**
Before you begin, ensure you have the following installed:
- Python 3.12 or later
- Poetry (Python dependency and environment manager)
- Any other dependencies specified in `pyproject.toml`

---

### **Installation**

1. Clone the repository:
   ```bash
   git clone https://github.com/hilbertp/hypermvp.git
   cd hypermvp
   ```

2. Install dependencies using Poetry:
   ```bash
   poetry install
   ```

3. Activate the Poetry-managed environment:
   ```bash
   poetry shell
   ```

4. Set up the environment using setup.sh:
   ```bash
   source setup.sh
   ```
---

## **Project Structure**
```bash 
hypermvp/
├── data/                  # Data directory for storage and processing
│   ├── 01_raw/            # Raw unprocessed data
│   ├── 02_processed/      # Processed intermediate data
│   └── 03_output/         # Cleaned and output data
├── documentation/         # Documentation files
├── src/hypermvp/          # Source code for the application
│   ├── afrr/              # Modules for handling aFRR data
│   ├── provider/          # Modules for handling provider data
├── tests/                 # Tests folder
│   ├── afrr/              # Tests for aFRR modules
│   └── provider/          # Tests for provider modules
```

---

## **Contributing**
Contributions are welcome! To contribute:
1. Fork the repository.
2. Create a new branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -m 'Add some feature'`).
4. Push to the branch (`git push origin feature-name`).
5. Open a Pull Request.

---

## **License**
This project is licensed under the [MIT License](LICENSE). See `LICENSE` for details.

---

## **Contact**
For questions or suggestions, please contact:  
- **Philipp Hilbert**, philipp (at) hyperion-grid (dot) com  
- [GitHub Profile](https://github.com/hilbertp)

---

Would you like to collaborate to find a way to gain free energy to mine bitcoin and get paid additionally for stabilizing the power grid? Then Hyperion Grid is the right project for you.
