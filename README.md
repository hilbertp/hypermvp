
# **HyperMVP**
HyperMVP is a Python-based project designed to process, analyze, and clean data related to the German energy control market, particularly focusing on aFRR (automatic frequency restoration reserve) and provider data.

## **Features**
- Structured data storage in directories for raw, processed, and output data (`data/`).
- Python-based automation for data processing and analysis within the `src/` folder.
- Main entry point to execute the project (`main.py`).
- Integrated data processing tests, likely within the `src/` folder (e.g., `test_loader.py`).

---

## **Getting Started**

### **Prerequisites**
Before you begin, ensure you have the following installed:
- Python 3.8 or later
- `pip` (Python package manager)
- pandas
- numpy
- other dependencies specified in requirements.txt


### **Installation**
1. Clone the repository:
   ```bash
   git clone https://github.com/hilbertp/hypermvp.git
   cd hypermvp
   ```
2. Create and activate a virtual environment (optional but recommended):

on Windows:
```bash
.\venv\Scripts\activate
```
on macOS/Linux:
```bash
source venv/bin/activate
```
3. Install the package as editable, with required dependencies:
```bash
   pip install -e .
```

---


## **Project Structure**
```bash 
hypermvp/
├── .vscode/               # Visual Studio Code configurations
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
└── .gitignore             # Git ignore file
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
- ** Philipp Hilbert**, philipp (at) hyperion-grid (dot) com  
- [GitHub Profile](https://github.com/hilbertp)

---

Would you like me to include placeholders for dependencies in `requirements.txt` or expand on any section? Let me know!
