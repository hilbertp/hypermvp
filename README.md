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
#### Recommended: Work on WSL2 for Windows Users

If you are a Windows user, we strongly recommend working on this project in WSL2 (Windows Subsystem for Linux). This ensures compatibility and prevents issues like crashes or performance bottlenecks that occur when working on Windows-mounted directories (/mnt/...).

#### **Setting Up WSL2**
1. **Install WSL2**:
   Open PowerShell as Administrator and run:
   ```bash
   wsl --install
   ```
Restart your system if prompted.

2. **Verify Installation**: Open a terminal and run:   
    ```bash
      wsl --install
   ```
   Ensure that WSL2 is installed and running.

3. **Set a Default Linux Distribution:**: If needed, set Ubuntu as your default distro:
    ```bash
      wsl --set-default Ubuntu
   ```

4. **Start WSL**: Launch WSL by simply typing:
    ```bash
      wsl
   ```
5. **Install Git in WSL**: Once inside WSL, ensure Git is installed:
   ```bash
      sudo apt update
      sudo apt install git
   ```

#### **Working in WSL2**
1. Clone the repo on a directory on the WSL, **do not** use a mounted windows directory!
  ```bash
      git clone https://github.com/hilbertp/hypermvp
   ```
---

### **Installation**

1. **Install Prerequisites:** Inside WSL, install the following:
   -  **pyenv** for managing Python versions:
   ```bash
      curl https://pyenv.run | bash   
   ```
      Add the following to your `~/.bashrc` or `~/.zshrc`:
   ```bash
      export PATH="$HOME/.pyenv/bin:$PATH"
      eval "$(pyenv init --path)"
      eval "$(pyenv init -)"   
   ```
Restart your terminal and install Python 3.12:   
   ```bash
      pyenv install 3.12.0
      pyenv global 3.12.0
   ```
   - **Poetry** for dependency management:
   ```bash
      curl -sSL https://install.python-poetry.org | python3 -
   ```
   Add Poetry to your path:
   ```bash
      export PATH="$HOME/.local/bin:$PATH"   
   ```







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
