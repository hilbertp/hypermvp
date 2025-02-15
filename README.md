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

> **Warning for Windows Users:** We strongly recommend working on this project in WSL2 (Windows Subsystem for Linux) because Windows introduces unnecessary complexity when working with Python environments, dependency management, and system-level operations. WSL2 provides a native Linux environment that ensures compatibility, simplifies package management, and prevents issues with file paths, symlinks, and permissions that are common when using Windows for development.



### **Setting Up WSL2** 
Mac and Linux User can skip ahead to next chapter "Development Environment Setup"

1. **Install WSL2**: Open PowerShell as Administrator and run
   ```bash
   wsl --install
   ```
   Restart your system if prompted.

1. **Verify Installation**: Open a terminal and run:   
    ```bash
   wsl --list --verbose   
   ```
   Ensure that WSL2 is installed and running.

1. **Install Ubuntu 24.04:**
   ```bash
   wsl --install Ubuntu-24.04   
   ```



---

### **Development Environment Setup**
1. **Install Git in WSL**: Once inside WSL, ensure Git is installed:
   ```bash
      sudo apt update
      sudo apt install git
   ```
1. **Install Required Build Dependencies**
   ```bash
         sudo apt update
         sudo apt install -y build-essential libssl-dev zlib1g-dev \
            libbz2-dev libreadline-dev libsqlite3-dev curl \
            llvm libncursesw5-dev xz-utils tk-dev libxml2-dev \
            libxmlsec1-dev libffi-dev liblzma-dev
   ```

1. **Programming Language:** Install pyenv for managing Python versions (or just install python 3.12.0+ alternatively):
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
2. **Dependencies and Environment Management:** Install Poetry which will setup up a virtual environment with exactly the same dependencies as the one's of other contributors of this project:
   ```bash
      curl -sSL https://install.python-poetry.org | python3 -
   ```
   Add Poetry to your PATH environment variable:
   ```bash
      export PATH="$HOME/.local/bin:$PATH"   
   ```
   Restart your terminal.
   ```bash
      source ~/.bashrc   
   ```

1. **Download the Repo**: Clone the repo on a unix/linux system, **do not** use a mounted windows directory! If your path looks like this `/mnt/<drive_letter>/path/to/directory` you are likely to run into problems.
   ```bash
      git clone https://github.com/hilbertp/hypermvp
   ```

1. **Installing the Dependencies and Environment:** Run the Setup Script:

   ```bash
      source setup.sh   
   ```

### Returning to Work
Whenever you restart your system and want to resume working on the project, follow these steps:
1. Start WSL
   ```bash
      wsl   
   ```
1. Navigate to Your Project Directory
   ```bash
      cd ~/hypermvp   
   ```
1. Ensure Dependencies Are Installed
   ```bash
      poetry install
   ```
1. Activate the Virtual Environment
   ```bash
      source $(poetry env info --path)/bin/activate   
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

Would you like to collaborate to find a way to gain free energy to mine bitcoin and get paid additionally for stabilizing the power grid? Then Hyperion is the right project for you.