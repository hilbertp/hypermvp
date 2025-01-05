from setuptools import setup, find_packages

setup(
    name="hypermvp",
    version="0.1",
    packages=find_packages("src"),  # Locate all packages in the "src" directory
    package_dir={"": "src"},  # "src" is the root for all packages
    install_requires=[
        "pandas>=1.0.0",
        "pytest>=7.1.0",
        # Add other dependencies if needed
    ],
)