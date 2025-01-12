import os

# Path to the directory to check
directory_path = "g:\\hyperMVP\\tests_data\\processed"

# Check if the directory is writable and list contents
if os.access(directory_path, os.W_OK):
    print(f"The directory {directory_path} is writable.")
    # List all files in the directory
    files = os.listdir(directory_path)
    if files:
        print(f"Files in {directory_path}:")
        for file in files:
            print(file)
    else:
        print(f"No files found in {directory_path}.")
else:
    print(f"The directory {directory_path} is not writable.")