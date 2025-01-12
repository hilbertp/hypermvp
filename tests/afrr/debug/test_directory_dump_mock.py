import os

# Path to the directory to check
directory_path = "g:\\hyperMVP\\tests_data\\processed"

# Check if the directory is writable
if os.access(directory_path, os.W_OK):
    print(f"The directory {directory_path} is writable.")
    # Attempt to write a test file
    try:
        test_file_path = os.path.join(directory_path, "test_write_permissions.txt")
        with open(test_file_path, 'w') as test_file:
            test_file.write("This is a test file to check write permissions.")
        print(f"Successfully wrote to the file {test_file_path}.")
        # Clean up the test file
        os.remove(test_file_path)
    except Exception as e:
        print(f"Failed to write to the directory {directory_path}. Error: {e}")
else:
    print(f"The directory {directory_path} is not writable.")