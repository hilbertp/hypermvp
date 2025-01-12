import os

# Define the target directory
target_directory = "g:\\hyperMVP\\tests_data\\processed"

# Ensure the directory exists
os.makedirs(target_directory, exist_ok=True)
print(f"Directory ensured: {target_directory}")

# Define the filename and content
filename = os.path.join(target_directory, "test_file.txt")
content = "asd"

# Write the content to the file
with open(filename, "w") as file:
    file.write(content)
print(f"File created: {filename}")

# Verify file existence
if os.path.isfile(filename):
    print(f"File {filename} exists after creation.")
else:
    print(f"File {filename} does not exist after creation.")

# List the directory contents
files = os.listdir(target_directory)
print(f"Files in {target_directory}:")
for file in files:
    print(file)