import os

# Define the directory path
directory_path = "queries/"

# Check if the directory exists
if os.path.exists(directory_path) and os.path.isdir(directory_path):
    # List all files in the directory
    for filename in os.listdir(directory_path):
        # Check if the filename contains '-'
        if "-" in filename:
            # Generate the new filename by replacing '-' with '_'
            new_filename = filename.replace("-", "_")
            # Generate full file paths
            old_file_path = os.path.join(directory_path, filename)
            new_file_path = os.path.join(directory_path, new_filename)
            # Rename the file
            os.rename(old_file_path, new_file_path)
            print(f'Renamed "{filename}" to "{new_filename}"')
else:
    print(f"The directory {directory_path} does not exist or is not a directory.")
