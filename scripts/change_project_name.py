# change_project_name.py
import os
import shutil
import sys


def replace_prefix_in_file(file_path, old_prefix, new_prefix):
    # Read the original content of the file
    with open(file_path, "r") as file:
        content = file.read()

    # Replace the old prefix with the new prefix
    updated_content = content.replace(old_prefix, new_prefix)

    # Write the updated content back to the file
    with open(file_path, "w") as file:
        file.write(updated_content)


def rename_files_in_queries(new_prefix, queries_path):
    for root, dirs, files in os.walk(queries_path):
        for file in files:
            # New logic to remove '___' and after, if present
            base_name = file.split("___")[0]
            # Remove extension for further processing
            name, ext = os.path.splitext(base_name)
            if "-" in name:
                # Keep the part after the first "-"
                old_prefix = name.split("_", 1)[0]
                parts = name.split("_", 1)
                new_file_name = new_prefix + "_" + parts[1] + ext + ".sql"
            else:
                new_file_name = new_prefix + "_" + name + ext + ".sql"
                old_prefix = name

            # Construct the full old and new file paths
            old_file_path = os.path.join(root, file)
            new_file_path = os.path.join(root, new_file_name)

            # Rename the file
            shutil.move(old_file_path, new_file_path)
            print(f"Renamed '{file}' to '{new_file_name}'")

            # Replace prefix within the file
            replace_prefix_in_file(new_file_path, old_prefix, new_prefix)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python change_project_name.py <new_prefix>")
        sys.exit(1)

    new_prefix = sys.argv[1]
    queries_path = "queries/"  # Adjust relative path as necessary

    # Check if the queries directory exists
    if not os.path.exists(queries_path):
        print(f"The directory {queries_path} does not exist.")
        sys.exit(1)
    else:
        rename_files_in_queries(new_prefix, queries_path)

    report_path = "scripts/query_report.py"

    # Read the content of the file
    with open(report_path, "r") as file:
        lines = file.readlines()

    # Modify the line that contains the project_name assignment
    for i, line in enumerate(lines):
        if line.strip().startswith("project_name ="):
            lines[i] = f'project_name = "{new_prefix}"\n'
            break  # Stop the loop once the line is found and modified

    # Write the modified content back to the file
    with open(report_path, "w") as file:
        file.writelines(lines)
