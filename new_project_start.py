import subprocess
import re
import os


def update_queries_yml():
    # Path to queries.yml in the root directory
    yml_path = os.path.join(os.getcwd(), "queries.yml")
    with open(yml_path, "w") as file:
        file.write("query_ids:\n")
    print("queries.yml has been reset.")


def main():
    print("🎉 Setting up a new project for a Steakhouse Financial Dune Dashboard! 🎉")
    while True:
        new_prefix = input(
            "Please give the new project name (1-10 chars, no spaces, ___, or -): "
        )
        if 1 < len(new_prefix) <= 10 and not re.search("[ _\-]", new_prefix):
            break
        else:
            print("Invalid project name. Make sure it meets the criteria.")

    # Replace queries.yml content
    update_queries_yml()

    # Call change_project_name.py script with new prefix
    change_script_path = os.path.join("scripts", "change_project_name.py")
    subprocess.run(["python", change_script_path, new_prefix], check=True)

    # Call create_queries.py script to create and update queries
    create_queries_script_path = os.path.join("scripts", "create_queries.py")
    subprocess.run(["python", create_queries_script_path], check=True)

    # Call push_to_dune.py script to push changes
    push_script_path = os.path.join("scripts", "push_to_dune.py")
    subprocess.run(["python", push_script_path], check=True)

    print("Your template has been added to Dune successfully!")


if __name__ == "__main__":
    main()
