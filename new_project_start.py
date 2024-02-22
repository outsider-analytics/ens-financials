import subprocess
import re
import os


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

    # Assuming change_project_name.py is in the 'scripts' folder and callable
    script_path = os.path.join("scripts", "change_project_name.py")
    subprocess.run(["python", script_path, new_prefix], check=True)


if __name__ == "__main__":
    main()
