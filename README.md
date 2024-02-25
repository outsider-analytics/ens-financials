# Steakhouse Financial New Project Template

A template for creating repos to [manage your Dune queries](https://dune.mintlify.app/api-reference/crud/endpoint/create) and any [CSVs as Dune tables](https://dune.mintlify.app/api-reference/upload/endpoint/upload).

### Onboarding A New Client: Steps

1. **Fork the ENS Template on GitHub**

   - Begin by forking the ENS template on GitHub, then clone it to your preferred code editor, such as Visual Studio Code.

2. **Review SQL Files in the Queries Directory**

   - Explore the `queries` directory.
   - Remove any SQL files not relevant to the new project.
   - Ignore file names prefixed with the project name (e.g., `ens-`).

3. **Set Up a Python Virtual Environment**

   - Create a virtual environment: `python -m venv venv`.
   - Activate the virtual environment: `source venv/bin/activate`.

4. **Install Dependencies**

   - Ensure `pip` is installed in your virtual environment.
   - Install required dependencies: `pip install -r requirements.txt`.

5. **Configure the `.env` File**

   - Create a new `.env` file: `touch .env`.
   - Copy the contents from `.env.test` into the `.env` file.
   - Add your Dune API key to the `.env` file.

6. **Delete any unneeded queries in the queries folder**

   - Look at the end (for endaoment) and acc (for accounting) to delete
   - Don't get rid off acc_main as this is the interchange that all queries go through

7. **Start a New Project**

   - Execute the script to initiate a new project: `python new_project_start.py`.

8. **Name the New Project**

   - Keep the project name under 5 characters (it will be cut off otherswise with big query names).
   - Avoid dashes (`-`), underscores (`_`), or spaces to prevent formatting issues.
   - Select a concise yet distinctive name to prefix all query file names.

9. **Finalize the Setup**

   - A success message indicates completion.
   - Your queries will be integrated into Dune for further analysis and pipeline development.

10. **Develop Your Data Pipeline**

    - Utilize the provided files as a foundation for building and expanding your data pipeline in the uploads and downloads folders.

11. **Query Report**
    - Execute `python scripts/query_report.py` to generate a status report of all queries and upload them as a CSV to Dune.
