##  Postgres Database Interaction Script

This Python script provides a class-based approach to interacting with a Postgres database. It utilizes the `psycopg2` library to establish connections, manage tables, and execute queries.

**Requirements:**

* Python 3.x
* `psycopg2` library (installable via `pip install psycopg2`)
* `.env` file (optional) to store database credentials securely

**Setup:**

1. Install `psycopg2`:
   

   ```bash
   pip install psycopg2
   ```
2. Create a file named `.env` in the same directory as this script (optional). Add environment variables for your Postgres credentials:

   ```
   POSTGRES_USERNAME=your_username
   POSTGRES_PASSWORD=your_password
   POSTGRES_HOST=your_host  # defaults to localhost
   POSTGRES_PORT=your_port  # defaults to 5432
   POSTGRES_DATABASE=your_database
   ```
3. Replace `"your_` values with your actual credentials.

**Usage:**

1. Modify the `env_path` variable in the script to point to your `.env` file location (if using).
2. Update the table schema (`table_schema`) dictionary to match your table structure.
3. Adjust the sample data (`data`) dictionary for your specific inserts.
4. Run the script:
   

   ```bash
   python postgres_script.py
   ```

**Functionality:**

* Connect to a Postgres database using environment variables.
* Create a database (optional).
* Create a table based on a provided schema.
* Insert data into a table.
* Execute a SQL query and retrieve results.
* Disconnect from the database.

**Note:**

* This script provides a basic example. You can extend it to include additional functionalities like data deletion, updates, and more complex queries.
* Remember to modify the script according to your specific database configuration and requirements.

**Additional Resources:**

* `psycopg2` documentation: [https://www.psycopg.org/docs/install.html](https://www.psycopg.org/docs/install.html)
* Postgres documentation: [https://www.postgresql.org/docs/current/index.html](https://www.postgresql.org/docs/current/index.html)
