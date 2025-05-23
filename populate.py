import sqlite3
import random
from faker import Faker
from datetime import datetime, timedelta
import os 

DB_NAME = 'user_transactions.db'
NUM_PEOPLE = 12 
MIN_TRANSACTIONS_PER_PERSON = 5
MAX_TRANSACTIONS_PER_PERSON = 25
POSSIBLE_LOCATIONS = ["New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Mumbai", "Online", "Arcot", "Chennai"]

fake = Faker()

def setup_database():
    """Creates the database and tables if they don't exist."""
    db_exists = os.path.exists(DB_NAME)
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create PeopleInformation table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PeopleInformation (
            person_id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE,
            phone_number TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Create TransactionData table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TransactionData (
            transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER NOT NULL,
            transaction_date TIMESTAMP NOT NULL,
            amount REAL NOT NULL,
            location TEXT,
            description TEXT,
            FOREIGN KEY (person_id) REFERENCES PeopleInformation(person_id)
        )
    ''')

    conn.commit()
    conn.close()
    if not db_exists:
        print(f"Database '{DB_NAME}' and tables created successfully.")
    else:
        print(f"Database '{DB_NAME}' already exists. Tables checked/created.")

def populate_dummy_data(num_people=NUM_PEOPLE):
    """Populates the database with dummy data."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM PeopleInformation")
    if cursor.fetchone()[0] > 0:
        print("Database already contains data. Skipping population.")
        conn.close()
        return

    print(f"Populating database with {num_people} people and their transactions...")
    people_added = 0
    while people_added < num_people:
        try:
            # Generate Person Data
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = fake.unique.email() # Ensure email is unique
            phone = fake.phone_number()

            # Insert Person
            cursor.execute('''
                INSERT INTO PeopleInformation (first_name, last_name, email, phone_number)
                VALUES (?, ?, ?, ?)
            ''', (first_name, last_name, email, phone))

            person_id = cursor.lastrowid # Get the ID of the newly inserted person
            people_added += 1

            # Generate Transaction Data for this person
            num_transactions = random.randint(MIN_TRANSACTIONS_PER_PERSON, MAX_TRANSACTIONS_PER_PERSON)
            for _ in range(num_transactions):
                # Generate realistic date within the last 2 years
                transaction_date = fake.date_time_between(start_date="-2y", end_date="now")
                # Ensure timezone info is removed if present, as SQLite might handle it differently
                transaction_date = transaction_date.replace(tzinfo=None)
                amount = round(random.uniform(5.0, 1000.0), 2) # Transaction amount between 5 and 1000
                location = random.choice(POSSIBLE_LOCATIONS)
                description = fake.sentence(nb_words=5) # Short description

                cursor.execute('''
                    INSERT INTO TransactionData (person_id, transaction_date, amount, location, description)
                    VALUES (?, ?, ?, ?, ?)
                ''', (person_id, transaction_date, amount, location, description))

        except sqlite3.IntegrityError as e:
             # This might happen if faker generates a duplicate email despite 'unique'
            print(f"Skipping duplicate entry error: {e}. Trying next person.")
            conn.rollback() # Rollback the failed person insert
        except Exception as e:
            print(f"An error occurred: {e}")
            conn.rollback() # Rollback any partial inserts for the current person


    conn.commit()
    conn.close()
    print("Dummy data population complete.")

# --- Query Functions ---

def get_transactions_by_month_and_name(first_name, last_name, month):
    """
    Fetches all transactions for a given person in a specific month.
    Args:
        first_name (str): First name of the person.
        last_name (str): Last name of the person.
        month (int): The month number (1-12).
    Returns:
        list: A list of tuples representing the transactions, or empty list if none found.
              Each tuple contains (transaction_id, transaction_date, amount, location, description).
    """
    conn = sqlite3.connect(DB_NAME)
    # Return rows as dictionary-like objects
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Format month with leading zero if needed (e.g., 1 -> '01')
    month_str = f"{month:02d}"

    cursor.execute('''
        SELECT td.transaction_id, td.transaction_date, td.amount, td.location, td.description
        FROM TransactionData td
        JOIN PeopleInformation pi ON td.person_id = pi.person_id
        WHERE pi.first_name = ? AND pi.last_name = ?
          AND strftime('%m', td.transaction_date) = ?
        ORDER BY td.transaction_date
    ''', (first_name, last_name, month_str))

    transactions = cursor.fetchall()
    conn.close()
    # Convert Row objects to standard dictionaries for easier use
    return [dict(row) for row in transactions]

def get_transactions_by_day_and_name(first_name, last_name, date_str):
    """
    Fetches all transactions for a given person on a specific day.
    Args:
        first_name (str): First name of the person.
        last_name (str): Last name of the person.
        date_str (str): The date string in 'YYYY-MM-DD' format.
    Returns:
        list: A list of tuples representing the transactions, or empty list if none found.
              Each tuple contains (transaction_id, transaction_date, amount, location, description).
    """
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Validate date format (basic check)
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        print("Error: Invalid date format. Please use YYYY-MM-DD.")
        conn.close()
        return []

    cursor.execute('''
        SELECT td.transaction_id, td.transaction_date, td.amount, td.location, td.description
        FROM TransactionData td
        JOIN PeopleInformation pi ON td.person_id = pi.person_id
        WHERE pi.first_name = ? AND pi.last_name = ?
          AND date(td.transaction_date) = ?
        ORDER BY td.transaction_date
    ''', (first_name, last_name, date_str))

    transactions = cursor.fetchall()
    conn.close()
    return [dict(row) for row in transactions]

def get_total_transaction_amount_by_name(first_name, last_name):
    """
    Calculates the total amount spent in transactions by a given person.
    Args:
        first_name (str): First name of the person.
        last_name (str): Last name of the person.
    Returns:
        float: The total amount spent, or 0.0 if the person or transactions are not found.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT SUM(td.amount)
        FROM TransactionData td
        JOIN PeopleInformation pi ON td.person_id = pi.person_id
        WHERE pi.first_name = ? AND pi.last_name = ?
    ''', (first_name, last_name))

    result = cursor.fetchone()
    conn.close()

    # fetchone() returns a tuple (total,) or (None,) if no matching rows
    if result and result[0] is not None:
        return round(result[0], 2)
    else:
        return 0.0
        
# --- Alternative: Get total transaction COUNT by name ---
# def get_total_transaction_count_by_name(first_name, last_name):
#     """Calculates the total number of transactions made by a given person."""
#     conn = sqlite3.connect(DB_NAME)
#     cursor = conn.cursor()
#     cursor.execute('''
#         SELECT COUNT(td.transaction_id)
#         FROM TransactionData td
#         JOIN PeopleInformation pi ON td.person_id = pi.person_id
#         WHERE pi.first_name = ? AND pi.last_name = ?
#     ''', (first_name, last_name))
#     result = cursor.fetchone()
#     conn.close()
#     return result[0] if result else 0


def list_all_people():
    """
    Fetches a list of all people in the system.
    Returns:
        list: A list of dictionaries, each representing a person.
              Keys: 'person_id', 'first_name', 'last_name', 'email', 'phone_number'
    """
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row # Return rows as dictionary-like objects
    cursor = conn.cursor()

    cursor.execute('''
        SELECT person_id, first_name, last_name, email, phone_number
        FROM PeopleInformation
        ORDER BY last_name, first_name
    ''')

    people = cursor.fetchall()
    conn.close()
    # Convert Row objects to standard dictionaries
    return [dict(row) for row in people]


def get_transactions_by_location(location):
    """
    Fetches all transactions that occurred at a specific location.
    Args:
        location (str): The location name to filter by.
    Returns:
        list: A list of dictionaries representing the transactions including person's name.
              Keys: 'transaction_id', 'first_name', 'last_name', 'transaction_date',
                    'amount', 'location', 'description'
    """
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('''
        SELECT
            td.transaction_id,
            pi.first_name,
            pi.last_name,
            td.transaction_date,
            td.amount,
            td.location,
            td.description
        FROM TransactionData td
        JOIN PeopleInformation pi ON td.person_id = pi.person_id
        WHERE td.location = ?
        ORDER BY td.transaction_date DESC
    ''', (location,))

    transactions = cursor.fetchall()
    conn.close()
    return [dict(row) for row in transactions]


# --- Main Execution ---
if __name__ == "__main__":
    # 1. Setup the database and tables
    setup_database()

    # 2. Populate with dummy data (only if empty)
    populate_dummy_data()

    print("\n--- Database Operations Demo ---")

    # 3. List all people
    print("\nListing all people in the system:")
    all_people = list_all_people()
    if all_people:
        for person in all_people[:5]: # Print first 5 for brevity
            print(f"  ID: {person['person_id']}, Name: {person['first_name']} {person['last_name']}, Email: {person['email']}")
        if len(all_people) > 5:
            print(f"  ... and {len(all_people) - 5} more.")

        # Select a random person for further demos
        demo_person = random.choice(all_people)
        demo_first_name = demo_person['first_name']
        demo_last_name = demo_person['last_name']
        print(f"\n--- Using '{demo_first_name} {demo_last_name}' for specific demos ---")

        # 4. Get transactions by month and name
        demo_month = random.randint(1, 12)
        print(f"\nTransactions for {demo_first_name} {demo_last_name} in Month {demo_month}:")
        month_transactions = get_transactions_by_month_and_name(demo_first_name, demo_last_name, demo_month)
        if month_transactions:
            for tx in month_transactions:
                print(f"  - {tx['transaction_date']} | ${tx['amount']:.2f} at {tx['location']} ({tx['description']})")
        else:
            print("  No transactions found for this month.")

        # 5. Get transactions by day and name (Use a date from the previous query if possible)
        demo_date = ""
        if month_transactions:
            # Try to get a date from the existing transactions for a higher chance of match
            try:
               first_tx_date_str = month_transactions[0]['transaction_date']
               # SQLite stores timestamps sometimes with fractional seconds, remove them
               if '.' in first_tx_date_str:
                   first_tx_date_str = first_tx_date_str.split('.')[0]
               # Extract just the date part
               demo_date = datetime.strptime(first_tx_date_str, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            except (ValueError, KeyError, IndexError):
                 # Fallback to a generic recent date if parsing fails
                 demo_date = (datetime.now() - timedelta(days=random.randint(10, 60))).strftime('%Y-%m-%d')
        else:
             demo_date = (datetime.now() - timedelta(days=random.randint(10, 60))).strftime('%Y-%m-%d')


        print(f"\nTransactions for {demo_first_name} {demo_last_name} on Day {demo_date}:")
        day_transactions = get_transactions_by_day_and_name(demo_first_name, demo_last_name, demo_date)
        if day_transactions:
            for tx in day_transactions:
                 print(f"  - {tx['transaction_date']} | ${tx['amount']:.2f} at {tx['location']} ({tx['description']})")
        else:
            print("  No transactions found for this specific day.")

        # 6. Get total transaction amount by name
        total_amount = get_total_transaction_amount_by_name(demo_first_name, demo_last_name)
        print(f"\nTotal transaction amount for {demo_first_name} {demo_last_name}: ${total_amount:.2f}")

    else:
        print("No people found in the database. Cannot run specific demos.")

    # 7. Get transactions by location
    demo_location = random.choice(POSSIBLE_LOCATIONS)
    print(f"\nTransactions at location '{demo_location}':")
    location_transactions = get_transactions_by_location(demo_location)
    if location_transactions:
        for tx in location_transactions[:5]: # Show first 5 for brevity
            print(f"  - {tx['transaction_date']} | {tx['first_name']} {tx['last_name']} | ${tx['amount']:.2f} ({tx['description']})")
        if len(location_transactions) > 5:
             print(f"  ... and {len(location_transactions) - 5} more.")
    else:
        print(f"  No transactions found for location '{demo_location}'.")

    print("\n--- Demo Complete ---")