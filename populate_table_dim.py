import cx_Oracle

# Oracle DB credentials
username = 'team33_vishnu'
password = 'team33_vishnu'

# Oracle DSN
dsn = cx_Oracle.makedsn(
    "orcl-aws.c8sefhobaih4.ap-south-1.rds.amazonaws.com",
    1521,
    service_name="ORCL"
)

connection = None
cursor = None

try:
    connection = cx_Oracle.connect(username, password, dsn)
    cursor = connection.cursor()

    # --- 1. UPDATE EXISTING RECORDS ---
    update_query = """
    UPDATE Store_dim tgt
    SET 
        (Store_code, Store_name, City_name, State_name, Country_name, Customer_name) = (
            SELECT s.Store_code, s.Store_name, c.City_nm, st.State_nm, co.Country_nm, cust.Customer_name
            FROM Stores s
            JOIN Cityc c ON s.City_id = c.City_id
            JOIN States st ON c.State_id = st.State_id
            JOIN Countryc co ON st.Country_id = co.Country_id
            JOIN Customersc cust ON s.Store_id = cust.Store_id
            WHERE s.Store_id = tgt.Store_id AND cust.Customer_id = tgt.Customer_id
        )
    WHERE EXISTS (
        SELECT 1
        FROM Stores s
        JOIN Cityc c ON s.City_id = c.City_id
        JOIN States st ON c.State_id = st.State_id
        JOIN Countryc co ON st.Country_id = co.Country_id
        JOIN Customersc cust ON s.Store_id = cust.Store_id
        WHERE s.Store_id = tgt.Store_id AND cust.Customer_id = tgt.Customer_id AND (
            tgt.Store_code != s.Store_code OR
            tgt.Store_name != s.Store_name OR
            tgt.City_name != c.City_nm OR
            tgt.State_name != st.State_nm OR
            tgt.Country_name != co.Country_nm OR
            tgt.Customer_name != cust.Customer_name
        )
    )
    """
    cursor.execute(update_query)
    updated_rows = cursor.rowcount

    # --- 2. INSERT NEW RECORDS ---
    insert_query = """
    INSERT INTO Store_dim (
        Store_id, Store_code, Store_name,
        City_name, State_name, Country_name,
        Customer_id, Customer_name
    )
    SELECT 
        s.Store_id, s.Store_code, s.Store_name,
        c.City_nm, st.State_nm, co.Country_nm,
        cust.Customer_id, cust.Customer_name
    FROM Stores s
    JOIN Cityc c ON s.City_id = c.City_id
    JOIN States st ON c.State_id = st.State_id
    JOIN Countryc co ON st.Country_id = co.Country_id
    JOIN Customersc cust ON s.Store_id = cust.Store_id
    WHERE NOT EXISTS (
        SELECT 1 FROM Store_dim tgt
        WHERE tgt.Store_id = s.Store_id AND tgt.Customer_id = cust.Customer_id
    )
    """
    cursor.execute(insert_query)
    inserted_rows = cursor.rowcount

    connection.commit()

    # --- Summary Output ---
    if inserted_rows > 0:
        print(f" {inserted_rows} record(s) inserted into Store_dim.")
    if updated_rows > 0:
        print(f" {updated_rows} record(s) updated in Store_dim.")
    if inserted_rows == 0 and updated_rows == 0:
        print(" No changes made to Store_dim.")

except cx_Oracle.DatabaseError as e:
    print(" Database error occurred:", e)

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
