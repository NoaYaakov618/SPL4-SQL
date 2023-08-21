import sqlite3
import atexit
import os
import sys

DBExist = os.path.isfile('database.db')
if DBExist:
    os.remove('database.db')

_conn = sqlite3.connect('database.db')

cur = _conn.cursor()


def _close_db(conn):
    conn.commit()
    conn.close()


atexit.register(lambda: _close_db(_conn))


class Vaccine:
    def __init__(self, id, date, supplier, quantity):
        self.id = id
        self.date = date
        self.supplier = supplier
        self.quantity = quantity


class Supplier:
    def __init__(self, id, name, logistic):
        self.id = id
        self.name = name
        self.logistic = logistic


class Clinic:
    def __init__(self, id, location, demand, logistic):
        self.id = id
        self.location = location
        self.demand = demand
        self.logistic = logistic


class Logistic:
    def __init__(self, id, name, count_sent, count_received):
        self.id = id
        self.name = name
        self.count_sent = count_sent
        self.count_received = count_received

    # DAO


class _Vaccines:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, vaccine):
        my_execute(self._conn, """
               INSERT INTO vaccines (id, date, supplier, quantity) VALUES (?, ?, ?, ?)
           """, [vaccine.id, vaccine.date, vaccine.supplier, vaccine.quantity])

    def find(self, vaccine_id):
        c = self._conn.cursor()
        c.execute("""
            SELECT id, date, supplier, quantity FROM vaccines WHERE id = ?
        """, [vaccine_id])
        return Vaccine(*c.fetchone())

    def sum_quantity(self):
        c = self._conn.cursor()
        c.execute("""SELECT SUM (quantity) FROM vaccines """)
        return int(*c.fetchone())

    def select_all(self):
        c = self._conn.cursor()
        c.execute("""SELECT * FROM vaccines""")
        all = c.fetchall()
        return all

    def order_by(self):
        c = self._conn.cursor()
        c.execute("""SELECT * FROM vaccines ORDER BY date""")
        return c.fetchall()

    def delete(self, date):
        my_execute(self._conn, """DELETE FROM vaccines WHERE date = ? """, [date])

    def update_quantity(self, new_quantity, date):
        my_execute(self._conn, """
                UPDATE vaccines SET quantity = ? WHERE date = ?
                """, [new_quantity, date])


class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, supplier):
        my_execute(self._conn, """
                INSERT INTO suppliers (id, name, logistic) VALUES (?, ?, ?) 
        """, [supplier.id, supplier.name, supplier.logistic])

    def find(self, name):
        c = self._conn.cursor()
        c.execute("""
                SELECT id,name,logistic FROM suppliers WHERE name = ?
            """, [name])

        return Supplier(*c.fetchone())


class _Clinics:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, clinic):
        my_execute(self._conn, """
            INSERT INTO clinics (id, location, demand, logistic) VALUES (?, ?, ?, ?)
        """, [clinic.id, clinic.location, clinic.demand, clinic.logistic])

    def find(self, location):
        c = self._conn.cursor()
        c.execute("""
                 SELECT id,location,demand,logistic FROM clinics WHERE location = ?
             """, [location])
        return Clinic(*c.fetchone())

    def update_demand(self, new_demand, clinic_location):
        my_execute(self._conn, """
                UPDATE clinics SET demand = ? WHERE location = ?
                """, [new_demand, clinic_location])

    def sun_demand(self):
        c = self._conn.cursor()
        c.execute("""SELECT SUM (demand) FROM clinics """)
        return int(*c.fetchone())


def my_execute(conn, cmd, params):
    conn.execute(cmd, params)
    conn.commit()


class _Logistics:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, logistic):
        my_execute(self._conn, """
            INSERT INTO logistics (id, name, count_sent, count_received) VALUES (?, ?, ?, ?)
         """, [logistic.id, logistic.name, logistic.count_sent, logistic.count_received])

    def find(self, id):
        c = self._conn.cursor()
        c.execute("""
                 SELECT id,name,count_sent, count_received FROM logistics WHERE id = ?
             """, [id])
        return Logistic(*c.fetchone())

    def update_count_received(self, new_quantity, logistic_id):
        my_execute(self._conn, """
                UPDATE logistics SET count_received = ? WHERE id = ?
                """, [new_quantity, logistic_id])

    def update_count_sent(self, new_quantity, logistic_id):
        my_execute(self._conn, """
                UPDATE logistics SET count_sent = ? WHERE id = ?
                """, [new_quantity, logistic_id])

    def sun_total_received(self):
        c = self._conn.cursor()
        c.execute("""SELECT SUM (count_received) FROM logistics """)

        return int(*c.fetchone())

    def sun_total_sent(self):
        c = self._conn.cursor()
        c.execute("""SELECT SUM (count_sent) FROM logistics """)

        return int(*c.fetchone())

    # Repository


class _Repository:
    def __init__(self):
        self._conn = sqlite3.connect('database.db')
        self.vaccines = _Vaccines(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.clinics = _Clinics(self._conn)
        self.logistics = _Logistics(self._conn)
        atexit.register(lambda: _close_db(self._conn))

    def create_tables(self):
        self._conn.executescript("""
        CREATE TABLE vaccines (
        id INT PRIMARY KEY ,
        date TEXT NOT NULL,
        supplier INT NOT NULL,
        quantity INT NOT NULL,
        FOREIGN KEY(supplier) REFERENCES suppliers(id)
        );

         CREATE TABLE suppliers (
         id INT PRIMARY KEY ,
         name TEXT NOT NULL,
         logistic INT NOT NULL,
         FOREIGN KEY(logistic) REFERENCES Logistic(id)
        );


        CREATE TABLE clinics (
        id INT PRIMARY KEY ,
        location TEXT NOT NULL,
        demand INT NOT NULL,
        logistic INT NOT NULL,
        FOREIGN KEY(logistic) REFERENCES Logistics(id)
        );

        CREATE TABLE logistics (
        id INT PRIMARY KEY ,
        name TEXT NOT NULL,
        count_sent INT NOT NULL,
        count_received INT NOT NULL
        );
        """)


repo = _Repository()

# atexit.register(repo.close)
