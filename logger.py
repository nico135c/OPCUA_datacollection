import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime
import threading
import queue
import time

class PostgresConnection:
    def __init__(self, credentials_path="database_credentials_local.txt"):
        self.credentials = self._read_credentials(credentials_path)

        self.host = self.credentials["host"]
        self.db_name = self.credentials["database"]
        self.user = self.credentials["user"]
        self.password = self.credentials.get("password")
        self.port = int(self.credentials.get("port", 5433))

    # -------------------------------------------------
    # CREDENTIALS
    # -------------------------------------------------
    def _read_credentials(self, path):
        creds = {}
        with open(path, "r") as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    creds[key] = value
        return creds

    # -------------------------------------------------
    # CONNECTION HANDLING
    # -------------------------------------------------
    def connect(self):
        try:
            return psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except Exception:
            # fallback for peer auth
            return psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                host=self.host,
                port=self.port
            )

    def execute(self, query, params=None, commit=True):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(query, params)
        if commit:
            conn.commit()
        cur.close()
        conn.close()

    def log_info(self, module_name, msg):
        print(f"[INFO] [{module_name}] {msg}")

    # -------------------------------------------------
    # DATABASE / TABLE SETUP
    # -------------------------------------------------
    def ensure_database(self):
        conn = self.connect()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (self.db_name,))
        exists = cur.fetchone()

        if not exists:
            cur.execute(f"CREATE DATABASE {self.db_name};")
            print(f"[SETUP] Created database '{self.db_name}'")
        else:
            print(f"[SETUP] Database '{self.db_name}' exists")

        cur.close()
        conn.close()

    def drop_table(self, table_name):
        self.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")


class PostgresLogger:
    def __init__(self, module_name, credentials_path="database_credentials_local.txt",reset=True):
        self.module_name = module_name
        self.table_name = module_name.lower().replace(" ", "_") + "_logs"
        self.db = PostgresConnection(credentials_path)
        self.reset = reset

        self.cycle_queue = queue.Queue()
        self.state_queue = queue.Queue()

        # Setup
        if reset:
            self.db.drop_table(self.table_name)

        self._ensure_log_table()
        self._ensure_states_table()

        # Workers
        self.stop_event = threading.Event()
        threading.Thread(target=self._cycle_worker, daemon=True).start()
        threading.Thread(target=self._state_worker, daemon=True).start()

        print(f"[{self.module_name}] Logger ready")

    # -------------------------------------------------
    # TABLES
    # -------------------------------------------------
    def _ensure_log_table(self):
        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                enter_time TIMESTAMPTZ NOT NULL,
                exit_time TIMESTAMPTZ NOT NULL,
                cycle_time BIGINT NOT NULL,
                order_number TEXT NOT NULL,
                part_number TEXT NOT NULL,
                order_position TEXT NOT NULL,
                operation_number TEXT NOT NULL,
                resource_id TEXT NOT NULL
            );
        """)

    def _ensure_states_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS station_states (
                station_name TEXT PRIMARY KEY,
                state TEXT NOT NULL
            );
        """)

    # -------------------------------------------------
    # PUBLIC API
    # -------------------------------------------------
    def log(self, enter_time, exit_time, cycle_time, order, part, pos, op, res):
        print(f"[LOG] [{self.module_name}] {datetime.now()}")
        self.cycle_queue.put((enter_time, exit_time, cycle_time, order, part, pos, op, res))

    def log_station_state(self, state):
        print(f"[STATE] [{self.module_name}] → {state}")
        self.state_queue.put((self.module_name, state))

    def log_info(self, msg):
        print(f"[INFO] [{self.module_name}] {msg}")

    # -------------------------------------------------
    # WORKERS
    # -------------------------------------------------
    def _cycle_worker(self):
        conn = None
        cur = None

        while not self.stop_event.is_set():
            try:
                if conn is None:
                    conn = self.db.connect()
                    cur = conn.cursor()

                data = self.cycle_queue.get(timeout=0.5)

                cur.execute(f"""
                    INSERT INTO {self.table_name} (
                        enter_time, exit_time, cycle_time,
                        order_number, part_number,
                        order_position, operation_number, resource_id
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
                """, data)

                conn.commit()

            except queue.Empty:
                continue
            except Exception as e:
                print(f"[LOGGER ERROR] {e}")
                conn = None
                time.sleep(1)

    def _state_worker(self):
        conn = None
        cur = None

        while not self.stop_event.is_set():
            try:
                if conn is None:
                    conn = self.db.connect()
                    cur = conn.cursor()

                station, state = self.state_queue.get(timeout=0.5)

                cur.execute("""
                    INSERT INTO station_states (station_name, state)
                    VALUES (%s, %s)
                    ON CONFLICT (station_name)
                    DO UPDATE SET state = EXCLUDED.state;
                """, (station, state))

                conn.commit()

            except queue.Empty:
                continue
            except Exception as e:
                print(f"[LOGGER STATE ERROR] {e}")
                conn = None

    # -------------------------------------------------
    def stop(self):
        self.stop_event.set()
