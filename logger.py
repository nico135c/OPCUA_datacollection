import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime
import threading
import queue
import time

class PostgresLogger:
    def __init__(self, module_name, database_credentials, reset=True):
        self.module_name = module_name
        self.table_name = module_name.lower().replace(" ", "_") + "_logs"

        self.credentials = self._read_credentials(database_credentials)

        self.host=self.credentials["host"]
        self.db_name = self.credentials["database"]
        self.user = self.credentials["user"]
        self.password = self.credentials["password"]
        self.port = self.credentials.get("port", 5433)
        self.reset = reset

        # Queues
        self.cycle_queue = queue.Queue()
        self.state_queue = queue.Queue()

        # Prepare database
        self._ensure_database()
        if self.reset:
            self._reset_tables()

        # Ensure tables for this module
        self._ensure_log_table()
        self._ensure_states_table()

        # Worker threads
        self.stop_event = threading.Event()
        threading.Thread(target=self._worker_loop, daemon=True).start()
        threading.Thread(target=self._state_worker, daemon=True).start()

        print(f"[{self.module_name}] Logger ready (single DB, table='{self.table_name}', reset={reset})")

    def _read_credentials(self, path):
        creds = {}
        with open(path, "r") as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    creds[key] = value
        return creds

    # ----------------------------------------------------------------------
    # CONNECTION
    # ----------------------------------------------------------------------
    def _connect(self):
        try:
            return psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except Exception:
            print("[AUTH] Password failed, trying peer authentication...")
            return psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                host=self.host,
                port=self.port
            )

    # ----------------------------------------------------------------------
    # DATABASE + TABLE CREATION
    # ----------------------------------------------------------------------
    def _ensure_database(self):
        """Ensures the shared database exists."""
        conn = self._connect()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (self.db_name,))
        exists = cur.fetchone()

        if not exists:
            cur.execute(f"CREATE DATABASE {self.db_name};")
            print(f"[SETUP] Created shared database '{self.db_name}'")
        else:
            print(f"[SETUP] Shared database '{self.db_name}' exists")

        cur.close()
        conn.close()

    def _ensure_log_table(self):
        conn = self._connect()
        cur = conn.cursor()

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                enter_time      TIMESTAMPTZ NOT NULL,
                exit_time       TIMESTAMPTZ NOT NULL,
                order_number    TEXT NOT NULL,
                part_number     TEXT NOT NULL,
                order_position  TEXT NOT NULL,
                operation_number TEXT NOT NULL,
                resource_id     TEXT NOT NULL
            );
        """)

        conn.commit()
        cur.close()
        conn.close()

    def _ensure_states_table(self):
        conn = self._connect()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS station_states (
                station_name TEXT PRIMARY KEY,
                state TEXT NOT NULL
            );
        """)

        conn.commit()
        cur.close()
        conn.close()

    # ----------------------------------------------------------------------
    # RESET
    # ----------------------------------------------------------------------
    def _reset_tables(self):
        """Drops ONLY this module's log table."""
        conn = self._connect()
        cur = conn.cursor()

        # Drop only this module's table
        cur.execute(f"DROP TABLE IF EXISTS {self.table_name} CASCADE;")
        print(f"[RESET] Dropped table '{self.table_name}'")

        # Note: We do NOT drop station_states here
        # unless you explicitly want a global reset of states.

        conn.commit()
        cur.close()
        conn.close()


    # ----------------------------------------------------------------------
    # LOGGING API
    # ----------------------------------------------------------------------
    def log(self, enter_time, exit_time, order, part, pos, op, res):
        print(f"[LOG] [{self.module_name}] [{datetime.now()}] [{enter_time}] [{exit_time}] [{order}] [{part}] [{pos}] [{op}] [{res}]")
        try:
            self.cycle_queue.put((enter_time, exit_time, order, part, pos, op, res))
        except Exception as e:
            print(f"[LOGGER WARNING] Failed to queue log event: {e}")

    def log_station_state(self, state):
        print(f"[STATE] [{self.module_name}] → {state}")
        self.state_queue.put((self.module_name, state))

    def log_info(self, msg):
        print(f"[INFO] [{self.module_name}] {msg}")

    # ----------------------------------------------------------------------
    # WORKERS
    # ----------------------------------------------------------------------
    def _worker_loop(self):
        conn = None
        cur = None

        while not self.stop_event.is_set():
            try:
                if conn is None:
                    conn = self._connect()
                    cur = conn.cursor()

                enter_time, exit_time, order, part, pos, op, res = \
                    self.cycle_queue.get(timeout=0.5)

                cur.execute(f"""
                    INSERT INTO {self.table_name} (
                        enter_time, exit_time, order_number, part_number,
                        order_position, operation_number, resource_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (enter_time, exit_time, order, part, pos, op, res))

                conn.commit()

            except queue.Empty:
                continue
            except Exception as e:
                print(f"[LOGGER ERROR] {e}")
                time.sleep(1)
                conn = None

    def _state_worker(self):
        conn = None
        cur = None

        while not self.stop_event.is_set():
            try:
                if conn is None:
                    conn = self._connect()
                    cur = conn.cursor()

                station_name, state = self.state_queue.get(timeout=0.5)

                cur.execute("""
                    INSERT INTO station_states (station_name, state)
                    VALUES (%s, %s)
                    ON CONFLICT (station_name)
                    DO UPDATE SET state = EXCLUDED.state;
                """, (station_name, state))

                conn.commit()

            except queue.Empty:
                continue
            except Exception as e:
                print(f"[LOGGER ERROR - STATION STATE] {e}")
                conn = None

    # ----------------------------------------------------------------------
    def stop(self):
        self.stop_event.set()
