from opcua import Client, ua
import time
import threading
from datetime import datetime
from logger import PostgresLogger, PostgresConnection

class OPCUAFestoModule:
    """
    Represents a Festo OPC-UA module.
    Handles connection, disconnection, and data access via OPC-UA.
    """

    def __init__(self, module_name: str, ip_address: str, port: int = 4840):
        self.module_name = module_name
        self.ip_address = ip_address
        self.port = port
        self.client = None
        self.endpoint = f"opc.tcp://{self.ip_address}:{self.port}"
        self.logger = PostgresLogger(self.module_name)

    def connect(self):
        try:
            self.client = Client(self.endpoint)
            self.client.connect()
            self.logger.log_info(f"Connected to {self.endpoint}")
        except Exception as e:
            self.logger.log_info(f"Failed to connect: {e}")
            self.client = None

    def disconnect(self):
        if self.client:
            try:
                self.client.disconnect()
                self.logger.log_info(f"Disconnected from server")
            except Exception as e:
                self.logger.log_info(f"Error during disconnect: {e}")
            self.client = None

    def get_value(self, node_id: str):
        if not self.client:
            self.logger.log_info(f"Not connected.")
            return None
        try:
            node = self.client.get_node(node_id)
            data_val = node.get_data_value()
            val = data_val.Value.Value
            return val
        except ua.UaError as e:
            self.logger.log_info(f"UA Error: {e}")
        except Exception as e:
            self.logger.log_info(f"Read Error: {e}")
        return None

    def set_value(self, node_id: str, value, value_type=ua.VariantType.Boolean):
        if not self.client:
            self.logger.log_info(f"Not connected.")
            return
        try:
            node = self.client.get_node(node_id)
            node.set_value(ua.Variant(value, value_type))
            self.logger.log_info(f"Wrote {value} to {node_id}")
        except ua.UaError as e:
            self.logger.log_info(f"UA Error: {e}")
        except Exception as e:
            self.logger.log_info(f"Write Error: {e}")


class OPCUAHandler:
    def __init__(self, module_list):
        self.modules = module_list
        self.mes_logger = PostgresConnection()
        self.table_name = "system_info"
        self.threads = []
        self.stop_event = threading.Event()

        self.module_states = {}
        self.module_states_lock = threading.Lock()

        self.system_downtime_start = None

        self.mes_logger.ensure_database()

        self.mes_logger.execute(f"DROP TABLE IF EXISTS {self.table_name};")

        self.mes_logger.execute(f"""
        CREATE TABLE {self.table_name} (
            system_start TIMESTAMPTZ NOT NULL,
            total_downtime BIGINT NOT NULL
        );
        """)

        self.mes_logger.execute(f"""
        INSERT INTO {self.table_name} (system_start, total_downtime)
        VALUES (NOW(), 0);
        """)


        for module in self.modules:
            module.connect()

        self.endpoints = {
            "xReady": 'ns=2;s=|var|CECC-LK.Application.FBs.stpStopper1.stAppState.xReady',
            "ONo" : 'ns=2;s=|var|CECC-LK.Application.FBs.stpStopper1.stRfidData.stMesData.udiONo',
            "PNo" : 'ns=2;s=|var|CECC-LK.Application.FBs.stpStopper1.stRfidData.stMesData.udiPNo',
            "OPos" : 'ns=2;s=|var|CECC-LK.Application.FBs.stpStopper1.stRfidData.stMesData.uiOPos',
            "OpNo" : 'ns=2;s=|var|CECC-LK.Application.FBs.stpStopper1.stRfidData.stMesData.uiOpNo',
            "ResourceId" : 'ns=2;s=|var|CECC-LK.Application.FBs.stpStopper1.stRfidData.stMesData.uiResourceId'
        }
    
    def monitor_mes(self):
        last_all_idle = None

        while not self.stop_event.is_set():
            with self.module_states_lock:
                states = list(self.module_states.values())

            if not states:
                time.sleep(0.5)
                continue

            all_idle = all(state is True for state in states)

            # ---------- ENTER DOWNTIME ----------
            if all_idle and not last_all_idle:
                self.system_downtime_start = datetime.now()
                self.mes_logger.log_info("MES","System entered downtime")

            # ---------- EXIT DOWNTIME ----------
            elif not all_idle and last_all_idle:
                if self.system_downtime_start:
                    delta = int((
                        datetime.now() - self.system_downtime_start
                    ).total_seconds() * 1000)
                    self.system_downtime_start = None
                    self.mes_logger.log_info("MES",f"System downtime ended ({delta:.2f}ms)")
                    self.mes_logger.execute("""
                            UPDATE system_info
                            SET total_downtime = total_downtime + %s;
                        """, (delta,))

            last_all_idle = all_idle
            time.sleep(0.5)

    
    def monitor_module(self, module):
        last_state = module.get_value(self.endpoints["xReady"])
        module.logger.log_station_state(last_state)
        with self.module_states_lock:
                self.module_states[module.module_name] = last_state

        enter_timestamp = None
        
        while not self.stop_event.is_set():
            current_state = module.get_value(self.endpoints["xReady"])

            # --- ENTER station (True → False) ---
            if last_state and not current_state:
                module.logger.log_station_state(current_state)
                with self.module_states_lock:
                    self.module_states[module.module_name] = current_state
                enter_timestamp = datetime.now()
                if module.module_name == "End Module":
                    order = module.get_value(self.endpoints["ONo"])
                    part = module.get_value(self.endpoints["PNo"])
                    pos = module.get_value(self.endpoints["OPos"])
                    op = module.get_value(self.endpoints["OpNo"])
                    res = module.get_value(self.endpoints["ResourceId"])

            # --- EXIT station (False → True) ---
            elif not last_state and current_state:
                module.logger.log_station_state(current_state)
                with self.module_states_lock:
                    self.module_states[module.module_name] = current_state
                exit_timestamp = datetime.now()
                time.sleep(0.5)

                # Read order data ONLY when exiting
                if module.module_name != "End Module":
                    order = module.get_value(self.endpoints["ONo"])
                    part = module.get_value(self.endpoints["PNo"])
                    pos = module.get_value(self.endpoints["OPos"])
                    op = module.get_value(self.endpoints["OpNo"])
                    res = module.get_value(self.endpoints["ResourceId"])

                # Calculate cycle time and log information
                cycle_time = int((exit_timestamp - enter_timestamp).total_seconds() * 1000)
                module.logger.log(enter_timestamp, exit_timestamp, cycle_time, order, part, pos, op, res)

                # Reset for the next carrier
                enter_timestamp = None

            last_state = current_state
            time.sleep(0.5)



    def start_monitoring(self):
        """
        Start a thread for each module to monitor state changes, and start MES monitoring.
        """
        for module in self.modules:
            t = threading.Thread(target=self.monitor_module, args=(module,), daemon=True)
            t.start()
            self.threads.append(t)
            module.logger.log_info(f"Monitoring thread started.")

        mes_thread = threading.Thread(
        target=self.monitor_mes,
        daemon=True
        )
        mes_thread.start()
        self.threads.append(mes_thread)
            


    def stop_all(self):
        """
        Signal all threads to stop and disconnect all modules.
        """
        print("\n[System] Stopping all threads and disconnecting modules...")
        self.stop_event.set()
        for module in self.modules:
            module.disconnect()
        print("[System] All modules disconnected.")