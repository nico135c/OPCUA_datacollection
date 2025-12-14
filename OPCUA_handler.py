from opcua import Client, ua
import time
import threading
from datetime import datetime
from logger import PostgresLogger

class OPCUAFestoModule:
    """
    Represents a Festo OPC-UA module.
    Handles connection, disconnection, and data access via OPC-UA.
    """

    def __init__(self, module_name: str, ip_address: str, database_credentials, port: int = 4840):
        self.module_name = module_name
        self.ip_address = ip_address
        self.port = port
        self.client = None
        self.endpoint = f"opc.tcp://{self.ip_address}:{self.port}"
        self.logger = PostgresLogger(self.module_name, database_credentials)

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
        self.threads = []
        self.stop_event = threading.Event()

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
    
    def monitor_module(self, module):
        last_state = module.get_value(self.endpoints["xReady"])
        module.logger.log_station_state(last_state)

        enter_timestamp = None
        
        while not self.stop_event.is_set():
            current_state = module.get_value(self.endpoints["xReady"])

            # --- ENTER station (True → False) ---
            if last_state and not current_state:
                module.logger.log_station_state(current_state)
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
                exit_timestamp = datetime.now()
                time.sleep(0.5)

                # Read order data ONLY when exiting
                if module.module_name != "End Module":
                    order = module.get_value(self.endpoints["ONo"])
                    part = module.get_value(self.endpoints["PNo"])
                    pos = module.get_value(self.endpoints["OPos"])
                    op = module.get_value(self.endpoints["OpNo"])
                    res = module.get_value(self.endpoints["ResourceId"])

                # Log includes enter + exit times
                
                module.logger.log(enter_timestamp, exit_timestamp, order, part, pos, op, res)

                # Reset for the next carrier
                enter_timestamp = None

            last_state = current_state
            time.sleep(0.5)



    def start_monitoring(self):
        """
        Start a thread for each module to monitor state changes.
        """
        for module in self.modules:
            t = threading.Thread(target=self.monitor_module, args=(module,), daemon=True)
            t.start()
            self.threads.append(t)
            module.logger.log_info(f"Monitoring thread started.")

    def stop_all(self):
        """
        Signal all threads to stop and disconnect all modules.
        """
        print("\n[System] Stopping all threads and disconnecting modules...")
        self.stop_event.set()
        for module in self.modules:
            module.disconnect()
        print("[System] All modules disconnected.")