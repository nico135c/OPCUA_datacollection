from opcua import Client, ua
import time
import threading
from datetime import datetime
from logger import Logger

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
        self.logger = Logger(self.module_name)

    def connect(self):
        try:
            self.client = Client(self.endpoint)
            self.client.connect()
            self.logger.log(f"Connected to {self.endpoint}")
        except Exception as e:
            self.logger.log(f"Failed to connect: {e}")
            self.client = None

    def disconnect(self):
        if self.client:
            try:
                self.client.disconnect()
                self.logger.log(f"Disconnected from server")
            except Exception as e:
                self.logger.log(f"Error during disconnect: {e}")
            self.client = None

    def get_value(self, node_id: str):
        if not self.client:
            self.logger.log(f"Not connected.")
            return None
        try:
            node = self.client.get_node(node_id)
            data_val = node.get_data_value()
            val = data_val.Value.Value
            return val
        except ua.UaError as e:
            self.logger.log(f"UA Error: {e}")
        except Exception as e:
            self.logger.log(f"Read Error: {e}")
        return None

    def set_value(self, node_id: str, value, value_type=ua.VariantType.Boolean):
        if not self.client:
            self.logger.log(f"Not connected.")
            return
        try:
            node = self.client.get_node(node_id)
            node.set_value(ua.Variant(value, value_type))
            self.logger.log(f"Wrote {value} to {node_id}")
        except ua.UaError as e:
            self.logger.log(f"UA Error: {e}")
        except Exception as e:
            self.logger.log(f"Write Error: {e}")


class OPCUAHandler:
    def __init__(self, module_list):
        self.modules = module_list
        self.threads = []
        self.stop_event = threading.Event()

        for module in self.modules:
            module.connect()

        self.endpoints = {
            "xBG21": 'ns=2;s=|var|CECC-LK.Application.Transport.xBG21',
            "xBG31": 'ns=2;s=|var|CECC-LK.Application.TransportByPass.xBG31',
            "ONo" : 'ns=2;s=|var|CECC-LK.Application.FBs.stpStopper1.stRfidData.stMesData.udiONo'
        }

    def monitor_module(self, module):
        """
        Continuously monitor a module, behavior depends on module type
        """
        last_state = False
        if module.module_name == "Robot Cell Module":
            node_id = self.endpoints["xBG31"]
        else:
            node_id = self.endpoints["xBG21"]
        
        while not self.stop_event.is_set():
            current_state = module.get_value(node_id)
            if current_state != last_state:
                timestamp = datetime.now()
                time.sleep(0.5)
                order_number = module.get_value(self.endpoints["ONo"])
                action = 'enter station' if current_state else 'exit station'
                module.logger.log(f"[{action}][{order_number}]", timestamp)
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
            module.logger.log(f"Monitoring thread started.")

    def stop_all(self):
        """
        Signal all threads to stop and disconnect all modules.
        """
        print("\n[System] Stopping all threads and disconnecting modules...")
        self.stop_event.set()
        for module in self.modules:
            module.disconnect()
        print("[System] All modules disconnected.")