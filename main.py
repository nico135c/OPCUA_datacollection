from OPCUA_handler import OPCUAFestoModule, OPCUAHandler

modules = [
    OPCUAFestoModule("Bottom Cover Module", '172.20.3.1'),
    OPCUAFestoModule("Error Module", '172.20.13.1'),
    OPCUAFestoModule("Camera Module", '172.20.5.1'),
    OPCUAFestoModule("Robot Cell Module", '172.20.4.1'),
    OPCUAFestoModule("Top Cover Module", '172.20.11.1'),
    OPCUAFestoModule("Drill Station Module", '172.20.16.1'),
    OPCUAFestoModule("End Module", '172.20.1.1')
]

if __name__ == "__main__":
    handler = OPCUAHandler(modules)
    handler.start_monitoring()

    print("\nPress Ctrl+C or 'q' + Enter to exit.\n")

    try:
        while True:
            user_input = input()
            if user_input.strip().lower() == "q":
                break
    except KeyboardInterrupt:
        pass
    finally:
        handler.stop_all()
        print("[System] Shutdown complete.")
