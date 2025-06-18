import asyncio
from pymodbus.client.tcp import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

PLC_HOST = "192.168.10.3"  # or your PLC IP
PLC_PORT = 502

async def main():
    client = AsyncModbusTcpClient(host=PLC_HOST, port=PLC_PORT)

    # Establish connection
    await client.connect()
    if not client.connected:
        print("❌ Could not connect to PLC.")
        return

    print(f"✅ Connected to PLC at {PLC_HOST}:{PLC_PORT}")

    try:
        # Read holding registers (example: address 100, 2 registers)
        response = await client.read_holding_registers(address=1000, count=2)
        if not response.isError():
            print(f"📦 Read registers: {response.registers}")
        else:
            print("⚠️ Read error:", response)

    except ModbusException as e:
        print("❌ Modbus Exception:", e)

    except Exception as e:
        print("❌ General Exception:", e)

    finally:
        client.close()
        print("🔌 Connection closed.")

# Run the async function
if __name__ == "__main__":
    asyncio.run(main())
