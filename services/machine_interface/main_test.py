import asyncio
from pymodbus.client.tcp import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

PLC_HOST = "192.168.10.3"  # or your PLC IP
PLC_PORT = 502

async def main():
    client = AsyncModbusTcpClient(host=PLC_HOST, port=PLC_PORT)
    print(client,"clientttttt")

    # Establish connection
    await client.connect()
    if not client.connected:
        print("❌ Could not connect to PLC.")
        return

    print(f"✅ Connected to PLC at {PLC_HOST}:{PLC_PORT}")

    try:
        # Read holding registers (example: address 100, 2 registers)
        response = await client.read_holding_registers(address=2000, count=12)
        registers = response.registers  # e.g., [12336, 16706, ...]

    # Step 2: Convert to bytes (big-endian assumed; adjust if needed)
        byte_data = b''.join(reg.to_bytes(2, byteorder='big') for reg in registers)
        print("Barcode:", byte_data)
    # Step 3: Decode bytes to string (assuming ASCII or UTF-8)
        barcode = byte_data.decode('ascii').rstrip('\x00')  # remove null chars if padded


        if not response.isError():
            registers = response.registers
            print("📦 Read registers:", registers)

            # Convert registers (16-bit) to bytes
            byte_data = b''.join(reg.to_bytes(2, byteorder='big') for reg in registers)

            # Decode to string, strip null bytes and control characters
            barcode = byte_data.decode('ascii', errors='ignore').strip().strip('\x00\r\n')

            print("✅ Barcode:", barcode)
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
