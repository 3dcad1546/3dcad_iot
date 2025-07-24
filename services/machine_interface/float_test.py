import asyncio
import struct
from pymodbus.client.async_tcp import AsyncModbusTcpClient

async def read_float_async(client, base_addr, reverse=False, unit=1):
    # Read two 16-bit registers (32 bits)
    result = await client.read_holding_registers(address=base_addr, count=2, unit=unit)
    if result.isError():
        raise Exception(f"Modbus read error: {result}")

    registers = result.registers
    print(f"Raw registers: {registers}")

    # Reverse if high word is at the higher address
    if reverse:
        registers = [registers[1], registers[0]]

    # Pack and unpack the float (32-bit IEEE)
    raw_bytes = struct.pack('>HH', *registers)
    value = struct.unpack('>f', raw_bytes)[0]
    return value

async def main():
    client = AsyncModbusTcpClient('192.168.0.10', port=502)
    await client.connect()

    try:
        # Example: high word is at address 1004, low word at 1003
        base_address = 1003
        float_value = await read_float_async(client, base_address, reverse=True)
        print(f"Float Value: {float_value}")
    finally:
        await client.close()

# Run the async main function
asyncio.run(main())
