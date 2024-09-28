import os
import struct
from datetime import datetime

class RegisterVar:
    FIXED_RECORD      = f'=H H H H I I I'
    FIXED_RECORD_SIZE = struct.calcsize(FIXED_RECORD)

    def __init__(self, 
        id: int, 
        age: int, 
        year: int, 
        education: str, 
        city: str, 
        gender: str,
    ):
        self.offset1   = self.FIXED_RECORD_SIZE + len(education)
        self.offset2   = self.FIXED_RECORD_SIZE + len(education) + len(city)
        self.offset3   = self.FIXED_RECORD_SIZE + len(education) + len(city) + len(gender)
        self.id        = id
        self.age       = age
        self.year      = year
        self.education = education
        self.city      = city
        self.gender    = gender
    

    def mount_struct(self):
        return struct.pack(
            self.FIXED_RECORD + f'{len(self.education)}s {len(self.city)}s {len(self.gender)}s',
            # -------------------------------------------------------------------------------- #
            0,
            self.offset1, self.offset2, self.offset3,
            self.id,
            self.age,
            self.year,
            self.education.encode('utf-8'),
            self.city.encode('utf-8'),
            self.gender.encode('utf-8'),
        )


class DatabaseVar:
    FILENAME      = 'heapvar.bin'
    BLOCK_SIZE    = 4096
    HEADER_STRUCT = f'=H H I I 64s 64s 64s'
    HEADER_SIZE   = struct.calcsize(HEADER_STRUCT)
    TABLE_NAME    = 'employee'

    def __init__(self):
        if not os.path.exists(self.FILENAME):
            self.create_file()
    

    def create_file(self):
        """Creates a new file with a header."""
        with open(self.FILENAME, 'wb') as f:
            pt_last_register  = self.pointer(1, self.HEADER_SIZE)
            table_name        = self.TABLE_NAME.ljust(64, '\x00')[:64]
            timestamp_created = str(datetime.now()).ljust(64, '\x00')[:64]
            timestamp_updated = timestamp_created


            header_block = struct.pack(
                self.HEADER_STRUCT + f'{self.BLOCK_SIZE - self.HEADER_SIZE}s',
                # ---------------------------------- #
                    self.BLOCK_SIZE,                   # 4096
                    self.HEADER_SIZE,                  # 204
                    self.punn(pt_last_register),       # pointer to first block and first record
                    0,                                 # SERIAL starts in 1
                    table_name.encode('utf-8'),        # Table Name
                    timestamp_created.encode('utf-8'), # Timestamps
                    timestamp_updated.encode('utf-8'), # Timestamps
                    ''.ljust(self.BLOCK_SIZE - self.HEADER_SIZE, '\x00')[:(self.BLOCK_SIZE - self.HEADER_SIZE)].encode('utf-8')
                # ---------------------------------- #
            )

            f.write(header_block)
    
    def write_header(
        self, 
        last_pointer: tuple | None = None, 
        new_serial: int | None = None
    ):
        put_last_pointer  = self.pointer(*last_pointer) if last_pointer is not None else self.pointer(self.last_register_pointer())
        put_new_serial    = new_serial if new_serial is not None else self.actual_serial()
        table_name        = self.TABLE_NAME.ljust(64, '\x00')[:64]
        timestamp_created = self.created_timestamp().ljust(64, '\x00')[:64]
        timestamp_updated = str(datetime.now()).ljust(64, '\x00')[:64]

        with open(self.FILENAME, 'rb+') as f:
            f.seek(0)

            header = struct.pack(
                self.HEADER_STRUCT,
            # ----------------------------------
                self.BLOCK_SIZE,                  
                self.HEADER_SIZE,                                 
                self.punn(put_last_pointer),          
                put_new_serial,                   
                table_name.encode('utf-8'),       
                timestamp_created.encode('utf-8'),
                timestamp_updated.encode('utf-8'),
            # ----------------------------------
            )

            f.write(header)


    def read_header(self):
        """Read the header information."""
        with open(self.FILENAME, 'rb') as f:
            f.seek(0)
            header = f.read(self.HEADER_SIZE)
            unpacked_data = struct.unpack(self.HEADER_STRUCT, header)

            block_size    = unpacked_data[0]
            header_size   = unpacked_data[1]
            pt_last       = self.deref(unpacked_data[2])
            SERIAL        = unpacked_data[3]
            table_name    = unpacked_data[4].decode('utf-8').rstrip('\x00')
            created_time  = unpacked_data[5].decode('utf-8').rstrip('\x00')
            updated_time  = unpacked_data[6].decode('utf-8').rstrip('\x00')

        return [ block_size, header_size, pt_last, 
                 SERIAL, table_name, created_time, updated_time ]
    

    def pointer(self, block: int, start_register_byte: int) -> bytes:
      return struct.pack('HH', block, start_register_byte)
    

    def deref_bytes(self, bytes_pointer: bytes) -> tuple:
        return struct.unpack('HH', bytes_pointer)


    def punn(self, pointer: bytes) -> int: 
        return struct.unpack('I', pointer)[0]


    def deref(self, pointer_number:int) -> tuple:
        return struct.unpack('HH', struct.pack('I', pointer_number))
    

    def last_register_pointer(self):
        return self.read_header()[2]
    

    def actual_serial(self):
        return self.read_header()[3]
    

    def created_timestamp(self):
        return self.read_header()[5]


    def updated_timestamp(self):
        return self.read_header()[6]
    

    def insert(self):
        pass

    def delete(self):
        pass

    def select(self):
        pass
