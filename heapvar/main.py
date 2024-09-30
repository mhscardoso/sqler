import os
import struct
from pprint import pprint
from datetime import datetime

class RecordVar:
    FIXED_RECORD      = f'=H H H H I I I'
    FIXED_RECORD_SIZE = struct.calcsize(FIXED_RECORD)

    def __init__(
        self, 
        id: int, 
        age: int, 
        year: int, 
        education: str, 
        city: str, 
        gender: str,
    ):  
        self.encode1 = education.encode('utf-8')
        self.encode2 = city.encode('utf-8')
        self.encode3 = gender.encode('utf-8')

        self.offset1 = self.FIXED_RECORD_SIZE + len(self.encode1)
        self.offset2 = self.FIXED_RECORD_SIZE + len(self.encode1) + len(self.encode2)
        self.offset3 = self.FIXED_RECORD_SIZE + len(self.encode1) + len(self.encode2) + len(self.encode3)
        
        self.id        = id
        self.age       = age
        self.year      = year
        self.education = education
        self.city      = city
        self.gender    = gender
    

    def mount_struct(self):
        return struct.pack(
            self.FIXED_RECORD + f'{len(self.encode1)}s {len(self.encode2)}s {len(self.encode3)}s',
            # -------------------------------------------------------------------------------- #
            0,
            self.offset1, self.offset2, self.offset3,
            self.id,
            self.age,
            self.year,
            self.encode1,
            self.encode2,
            self.encode3,
        )


class DatabaseVar:
    """
        BLOCK SIZE            H      4096
        Header Size           H      204
        Pt Last Register      I      (BLOCK, START BYTE)
        NEXT SERIAL           I      Starts in 1
        Table Name:           64s    employee
        Timestamp created:    64s
        Timestamp updated:    64s    
    """
    
    FILENAME       = 'heapvar.bin'
    BLOCK_SIZE     = 4096
    HEADER_STRUCT  = f'=H H H I I 64s 64s 64s'
    HEADER_SIZE    = struct.calcsize(HEADER_STRUCT)
    TABLE_NAME     = 'employee'


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
                self.BLOCK_SIZE,                     # 4096
                self.HEADER_SIZE,                    # 204
                0,                                   # Deleted Bytes (starts with 0)
                self.punn(pt_last_register),         # pointer to first block and first record
                0,                                   # SERIAL starts in 0
                table_name.encode('utf-8'),          # Table Name
                timestamp_created.encode('utf-8'),   # Timestamps
                timestamp_updated.encode('utf-8'),   # Timestamps
                ''.ljust(self.BLOCK_SIZE - self.HEADER_SIZE, '\x00')[:(self.BLOCK_SIZE - self.HEADER_SIZE)].encode('utf-8')
                # ---------------------------------- #
            )

            f.write(header_block)
    

    def write_block(self):
        with open(self.FILENAME, 'ab+') as f:
            block = struct.pack(
                f'{self.BLOCK_SIZE}s',
                ''.ljust(self.BLOCK_SIZE, '\x00')[:(self.BLOCK_SIZE)].encode('utf-8')
            )

            f.write(block)
    

    def write_record(
        self, 
        age: int, 
        year: int, 
        education: str, 
        city: str, 
        gender: str,
    ):
        next_id: int            = self.actual_serial() + 1
        last_pointer: tuple     = self.last_register_pointer()
        record: RecordVar       = RecordVar(next_id, age, year, education, city, gender)
        record_size: int        = record.offset3
        record_too_bigger: bool = record_size >= self.distance_end_of_block(last_pointer)
        
        if record_too_bigger:
            self.write_block()
        
        next_last_pointer = self.next_pointer(last_pointer, record_size)
        written_data      = record.mount_struct()
        written_offset    = self.calculate_offset(last_pointer)

        with open(self.FILENAME, 'rb+') as f:
            f.seek(written_offset)
            f.write(written_data)

        self.write_header(last_pointer=next_last_pointer, new_serial=next_id)
    

    def compare(self, dt1, dt2) -> bool:
        if type(dt2) == str or type(dt2) == int:
            return dt1 == dt2

        elif type(dt2) == set:
            return dt1 in dt2

        return False
    

    def delete_by_id(self, id: int) -> int:
        pointer = (1, self.HEADER_SIZE)
        last_pointer  = self.last_register_pointer()
        deleted_struct_size = 0

        accessed_blocks = 1

        with open(self.FILENAME, 'rb+') as f:
            while pointer != last_pointer:
                f.seek(self.calculate_offset(pointer))

                start = f.read(RecordVar.FIXED_RECORD_SIZE)
                unpack_start = struct.unpack(
                    RecordVar.FIXED_RECORD,
                    start,
                )

                offset3 = unpack_start[3]

                if unpack_start[0] == 1 or not unpack_start[4] == id:
                    next_pointer = self.next_pointer(pointer, offset3)
                    if next_pointer[0] > pointer[0]:
                        accessed_blocks += 1
                    pointer = next_pointer

                    continue

                THIS_DELETED_STRUCT = RecordVar.FIXED_RECORD + f'{offset3 - RecordVar.FIXED_RECORD_SIZE}s'

                f.seek(self.calculate_offset(pointer))

                deleted_buffer = struct.pack(
                    THIS_DELETED_STRUCT,
                    1,
                    unpack_start[1],
                    unpack_start[2],
                    unpack_start[3],
                    0,
                    0,
                    0,
                    ''.ljust(offset3 - RecordVar.FIXED_RECORD_SIZE, '\x00')[:(offset3 - RecordVar.FIXED_RECORD_SIZE)].encode('utf-8')
                )

                f.write(deleted_buffer)

                deleted_struct_size = struct.calcsize(THIS_DELETED_STRUCT)
    
                break
        
        if deleted_struct_size != 0:
            compressed, blocks = self.write_header(deleted_bytes=deleted_struct_size)

            if compressed == 1:
                accessed_blocks += self.last_register_pointer()[0] + blocks
    
        return accessed_blocks


    def delete_by_year(self, year: int) -> int:
        pointer = (1, self.HEADER_SIZE)
        last_pointer  = self.last_register_pointer()
        deleted_struct_size = 0
        accessed_blocks = 1

        with open(self.FILENAME, 'rb+') as f:
            while pointer != last_pointer:
                f.seek(self.calculate_offset(pointer))

                start = f.read(RecordVar.FIXED_RECORD_SIZE)
                unpack_start = struct.unpack(
                    RecordVar.FIXED_RECORD,
                    start,
                )

                offset3 = unpack_start[3]

                if unpack_start[0] == 1 or not unpack_start[6] == year:
                    next_pointer = self.next_pointer(pointer, offset3)
                    if next_pointer[0] > pointer[0]:
                        accessed_blocks += 1
                    pointer = next_pointer

                    continue

                THIS_DELETED_STRUCT = RecordVar.FIXED_RECORD + f'{offset3 - RecordVar.FIXED_RECORD_SIZE}s'

                f.seek(self.calculate_offset(pointer))

                deleted_buffer = struct.pack(
                    THIS_DELETED_STRUCT,
                    1,
                    unpack_start[1],
                    unpack_start[2],
                    unpack_start[3],
                    0,
                    0,
                    0,
                    ''.ljust(offset3 - RecordVar.FIXED_RECORD_SIZE, '\x00')[:(offset3 - RecordVar.FIXED_RECORD_SIZE)].encode('utf-8')
                )

                f.write(deleted_buffer)

                deleted_struct_size += struct.calcsize(THIS_DELETED_STRUCT)
    
        
        if deleted_struct_size != 0:
            compressed, blocks = self.write_header(deleted_bytes=deleted_struct_size)

            if compressed == 1:
                accessed_blocks += self.last_register_pointer()[0] + blocks
    
        return accessed_blocks
    

    def read_by_id(self, id: int | set):
        pointer = (1, self.HEADER_SIZE)
        last_pointer  = self.last_register_pointer()
        accessed_blocks = 1

        table = list()

        with open(self.FILENAME, 'rb') as f:
            while pointer != last_pointer:
                f.seek(self.calculate_offset(pointer))

                start = f.read(RecordVar.FIXED_RECORD_SIZE)
                unpack_start = struct.unpack(
                    RecordVar.FIXED_RECORD,
                    start,
                )

                offset1 = unpack_start[1]
                offset2 = unpack_start[2]
                offset3 = unpack_start[3]
                
                next_pointer = self.next_pointer(pointer, offset3)
                if next_pointer[0] > pointer[0]:
                    accessed_blocks += 1
                pointer = next_pointer

                if unpack_start[0] == 1 or not self.compare(unpack_start[4], id):
                    continue

                VAR_RECORD = self.var_struct(offset1, offset2, offset3)
                VAR_SIZE   = struct.calcsize(VAR_RECORD)

                end = f.read(VAR_SIZE)
                unpack_end = struct.unpack(VAR_RECORD, end)

                readable_end = list(map(lambda word: word.decode('utf-8'), unpack_end))

                table.append([*unpack_start[4:], *readable_end])

                if type(id) == int:
                    break
                else:
                    id.remove(unpack_start[4])
                
                if type(id) == set and len(id) == 0:
                    break
        
        return (table, accessed_blocks)


    def read_by_year(self, year: int):
        pointer = (1, self.HEADER_SIZE)
        last_pointer  = self.last_register_pointer()
        accessed_blocks = 1

        table = list()

        with open(self.FILENAME, 'rb') as f:
            while pointer != last_pointer:
                f.seek(self.calculate_offset(pointer))

                start = f.read(RecordVar.FIXED_RECORD_SIZE)
                unpack_start = struct.unpack(
                    RecordVar.FIXED_RECORD,
                    start,
                )

                offset1 = unpack_start[1]
                offset2 = unpack_start[2]
                offset3 = unpack_start[3]
                
                next_pointer = self.next_pointer(pointer, offset3)
                if next_pointer[0] > pointer[0]:
                    accessed_blocks += 1
                pointer = next_pointer

                if unpack_start[0] == 1 or not self.compare(unpack_start[6], year):
                    continue

                VAR_RECORD = self.var_struct(offset1, offset2, offset3)
                VAR_SIZE   = struct.calcsize(VAR_RECORD)

                end = f.read(VAR_SIZE)
                unpack_end = struct.unpack(VAR_RECORD, end)

                readable_end = list(map(lambda word: word.decode('utf-8'), unpack_end))

                table.append([*unpack_start[4:], *readable_end])
        
        return (table, accessed_blocks)
    

    def read_sequence(self):
        pointer = (1, self.HEADER_SIZE)
        last_pointer  = self.last_register_pointer()
        accessed_blocks = 1

        table = list()

        with open(self.FILENAME, 'rb') as f:
            while pointer != last_pointer:
                f.seek(self.calculate_offset(pointer))

                start = f.read(RecordVar.FIXED_RECORD_SIZE)
                unpack_start = struct.unpack(
                    RecordVar.FIXED_RECORD,
                    start,
                )

                offset1 = unpack_start[1]
                offset2 = unpack_start[2]
                offset3 = unpack_start[3]
                
                next_pointer = self.next_pointer(pointer, offset3)
                if next_pointer[0] > pointer[0]:
                    accessed_blocks += 1
                pointer = next_pointer

                if unpack_start[0] == 1:
                    continue

                VAR_RECORD = self.var_struct(offset1, offset2, offset3)
                VAR_SIZE   = struct.calcsize(VAR_RECORD)

                end = f.read(VAR_SIZE)
                unpack_end = struct.unpack(VAR_RECORD, end)

                readable_end = list(map(lambda word: word.decode('utf-8'), unpack_end))

                table.append([*unpack_start[4:], *readable_end])
        
        return (table, accessed_blocks)

    
    def write_header(
        self, 
        last_pointer: tuple | None = None, 
        new_serial: int | None = None,
        deleted_bytes: int | None = None,
    ) -> tuple:
        put_last_pointer  = self.pointer(*last_pointer) if last_pointer is not None else self.pointer(*self.last_register_pointer())
        put_new_serial    = new_serial if new_serial is not None else self.actual_serial()
        table_name        = self.TABLE_NAME.ljust(64, '\x00')[:64]
        timestamp_created = self.created_timestamp().ljust(64, '\x00')[:64]
        timestamp_updated = str(datetime.now()).ljust(64, '\x00')[:64]
        put_deleted_bytes = deleted_bytes + self.deleted_bytes() if deleted_bytes is not None else self.deleted_bytes()

        if put_deleted_bytes >= 2 ** 16: # 65536
            blocks = self.compress()
            return (1, blocks)

        with open(self.FILENAME, 'rb+') as f:
            f.seek(0)

            header = struct.pack(
                self.HEADER_STRUCT,
                # ----------------------------------
                self.BLOCK_SIZE,
                self.HEADER_SIZE,
                put_deleted_bytes,
                self.punn(put_last_pointer),
                put_new_serial,
                table_name.encode('utf-8'),
                timestamp_created.encode('utf-8'),
                timestamp_updated.encode('utf-8'),
                # ----------------------------------
            )

            f.write(header)
        
        return (0, 0)


    def read_header(self):
        """Read the header information."""
        with open(self.FILENAME, 'rb') as f:
            f.seek(0)
            header = f.read(self.HEADER_SIZE)
            unpacked_data = struct.unpack(self.HEADER_STRUCT, header)

            block_size    = unpacked_data[0]
            header_size   = unpacked_data[1]
            deleted_bytes = unpacked_data[2]
            pt_last       = self.deref(unpacked_data[3])
            SERIAL        = unpacked_data[4]
            table_name    = unpacked_data[5].decode('utf-8').rstrip('\x00')
            created_time  = unpacked_data[6].decode('utf-8').rstrip('\x00')
            updated_time  = unpacked_data[7].decode('utf-8').rstrip('\x00')

        return [ block_size, header_size, deleted_bytes, pt_last,
                 SERIAL, table_name, created_time, updated_time ]


    def calculate_offset(self, pointer: tuple) -> int:
        block = pointer[0] - 1
        register_start = pointer[1]

        return block * self.BLOCK_SIZE + register_start


    def next_pointer(self, pointer: tuple, register_size: int) -> tuple:
        if pointer[1] + register_size >= self.BLOCK_SIZE:
            next_block = pointer[0] + 1
        else:
            next_block = pointer[0]
        
        next_register_byte = (pointer[1] + register_size) % self.BLOCK_SIZE

        return (next_block, next_register_byte)


    def compress(self) -> int:
        print("Compressing File...:")

        # Arquivo Antigo
        data, blocks = self.read_sequence()
        temp_filename = 'uncompressed.bin'
        os.rename(self.FILENAME, temp_filename)

        # Arquivo Novo
        self.create_file()
        for row in data:
            self.write_record(*row[1:])
        
        os.remove(temp_filename)

        return blocks


    def var_struct(self, offset1: int, offset2: int, offset3: int):
        struct = f'={offset1 - RecordVar.FIXED_RECORD_SIZE}s {offset2 - offset1}s {offset3 - offset2}s'
        return struct
    

    def distance_end_of_block(self, pointer: tuple) -> int:
        return self.BLOCK_SIZE - pointer[1]
    

    def pointer(self, block: int, start_register_byte: int) -> bytes:
        return struct.pack('HH', block, start_register_byte)
    

    def deref_bytes(self, bytes_pointer: bytes) -> tuple:
        return struct.unpack('HH', bytes_pointer)


    def punn(self, pointer: bytes) -> int: 
        return struct.unpack('I', pointer)[0]


    def deref(self, pointer_number:int) -> tuple:
        return struct.unpack('HH', struct.pack('I', pointer_number))
    

    def deleted_bytes(self) -> int:
        return self.read_header()[2]
    

    def last_register_pointer(self) -> tuple:
        return self.read_header()[3]
    

    def actual_serial(self) -> int:
        return self.read_header()[4]


    def tablename(self) -> str:
        return self.read_header()[5]
    

    def created_timestamp(self) -> str:
        return self.read_header()[6]


    def updated_timestamp(self) -> str:
        return self.read_header()[7]
    

    def insert(self, age: int, year: int, education: str, city: str, gender: str):
        self.write_record(age, year, education, city, gender)


    def delete(self, id=None, year=None):
        if id == None and year == None:
         raise KeyError("DELETION must have a key")
        elif id != None and year == None:
            return self.delete_by_id(id)
        elif id == None and year != None:
            return self.delete_by_year(year)
        else:
            raise NotImplemented


    def select(self, id=None, year=None):
        if id == None and year == None:
            data, blocks = self.read_sequence()
        elif id != None and year == None:
            data, blocks = self.read_by_id(id)
        elif id == None and year != None:
            data, blocks = self.read_by_year(year)
        else:
            raise NotImplemented

        # pprint(data)
        return blocks


db = DatabaseVar()
