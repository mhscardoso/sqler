import os
import struct
from datetime import datetime
from pprint import pprint


class DatabaseHeap:
   FILENAME         = 'test.bin'
   BLOCK_SIZE       = 4096
   HEADER_STRUCTURE = f'=H H I I I I 64s 64s 64s'
   TABLE_STRUCTURE  = f'=H I I I 9s 9s 6s'
   TABLE_NAME       = 'employee'
   HEADER_SIZE      = struct.calcsize(HEADER_STRUCTURE)
   RECORD_SIZE      = struct.calcsize(TABLE_STRUCTURE)
   DELETED_STRUCT   = f'=H I {RECORD_SIZE - 6}s'

   def __init__(self):
      if not os.path.exists(self.FILENAME):
         self._create_file()


   def _create_file(self):
      """Creates a new file with a header."""
      with open(self.FILENAME, 'wb') as f:
         pt_last_register  = self.pointer(1, 1)
         pt_del_register   = self.pointer(0, 0)
         table_name        = self.TABLE_NAME.ljust(64, '\x00')[:64]
         timestamp_created = str(datetime.now()).ljust(64, '\x00')[:64]
         timestamp_updated = timestamp_created


         header_block = struct.pack(
            self.HEADER_STRUCTURE + f'{self.BLOCK_SIZE - self.HEADER_SIZE}s',
          # ---------------------------------- #
            self.BLOCK_SIZE,                   # 4096
            self.HEADER_SIZE,                  # 212
            self.RECORD_SIZE,                  # 38
            self.punn(pt_last_register),       # pointer to first block and first record
            self.punn(pt_del_register),        # pointer to 0, 0 (NULL)
            0,                                 # SERIAL starts in 1
            table_name.encode('utf-8'),        # Table Name
            timestamp_created.encode('utf-8'), # Timestamps
            timestamp_updated.encode('utf-8'), # Timestamps
            ''.ljust(self.BLOCK_SIZE - self.HEADER_SIZE, '\x00')[:(self.BLOCK_SIZE - self.HEADER_SIZE)].encode('utf-8')
          # ---------------------------------- #
         )

         f.write(header_block)


   def next_register_pointer(self, block: int, register: int):
      if block == 1:
         space = self.BLOCK_SIZE - self.HEADER_SIZE - (self.RECORD_SIZE * register)
      else:
         space = self.BLOCK_SIZE - (self.RECORD_SIZE * register)

      new_block = block if space >= self.RECORD_SIZE else block + 1
      if new_block == block:
         new_register = register + 1
      else:
         new_register = 1

      return self.pointer(new_block, new_register)


   def write_block(self):
      last_pointer = self.last_register_pointer()
      offset = self.BLOCK_SIZE * last_pointer[0]

      with open(self.FILENAME, 'ab+') as f:
         f.seek(offset)
         block = struct.pack(
            f'{self.BLOCK_SIZE}s',
            ''.ljust(self.BLOCK_SIZE, '\x00')[:(self.BLOCK_SIZE)].encode('utf-8')
         )

         f.write(block)


   def write_header(self, last_pointer = None, del_pointer = None, new_serial = None):
      # the pointers are bytes
      header_data = self.read_header()

      put_last_pointer = last_pointer if last_pointer is not None else self.pointer(*header_data[3])
      put_del_pointer  = del_pointer if del_pointer is not None else self.pointer(*header_data[4])
      put_new_serial   = new_serial if new_serial is not None else self.actual_serial()

      table_name        = self.TABLE_NAME.ljust(64, '\x00')[:64]
      timestamp_created = self.created_timestamp().ljust(64, '\x00')[:64]
      timestamp_updated = str(datetime.now()).ljust(64, '\x00')[:64]

      with open(self.FILENAME, 'rb+') as f:
         f.seek(0)

         header = struct.pack(
            self.HEADER_STRUCTURE,
          # ----------------------------------
            self.BLOCK_SIZE,                  
            self.HEADER_SIZE,                 
            self.RECORD_SIZE,                 
            self.punn(put_last_pointer),      
            self.punn(put_del_pointer),       
            put_new_serial,                   
            table_name.encode('utf-8'),       
            timestamp_created.encode('utf-8'),
            timestamp_updated.encode('utf-8'),
          # ----------------------------------
         )

         f.write(header)


   def write_register(self,
                     age=None, 
                     year=None, 
                     education=None, 
                     city=None, 
                     gender=None,):

      del_pointer = self.del_register_pointer() # (bloco, registro)
      last_pointer = self.last_register_pointer()
      new_head_deleted = None

      if del_pointer != (0, 0):
         offset = self.calculate_offset(del_pointer)
      else:
         offset = self.calculate_offset(last_pointer)

      put_age       = age if age is not None else 0
      put_year      = year if year is not None else 0
      put_education = education.ljust(9, '\x00')[:9] if education is not None else ''.ljust(9, '\x00')[:9]
      put_city      = city.ljust(9, '\x00')[:9] if city is not None else ''.ljust(9, '\x00')[:9]
      put_gender    = gender.ljust(6, '\x00')[:6] if gender is not None else ''.ljust(6, '\x00')[:6]
      NEW_SERIAL    = self.actual_serial() + 1

      with open(self.FILENAME, 'rb+') as f:
         f.seek(offset)

         if del_pointer != (0, 0):
            del_register = f.read(self.RECORD_SIZE)
            next_pointer_deleted = struct.unpack(self.DELETED_STRUCT, del_register)[1]
            new_head_deleted = self.deref(next_pointer_deleted)

            f.seek(offset)

         register = struct.pack(
            self.TABLE_STRUCTURE,
            0,                   # DELETED MARK
            NEW_SERIAL,
            put_age,
            put_year,
            put_education.encode('utf-8'),
            put_city.encode('utf-8'),
            put_gender.encode('utf-8'),)

         f.write(register)

      next_register = self.next_register_pointer(last_pointer[0], last_pointer[1])
      if struct.unpack('HH', next_register)[0] > last_pointer[0] and del_pointer == (0, 0):
         print(struct.unpack('HH', next_register))
         self.write_block()

      if del_pointer != (0, 0):
         self.write_header(del_pointer=self.pointer(*new_head_deleted), new_serial=NEW_SERIAL)
      else:
         self.write_header(last_pointer=next_register, new_serial=NEW_SERIAL)



   def read_register(self, pointer):
      offset = self.calculate_offset(pointer)

      with open(self.FILENAME, 'rb') as f:
         f.seek(offset)

         register = f.read(self.RECORD_SIZE)
         data = struct.unpack(self.TABLE_STRUCTURE, register)

         return data


   def compare(self, dt1, dt2) -> bool:
      if type(dt2) == str or type(dt2) == int:
         return dt1 == dt2

      elif type(dt2) == set:
         return dt1 in dt2

      return False


   def readable_out(self, data):
      return (
         data[0],
         data[1],
         data[2],
         data[3],
         data[4].decode('utf-8').rstrip('\x00'),
         data[5].decode('utf-8').rstrip('\x00'),
         data[6].decode('utf-8').rstrip('\x00'),
      )
   
   def read_by_id(self, id):
      pointer = struct.unpack('HH', self.pointer(1, 1))
      first_offset = self.calculate_offset(pointer)
      last_pointer = self.last_register_pointer()

      table = list()

      with open(self.FILENAME, 'rb') as f:
         f.seek(first_offset)

         while pointer != last_pointer:
            f.seek(self.calculate_offset(pointer))
            register = f.read(self.RECORD_SIZE)
            data = struct.unpack(self.TABLE_STRUCTURE, register)

            # Next Pointer
            pointer = struct.unpack('HH', self.next_register_pointer(pointer[0], pointer[1]))
            
            if data[0] == 1 or not self.compare(data[1], id):
               continue

            readable_data = self.readable_out(data)
            table.append(readable_data)

            if type(id) != set:
               break

      return table
   

   def read_by_year(self, year):
      pointer = struct.unpack('HH', self.pointer(1, 1))
      first_offset = self.calculate_offset(pointer)
      last_pointer = self.last_register_pointer()

      table = list()

      with open(self.FILENAME, 'rb') as f:
         f.seek(first_offset)

         while pointer != last_pointer:
            f.seek(self.calculate_offset(pointer))
            register = f.read(self.RECORD_SIZE)
            data = struct.unpack(self.TABLE_STRUCTURE, register)

            # Next Pointer
            pointer = struct.unpack('HH', self.next_register_pointer(pointer[0], pointer[1]))
            
            if data[0] == 1 or not self.compare(data[3], year):
               continue

            readable_data = self.readable_out(data)
            table.append(readable_data)

      return table


   def read_many_registers(self):
      pointer = struct.unpack('HH', self.pointer(1, 1))
      first_offset = self.calculate_offset(pointer)
      last_pointer = self.last_register_pointer()

      table = list()

      with open(self.FILENAME, 'rb') as f:
         f.seek(first_offset)

         while pointer != last_pointer:
            f.seek(self.calculate_offset(pointer))
            register = f.read(self.RECORD_SIZE)
            data = struct.unpack(self.TABLE_STRUCTURE, register)

            # Next Pointer
            pointer = struct.unpack('HH', self.next_register_pointer(pointer[0], pointer[1]))
            
            if data[0] == 1:
               continue

            readable_data = self.readable_out(data)
            table.append(readable_data)

      return table


   def deletion_by_id(self, id):
      pointer = struct.unpack('HH', self.pointer(1, 1))
      last_pointer = self.last_register_pointer()
      del_pointer  = self.pointer(*self.del_register_pointer())
      deleted = 0

      with open(self.FILENAME, 'rb+') as f:
         while pointer != last_pointer:
            offset = self.calculate_offset(pointer)
            f.seek(offset)
            register = f.read(self.RECORD_SIZE)
            data = struct.unpack(self.TABLE_STRUCTURE, register)
            
            if data[0] == 1:
               pointer = struct.unpack('HH', self.next_register_pointer(*pointer))
               continue

            if data[1] == id:
               f.seek(self.calculate_offset(pointer))
               deletion_record = struct.pack(
                  self.DELETED_STRUCT,
                  1, self.punn(del_pointer),
                  ''.ljust(self.RECORD_SIZE - 6, '\x00')[:(self.RECORD_SIZE - 6)].encode('utf-8')
               )

               f.write(deletion_record)
               
               deleted = 1
               break

            pointer = struct.unpack('HH', self.next_register_pointer(*pointer))

      if deleted == 1:
         self.write_header(del_pointer=self.pointer(*pointer))


   def deletion_by_year(self, year):
      header_data = self.read_header()
      pointer = struct.unpack('HH', self.pointer(1, 1))
      last_pointer = self.last_register_pointer()

      deleted_pointers = self.del_register_pointer()

      with open(self.FILENAME, 'rb+') as f:
         while pointer != last_pointer:
            offset = self.calculate_offset(pointer)
            f.seek(offset)
            register = f.read(self.RECORD_SIZE)
            data = struct.unpack(self.TABLE_STRUCTURE, register)
            
            if data[0] == 1:
               pointer = struct.unpack('HH', self.next_register_pointer(*pointer))
               continue

            if data[3] == year:
               f.seek(self.calculate_offset(pointer))

               deletion_record = struct.pack(
                  self.DELETED_STRUCT,
                  1, self.punn(self.pointer(*deleted_pointers)),
                  ''.ljust(self.RECORD_SIZE - 6, '\x00')[:(self.RECORD_SIZE - 6)].encode('utf-8')
               )

               deleted_pointers = pointer

               f.write(deletion_record)

               put_last_pointer  = self.pointer(*header_data[3])
               put_del_pointer   = self.pointer(*pointer)                # Only change
               put_new_serial    = self.actual_serial()
               table_name        = self.TABLE_NAME.ljust(64, '\x00')[:64]
               timestamp_created = header_data[7].ljust(64, '\x00')[:64]
               timestamp_updated = header_data[8].ljust(64, '\x00')[:64]

               f.seek(0)

               header = struct.pack(
                  self.HEADER_STRUCTURE,
                # ----------------------------------
                  self.BLOCK_SIZE,                  
                  self.HEADER_SIZE,                 
                  self.RECORD_SIZE,                 
                  self.punn(put_last_pointer),      
                  self.punn(put_del_pointer),       
                  put_new_serial,                   
                  table_name.encode('utf-8'),       
                  timestamp_created.encode('utf-8'),
                  timestamp_updated.encode('utf-8'),
                # ----------------------------------
               )

               f.write(header)

               next_pointer = struct.unpack('HH', self.next_register_pointer(*pointer))
               off = self.calculate_offset(next_pointer)

               f.seek(off)

            pointer = struct.unpack('HH', self.next_register_pointer(*pointer))



   def calculate_offset(self, pointer, null=False):
      # this pointer is a tuple
      block = pointer[0] - 1
      register = pointer[1] - 1

      if (block < 0 or register < 0) and not null:
         raise KeyError("DATABASE CORRUPTED")
      
      return self.HEADER_SIZE + block * self.BLOCK_SIZE + register * self.RECORD_SIZE


   def read_header(self):
      """Read the header information."""
      with open(self.FILENAME, 'rb') as f:
         f.seek(0)
         header = f.read(self.HEADER_SIZE)
         unpacked_data = struct.unpack(self.HEADER_STRUCTURE, header)

         block_size    = unpacked_data[0]
         header_size   = unpacked_data[1]
         register_size = unpacked_data[2]
         pt_last       = self.deref(unpacked_data[3])
         pt_del        = self.deref(unpacked_data[4])
         SERIAL        = unpacked_data[5]
         table_name    = unpacked_data[6].decode('utf-8').rstrip('\x00')
         created_time  = unpacked_data[7].decode('utf-8').rstrip('\x00')
         updated_time  = unpacked_data[8].decode('utf-8').rstrip('\x00')

      return [ block_size, header_size, register_size, 
              pt_last, pt_del, SERIAL, table_name, created_time, updated_time ]


   def pointer(self, block, register):
      # returns bytes
      return struct.pack('HH', block, register)


   def punn(self, pointer): 
      # this pointer is bytes, returns a int
      return struct.unpack('I', pointer)[0]


   def deref(self, pointer_number):
      # this pointer is a int, returns a tuple
      return struct.unpack('HH', struct.pack('I', pointer_number))


   def last_register_pointer(self):
      return self.read_header()[3]


   def del_register_pointer(self):
      return self.read_header()[4]


   def actual_serial(self):
      return self.read_header()[5]


   def created_timestamp(self):
      return self.read_header()[7]


   def updated_timestamp(self):
      return self.read_header()[8]


   def insert(self, age, year, education, city, gender):
      self.write_register(age, year, education, city, gender)


   def select(self, id=None, year=None):
      if id == None and year == None:
         pprint(self.read_many_registers())
      elif id != None and year == None:
         pprint(self.read_by_id(id))
      elif id == None and year != None:
         pprint(self.read_by_year(year))
      else:
         raise NotImplemented
      

   def delete(self, id=None, year=None):
      if id == None and year == None:
         raise KeyError("DELETION must have a key")
      elif id != None and year == None:
         self.deletion_by_id(id)
      elif id == None and year != None:
         self.deletion_by_year(year)
      else:
         raise NotImplemented


db = DatabaseHeap()

# db.insert(10, 2024, 'eng.' ,  'Rio'    , 'Male')
# db.insert(20, 2021, 'lang.',  'Sampa'  , 'Female')
# db.insert(45, 2019, 'bio.' ,  'Vitória', 'Female')
# db.insert(12, 2018, 'med.' ,  'Rio'    , 'Male')
# db.insert(75, 1945, 'fis.' ,  'Sampa'  , 'Female')
# db.insert(25, 1984, 'eng.' ,  'Rio'    , 'Male')


# table = db.select()

