import os
import struct
from datetime import datetime
from pprint import pprint


class Database:

   FILENAME         = 'test.bin'
   BLOCK_SIZE       = 4096
   HEADER_STRUCTURE = f'=H H I I I 64s 64s 64s'
   TABLE_STRUCTURE  = f'=H I I 9s 9s 6s'
   TABLE_NAME       = 'employee'
   HEADER_SIZE      = struct.calcsize(HEADER_STRUCTURE)
   RECORD_SIZE      = struct.calcsize(TABLE_STRUCTURE)


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
            self.HEADER_SIZE,                  # 208
            self.RECORD_SIZE,                  # 36
            self.punn(pt_last_register),       # pointer to first block and first record
            self.punn(pt_del_register),        # pointer to 0, 0 (NULL)
            table_name.encode('utf-8'),        # Table Name
            timestamp_created.encode('utf-8'), # Timestamps
            timestamp_updated.encode('utf-8'), # Timestamps
            ''.ljust(self.BLOCK_SIZE - self.HEADER_SIZE, '\x00')[:(self.BLOCK_SIZE - self.HEADER_SIZE)].encode('utf-8')
          # ---------------------------------- #
         )

         f.write(header_block)


   def next_register_pointer(self, block, register):
      if block == 1:
         space = self.BLOCK_SIZE - self.HEADER_SIZE - (self.RECORD_SIZE * register)
      else:
         space = self.BLOCK_SIZE - (self.RECORD_SIZE * register)

      new_block = block if space >= self.RECORD_SIZE else block + 1
      new_register = register + 1 if new_block == block else 1

      return self.pointer(new_block, new_register)


   def write_block(self):
      with open(self.FILENAME, 'ab') as f:
         block = struct.pack(
            f'{self.BLOCK_SIZE}s',
            ''.ljust(self.BLOCK_SIZE, '\x00')[:(self.BLOCK_SIZE)].encode('utf-8')
         )

         f.write(block)


   def write_header(self, last_pointer = None, del_pointer = None):
      header_data = self.read_header()

      put_last_pointer = last_pointer if last_pointer is not None else self.pointer(*header_data[3])
      put_del_pointer  = del_pointer if del_pointer is not None else self.pointer(*header_data[4])

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

      last_pointer = self.last_register_pointer()
      offset = self.calculate_offset(last_pointer)

      put_age       = age if age is not None else 0
      put_year      = year if year is not None else 0
      put_education = education.ljust(9, '\x00')[:9] if education is not None else ''.ljust(9, '\x00')[:9]
      put_city      = city.ljust(9, '\x00')[:9] if city is not None else ''.ljust(9, '\x00')[:9]
      put_gender    = gender.ljust(6, '\x00')[:6] if gender is not None else ''.ljust(6, '\x00')[:6]


      with open(self.FILENAME, 'rb+') as f:
         f.seek(offset)

         register = struct.pack(
            self.TABLE_STRUCTURE,
            0,                   # DELETED MARK
            put_age,
            put_year,
            put_education.encode('utf-8'),
            put_city.encode('utf-8'),
            put_gender.encode('utf-8'),)

         f.write(register)

      next_register = self.next_register_pointer(last_pointer[0], last_pointer[1])
      if next_register[0] > last_pointer[0]:
         self.write_block()

      self.write_header(last_pointer=next_register)


   def read_register(self, pointer):
      offset = self.calculate_offset(pointer)

      with open(self.FILENAME, 'rb') as f:
         f.seek(offset)

         register = f.read(self.RECORD_SIZE)
         data = struct.unpack(self.TABLE_STRUCTURE, register)

         return data


   def compare(self, dt1, dt2):
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
         data[3].decode('utf-8').rstrip('\x00'),
         data[4].decode('utf-8').rstrip('\x00'),
         data[5].decode('utf-8').rstrip('\x00'),
      )


   def read_many_registers(self, age=None, year=None, education=None, city=None, gender=None):
      # comparables = dict()

      # if age != None:
      #    comparables['age'] = age

      # if year != None:
      #    comparables['year'] = year

      # if education != None:
      #    comparables['education'] = education

      # if city != None:
      #    comparables['city'] = city

      # if gender != None:
      #    comparables['gender'] = gender


      pointer = struct.unpack('HH', self.pointer(1, 1))
      first_offset = self.calculate_offset(pointer)
      last_pointer = self.last_register_pointer()

      table = list()

      with open(self.FILENAME, 'rb') as f:
         f.seek(first_offset)

         while pointer != last_pointer:
            register = f.read(self.RECORD_SIZE)
            data = struct.unpack(self.TABLE_STRUCTURE, register)
            if data[0] == 1:
               pointer = struct.unpack('HH', self.next_register_pointer(pointer[0], pointer[1]))
               continue

            readable_data = self.readable_out(data)

            table.append(readable_data)

            pointer = struct.unpack('HH', self.next_register_pointer(pointer[0], pointer[1]))

      return table


   def calculate_offset(self, pointer, null=False):
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
         table_name    = unpacked_data[5].decode('utf-8').rstrip('\x00')
         created_time  = unpacked_data[6].decode('utf-8').rstrip('\x00')
         updated_time  = unpacked_data[7].decode('utf-8').rstrip('\x00')

      return [ block_size, header_size, register_size, 
              pt_last, pt_del, table_name, created_time, updated_time ]


   def pointer(self, block, register):
      return struct.pack('HH', block, register)


   def punn(self, pointer):
      return struct.unpack('I', pointer)[0]


   def deref(self, pointer_number):
      return struct.unpack('HH', struct.pack('I', pointer_number))


   def last_register_pointer(self):
      return self.read_header()[3]


   def del_register_pointer(self):
      return self.read_header()[4]


   def created_timestamp(self):
      return self.read_header()[6]


   def updated_timestamp(self):
      return self.read_header()[7]


   def insert(self, age, year, education, city, gender):
      self.write_register(age, year, education, city, gender)

   def select(self):
      pprint(self.read_many_registers())

   def delete(self):
      pass



db = Database()

# db.insert(10, 2024, 'eng.' ,  'Rio'    , 'Male')
# db.insert(20, 2021, 'lang.',  'Sampa'  , 'Female')
# db.insert(45, 2019, 'bio.' ,  'Vitória', 'Female')
# db.insert(12, 2018, 'med.' ,  'Rio'    , 'Male')
# db.insert(75, 1945, 'fis.' ,  'Sampa'  , 'Female')
# db.insert(25, 1984, 'eng.' ,  'Rio'    , 'Male')

table = db.select()

