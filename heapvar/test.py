from main import DatabaseVar

import time

db = DatabaseVar()

def load_data():
	total_time = 0
	count = 0

	with open('data/Employee.csv', 'r') as data:
		head = data.readline()

		for line in data:
			ls = line.split(',')
			ls[4] = ls[4].rstrip('\n')
			
			start_time = time.time()
			db.insert(int(ls[3]), int(ls[1]), ls[0], ls[2], ls[4])
			end_time = time.time()

			total_time += end_time - start_time
			count += 1
	
	last_pointer = db.last_register_pointer()

	print(f"Total blocks: {last_pointer[0]}")
	
	return total_time / count


def main():
	if db.last_register_pointer() == (1, DatabaseVar.HEADER_SIZE):
		print('Loading Data:')
		for i in range(10):
			mean = load_data()
			print(f"Mean time for all insertions: {mean}\n")
	
	blocks_list = []
	times = []
	SELECTION_TIMES = 10

	print('Selecting All')
	for i in range(SELECTION_TIMES):
		start_time = time.time()
		blocks = db.select()
		end_time = time.time()

		total = end_time - start_time

		blocks_list.append(blocks)
		times.append(total)
	

	for i in range(SELECTION_TIMES):
		print(f'    {i+1}º Read: {times[i]}s ---- {blocks_list[i]} blocks')
	
	print("-------------------------------")
	print(f'    Mean Time: {sum(times) / len(times)}')
	print(f'    Mean Accessed Blocks: {sum(blocks_list) / len(blocks_list)}')

	print('\n\n')
	

	ids = [44, 64, 94, 491, 930, 8519, 20192, 58392, 83921, 98493, 120183, 284932, 328492, 389432, 400000]
	blocks_list = []
	times = []

	print('Selecting by id')
	for id in ids:
		start_time = time.time()
		blocks = db.select(id=id)
		end_time = time.time()

		total = end_time - start_time

		blocks_list.append(blocks)
		times.append(total)
	

	for i in range(len(ids)):
		print(f'    {i+1}º Read: {times[i]}s ---- {blocks_list[i]} blocks')
	
	print("-------------------------------")
	print(f'    Mean Time: {sum(times) / len(times)}')
	print(f'    Mean Accessed Blocks: {sum(blocks_list) / len(blocks_list)}')

	print('\n\n')


	print('Selecting by ids')
	ids_set = set(ids)

	start_time = time.time()
	blocks = db.select(id=ids_set)
	end_time = time.time()

	total = end_time - start_time
	
	print("-------------------------------")
	print(f'    Total Time: {total}')
	print(f'    Accessed Blocks: {blocks}')

	print('\n\n')


	ids = [44, 64, 94, 491, 930, 8519, 20192]
	blocks_list = []
	times = []

	print("Deleting series by id:")
	for i, id in enumerate(ids):
		start_time = time.time()
		block = db.delete(id=id)
		end_time = time.time()

		total_time = end_time - start_time

		blocks_list.append(block)
		times.append(total_time)

		print(f"    Record {i}: {total_time}s --- {block} blocks accessed.")
	
	print("-------------------------------")
	print(f"\n    Deletions by id mean time: {sum(times) / len(times)}")
	print(f"    Mean accessed blocks: {sum(blocks_list) / len(blocks_list)}")

	print('\n\n')


	blocks_list = []
	times = []

	print('Deleting by year - Many registers erased')
	year = 2017

	start_time = time.time()
	blocks = db.delete(year=year)
	end_time = time.time()

	total_time = end_time - start_time

	print("-------------------------------")
	print(f'    Total Time: {total_time}')
	print(f'    Accessed Blocks: {blocks}')

	print('\n\n')

	print('Inserting Again')

	for i in range(4):
		mean = load_data()
		print(f"    Mean time: {mean}")


main()