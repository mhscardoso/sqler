from main import DatabaseHeap

db = DatabaseHeap()

with open('data/Employee.csv', 'r') as data:
	print(data.readline())

	for line in data:
		ls = line.split(',')
		ls[4] = ls[4].rstrip('\n')

		db.insert(int(ls[3]), int(ls[1]), ls[0], ls[2], ls[4])
