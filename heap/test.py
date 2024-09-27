from main import Database

db = Database()

with open('heap/data/Employee.csv', 'r') as data:
	print(data.readline())

	counter = 1

	for line in data:
		ls = line.split(',')
		ls[4] = ls[4].rstrip('\n')
		
		if counter == 198:
			break

		counter += 1

		db.insert(int(ls[3]), int(ls[1]), ls[0], ls[2], ls[4])
