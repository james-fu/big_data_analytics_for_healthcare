import csv

f = open('pig/features_normalized/part-r-00000', 'rb')
max = 0
reader = csv.reader(f)
for row in reader:
    if int(row[1]) > max:
        max = int(row[1])

f.close()

print max
