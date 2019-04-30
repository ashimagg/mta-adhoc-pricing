import csv

with open('../data/processed_data/1Line.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter="\n")
    for index,row in enumerate(readCSV):
        print(index,row[0])
     