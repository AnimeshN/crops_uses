import os
import csv
with open('name_dictionary.csv', 'r') as f:
  reader = csv.reader(f)
  list = list(reader)

for i in range(2,56):
    if list[i][3]:
        file = list[i][3]
        print("********"+file)
        file = file.replace(" ", "\ ")
        tca = list[i][2]
        os.system("python3 run.py "+file+" "+tca)






# file = "Basic Info _ CP Data Eklahare"
# file = file.replace(" ", "\ ")
# os.system("python3 run.py "+file+" 100")
