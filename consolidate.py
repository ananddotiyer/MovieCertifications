import json
import os
import pandas as pd
import re

folder = "."
df = list()
for each in os.listdir(folder):
	try:
		with open(each) as fp:
			if re.search(".+-\d+-\d+\.json", "anand-1-2.json") is not None:
				print(each)
				data1 = json.load(fp)
				df.extend(data1)
	except PermissionError:
		pass
with open('consolidated.json', 'w') as output_file:
	json.dump(df, output_file)

df = pd.read_json('cbfc.json')
df.head(10)

print(df.shape[0])

with pd.ExcelWriter('cbfc.xlsx') as writer:
  df.to_excel(writer, sheet_name='cbfc')