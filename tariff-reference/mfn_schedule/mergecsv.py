import os, sys
# Merge the text files
#set to the file extension of "to-be-merged" files
ext = '.csv'
#set to your working directory
dir_path = "/Users/matt.admin/projects/tariffs/tariff-reference/mfn_schedule/output/schedule/csv/"
#set to the name of your output file
results = 'final.csv'

i = 0
files = os.listdir(dir_path)
my_list = []
for f in files:
	print (f)
	my_list.append(f)

my_list.sort()
for f in my_list:
	print (f)

out = open(results, 'w')
out.close()

for f in my_list:
	if f.endswith(ext):
		f2 = os.path.join(dir_path, f)
		print (f2)
		data = open(f2)
		out = open(results, 'a')
		contents = data.read()
		out.write (contents)
		
		data.close()
		out.close()
		i += 1
