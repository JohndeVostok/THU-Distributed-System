import os
import math

if __name__ == "__main__":
	rootDir = "shakespeare"
	dirs = os.listdir(rootDir)
	data = {}
	for filename in dirs:
		data[filename] = ["", [], [0]]
		tmpDir = os.path.join(rootDir, filename)
		with open(tmpDir, "r") as f:
			data[filename][0] = f.read()
		with open(tmpDir, "r") as f:
			data[filename][1] = f.readlines()
		tmp = 0
		for line in data[filename][1]:
			tmp += len(line)
			data[filename][2].append(tmp)

	with open("res.txt", "r") as f:
		lines = f.readlines()

	text = {}
	count = {}

	for line in lines:
		cnt = 0
		tmp = line.split("\t")
		term = tmp[0]
		res = ""
#		res = "Term: " + term + "\n";
		tmp = tmp[1].split(";")
		for tmpfile in tmp:
			tmpp = tmpfile.split(":")
			filename = tmpp[0]
			res += "Filename: " + filename + "\n"
			tmppp = tmpp[1].split(",")
			for offstr in tmppp:
				offset = int(offstr)
				l = 0; r = len(data[filename][2])
				while l + 1 < r:
					mid = math.floor((l + r) / 2)
					if (data[filename][2][mid] <= offset):
						l = mid
					else:
						r = mid
				res += "line " + str(l) + ":" + data[filename][1][l]
				cnt += 1
		text[term] = res
		count[term] = cnt

	print("Initialized.")
	while True:
		term = input("Term: ")
		if not term in text:
			print("Term not existed.")
			continue
		if count[term] > 1000:
			print("Term is noise.")
			continue
		print(text[term])

