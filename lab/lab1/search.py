import os

if __name__ == "__main__":
#	with open("res.txt", "r") as f:
		text = f.read()
		lines = f.readlines();
	offset = []
	tmp = 0
	for line in lines:
		offset.append(tmp)
		tmp += len(line)
	
