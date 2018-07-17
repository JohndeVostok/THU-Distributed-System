if __name__ == "__main__":
	with open("res/part-r-00000", "r") as f:
		movies = f.readlines();
	with open("netflix/movie_titles.txt") as f:
		packs = f.readlines();
	titles = []
	for pack in packs:
		titles.append(pack.split(",")[2])
	tmp = {}
	for movie in movies:
		movieid = movie.split()[0];
		clusterid = movie.split()[1];
		if not clusterid in tmp:
			tmp[clusterid] = []
		tmp[clusterid].append(titles[int(movieid) - 1])
	for clusterid in tmp:
		cmovies = tmp[clusterid]
		print(clusterid);
		for m in cmovies:
			print(clusterid + " " + m);

