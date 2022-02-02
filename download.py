#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import argparse
import json
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
import pandas as pd
import requests
requests.request


URL = "https://www.cbfcindia.gov.in/main/search-result?movie_id={movie_id}&lang_id={lang_id}"

parser = argparse.ArgumentParser()
parser.add_argument("--range")
parser.add_argument("--n-jobs", default=-1)
parser.add_argument("--batch-size", default=1000)
args = parser.parse_args()


def get_movie_details(movie_id, lang_id):
	url = URL.format(movie_id=movie_id, lang_id=lang_id)
	details = {"movie_id": movie_id, "lang_id": lang_id}
	try:
		resp = requests.get(url)
	except requests.exceptions.ConnectionError:
		return details
	try:
		resp.raise_for_status()
	except requests.exceptions.HTTPError as err:  # NOQA: F841
		return {}

	soup = BeautifulSoup(resp.content, features="lxml")
	movie_details = [each.text for each in soup.find_all("td")[1:]]
	details.update({k: v for k, v in zip(movie_details[::2], movie_details[1::2])})
	return details


movies = pd.read_csv("movies.csv", index_col=["movie_id"], squeeze=True).to_dict()
start, end = map(int, args.range.split("-"))
n_jobs = int(args.n_jobs)

ids = [(k, movies[k]) for k in range(start, end + 1) if k in movies]

func = delayed(get_movie_details)

batch_size = int(args.batch_size)
print(f"Saving in batches of {batch_size}...")
break_tag = False
for batch in range(round(len(ids)/batch_size) + 1):	 #let it fail in last iteration
	from_idx = batch_size * batch
	to_idx = batch_size * (batch + 1)
	if start + to_idx > end:
		to_idx = end - start
		break_tag = True
	ids_batch = ids[from_idx:to_idx]
	try:
		if to_idx < from_idx + 1:
			to_idx = from_idx + 1
		result = Parallel(n_jobs=n_jobs, verbose=10)(func(m_id, l_id) for m_id, l_id in ids_batch)
		with open(f'movies-{start + from_idx}-{start + to_idx}.json', 'w') as fout:
			json.dump(result, fout, indent=2)
		print(f"Saved batch-{batch + 1} of all movies to movies-{start + from_idx}-{start + to_idx}.json")
		if break_tag:
			break
	except:
		pass
