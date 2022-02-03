import argparse
import json
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
import pandas as pd
import requests
from tqdm import tqdm


URL = "https://www.cbfcindia.gov.in/main/search-result?movie_id={movie_id}&lang_id={lang_id}"

parser = argparse.ArgumentParser()
parser.add_argument("--range")
parser.add_argument("--n-jobs", default=-1)
parser.add_argument("--batch-size", default=1000)
parser.add_argument("--use-tor", dest="tor", action="store_true")
parser.set_defaults(tor=False)
args = parser.parse_args()

proxies = (
    {"http": "socks5://127.0.0.1:9050", "https": "socks5://127.0.0.1:9050"}
    if args.tor
    else {}
)


def get_movie_details(movie_id, lang_id):
    url = URL.format(movie_id=movie_id, lang_id=lang_id)
    details = {"movie_id": movie_id, "lang_id": lang_id}
    try:
        resp = requests.get(url, proxies=proxies)
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
batches = [
    (i * batch_size, (i + 1) * batch_size) for i in range(len(ids) // batch_size)
]


for start, end in tqdm(batches):
    result = Parallel(n_jobs=n_jobs, verbose=10)(
        func(m_id, l_id) for m_id, l_id in ids[start:end]
    )
    with open(f"movies-{start}-{end}.json", "w") as fout:
        json.dump(result, fout, indent=2)

# Last leftover batch
result = Parallel(n_jobs=n_jobs, verbose=10)(
    func(m_id, l_id) for m_id, l_id in ids[-(len(ids) % batch_size):]
)
with open("movies-last-batch-jd.json", "w") as fout:
    json.dump(result, fout, indent=2)
