# Scrape data on movie certifications from CBFC site

## Installation
Run the following command in your command prompt or terminal (note that a Python
environment must be present)

```bash
$ pip install -r requirements.txt
```

## Usage

#### Step1: 
Download the repo into your system, into a folder of your choice.

#### Step2: 
Open the movies.csv on a text editor/excel as you wish.  It contains a huge list of 5L+ movies.  Each row contains two pieces of information - the movie id and lang_id, as used by the [CBFC site](https://www.cbfcindia.gov.in/main/)

Decide which movies you want to scrape - the **_start_** and **_end_** indices.

#### Step3: 
Run the following command in the terminal / command prompt, (assuming that the
dependencies are installed.), and once you have decided the start and end
indices.  If you supply batch-size, the processing is performed in batches of <batch-size> movies at a time.  Output json will be created correspondingly.

```bash
$ python download.py --range <start-index>-<end-index> --batch-size=<batch-size>
```

For example, if you want to scrape details of movies from movie ID 2 to movie ID
102, and save them in batches of 100 movies at a time, run the following:

```bash
$ python download.py --range 2-102 --batch-size=100
```

**Note1**: By default, the batch-size is 1000.

**Note2**: By default, this process will work in parallel and consume all cores on your computer. If you want to allocate only a specific number of cores to this task, add another argument `--n-jobs` to the command, as follows:
```bash
$ python download.py --range 2-102 --n-jobs 2  # Use only two cores
```
