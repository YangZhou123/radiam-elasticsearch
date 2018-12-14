## Radiam python client for linux




## Getting Started

Check Elasticsearch and tika are running and install Python dependencies using `pip`.
```sh
$ pip install -r requirements.txt
```

### Usage examples

```sh
$ python /path/to/radiam.py -d /rootpath/you/want/to/crawl -i indexname
```

run backend mode, keep file system monitor running
```sh
$ python /path/to/radiam.py -d /rootpath/you/want/to/crawl -i indexname -b
```