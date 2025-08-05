location of new dataset, use:

```
wget https://web.corral.tacc.utexas.edu/arctos/data/filtered_flat.csv.gz
```


Index Fields
```
field,indexed,type
cataloged_item_type,yes,keywords
cat_num,yes,text
institution_acronym,yes,keywords
collection_cde,yes,keywords
collectors,yes,comma separated list put into key words
continent_ocean,yes,keywords
country,yes,keywords
state_prov,yes,keywords
county,yes,keywords
dec_lat,yes,decimal
dec_long,yes,decimal
datum,yes,text
coordinateuncertaintyinmeters,yes,decimal
scientific_name,yes,text
identifiedby,yes,text
kingdom,yes,text
phylum,yes,text
family,yes,text
genus,yes,text
species,yes,text
subspecies,yes,text
relatedinformation,yes,url link
year,yes,integer
month,yes,integer
day,yes,integer
taxon_rank,yes,keywords
```

Process for installing loader and getting it running use pyenv 3.10.0
1. install openssl libs
```
sudo apt update
sudo apt install libssl-dev libssl3
```

2. rebuild python 3.10.0
```
pyenv uninstall 3.10.0
pyenv install 3.10.0
```

pyenv local 3.10.0


