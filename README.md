## setup instruction
myrocks compilation and installation
```
cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_SSL=system -DWITH_ZLIB=bundled -DMYSQL_MAINTAINER_MODE=0 -DENABLED_LOCAL_INFILE=1 -DENABLE_DTRACE=0 -DCMAKE_CXX_FLAGS="-march=native" -DFORCE_INSOURCE_BUILD=1 -DDOWNLOAD_BOOST=1 -DWITH_BOOST=../boost/ -DWITH_FB_VECTORDB=1 -DWITH_NEXT_SPATIALDB=1 -DWITH_SEMANTICDB=1
make -j8
make DESTDIR=[build_dir_path] install
```

my.cnf
```
[mysqld]
rocksdb
default-storage-engine=rocksdb
skip-innodb
default-tmp-storage-engine=MyISAM

log-bin
binlog-format=ROW

basedir=[build_dir_path]/usr/local/mysql
datadir=[build_dir_path]/usr/local/mysql/data
port=3306
```

Vector index requires loading pre-trained centroids from local csv. The pre-trained centroid files can be found in [`./vector_index_centroids`](https://github.com/Jamesyang2333/spatial-x-db/edit/vector-data/vector_index_centroids). We default use the `centroids_300.csv` by default. If you are using other centroid configurations, please edit path configuration in `spatial-x-db/rocksdb/table/block_based/block_based_table_factory.h`, function `SetIndexOptions()`

Start the DB
```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:[root_dir_path]/faiss/faiss

export OPENAI_API_KEY=“”

rm -r data

rm -r global_index/*

bin/mysqld --defaults-file=[build_dir_path]/usr/local/mysql/my.cnf --initialize

bin/mysqld --defaults-file=[build_dir_path]/usr/local/mysql/my.cnf &

bin/mysql -u root -p --port=3306
```

When creating table schema, always put the spatial attribute as the first field (except for primary key), because currently the next spatial index parse the first field value at default. If multiple tables are created using COMMENT after all indexes to store each table in a separate column family. Example:
```sql
CREATE TABLE `poi` (
  `coordinate` point NOT NULL SRID 4326,
  `id` int NOT NULL,
  `review_id` varchar(30) NOT NULL,
  `text` text NOT NULL,
  `business_id` varchar(30) NOT NULL,
  `stars` float NOT NULL,
  `date` timestamp NOT NULL,
  `user_id` varchar(30) NOT NULL,
  `city_id` int NOT NULL,
  `text_embedding` json NOT NULL FB_VECTOR_DIMENSION 128,
  PRIMARY KEY  (id) COMMENT 'cfname=cf1',
  Spatial INDEX key1(coordinate) NEXT_SPATIAL_INDEX_TYPE 'global' COMMENT 'cfname=cf1',
  INDEX key2(text_embedding) FB_VECTOR_INDEX_TYPE 'lsmidx' COMMENT 'cfname=cf1'
) ENGINE=ROCKSDB;

CREATE TABLE IF NOT EXISTS city (
    id INT,
    city_name VARCHAR(50) NOT NULL,
    geometry GEOMETRY NOT NULL SRID 4326,
    PRIMARY KEY  (id) COMMENT 'cfname=cf2'
) ENGINE=ROCKSDB;

CREATE TABLE `tweets` (
  `coordinate` point NOT NULL SRID 4326,
  `tweet_id` bigint NOT NULL,
  `id` int NOT NULL,
  `time` timestamp NOT NULL,
  `content` text NOT NULL,
  `city_id` int NOT NULL,
  `country` varchar(50) NOT NULL,
  `user_id` bigint NOT NULL,
  `language` varchar(20) NOT NULL,
  `text_embedding` json NOT NULL FB_VECTOR_DIMENSION 128,
  `user_name` varchar(50) NOT NULL,
  `description` text,
  PRIMARY KEY  (id) COMMENT 'cfname=cf3',
  Spatial INDEX key1(coordinate) NEXT_SPATIAL_INDEX_TYPE 'global' COMMENT 'cfname=cf3',
  INDEX key2(text_embedding) FB_VECTOR_INDEX_TYPE 'lsmidx' COMMENT 'cfname=cf3'
) ENGINE=ROCKSDB;
```
After data loading, shutdown the db instance and reopen to flush the data to disk. (for test purpose load a small portion because semantic query is expensive)
```sql
SET GLOBAL local_infile = 1;

After data loading, shutdown the db instance and reopen to flush the data to disk. Note that currently "rocksdb_bulk_load = 1" is not supported for loading table into a separate column family
```sql

source /home/jingyi/Desktop/Dataset/arcade/sql/load_poi.sql

shutdown;
```

Semantic query.
```sql 
SELECT id, text
from poi 
where SEMANTIC_FILTER_SINGLE_COL('Is {poi.text} describing a restaurant?', text) = 1;
```

Vector index only work for ANN query or ANN query with distance filter. 
```sql
SELECT poi.text, FB_VECTOR_L2(poi.text_embedding, '[0.44306,0.02230,0.11578,0.41873,0.74409,0.34334,0.33453,0.40716,0.37610,0.80149,0.60118,0.11211,0.47747,0.93887,0.87177,0.15745,0.98913,0.45836,0.69989,0.85991,0.43448,0.13727,0.05792,0.81027,0.21215,0.20782,0.51014,0.45798,0.59915,0.46833,0.91969,0.39313,0.18096,0.67455,0.17223,0.15206,0.79362,0.75081,0.85493,0.55733,0.76144,0.54786,0.53391,0.01942,0.12268,0.34548,0.93812,0.64510,0.01773,0.84336,0.69197,0.54395,0.10309,0.73041,0.08343,0.27263,0.20064,0.14560,0.99416,0.83500,0.73381,0.96731,0.67922,0.61853,0.81316,0.20826,0.74992,0.22053,0.02797,0.34607,0.94980,0.30097,0.40306,0.43160,0.79259,0.92654,0.57629,0.54037,0.81462,0.16278,0.25577,0.62755,0.39245,0.35590,0.04336,0.36093,0.87688,0.45138,0.11825,0.99853,0.52337,0.32730,0.31417,0.66618,0.99730,0.72810,0.20480,0.03688,0.19259,0.62002,0.65370,0.88382,0.91929,0.59139,0.47161,0.62921,0.23437,0.91053,0.21468,0.36189,0.42834,0.33270,0.72881,0.97116,0.68935,0.95970,0.60625,0.85554,0.33919,0.84489,0.89781,0.99438,0.61604,0.80038,0.90440,0.08618,0.91900,0.55515]') AS dis FROM poi ORDER BY dis asc LIMIT 10;

SELECT poi.text, FB_VECTOR_L2(poi.text_embedding, '[0.44306,0.02230,0.11578,0.41873,0.74409,0.34334,0.33453,0.40716,0.37610,0.80149,0.60118,0.11211,0.47747,0.93887,0.87177,0.15745,0.98913,0.45836,0.69989,0.85991,0.43448,0.13727,0.05792,0.81027,0.21215,0.20782,0.51014,0.45798,0.59915,0.46833,0.91969,0.39313,0.18096,0.67455,0.17223,0.15206,0.79362,0.75081,0.85493,0.55733,0.76144,0.54786,0.53391,0.01942,0.12268,0.34548,0.93812,0.64510,0.01773,0.84336,0.69197,0.54395,0.10309,0.73041,0.08343,0.27263,0.20064,0.14560,0.99416,0.83500,0.73381,0.96731,0.67922,0.61853,0.81316,0.20826,0.74992,0.22053,0.02797,0.34607,0.94980,0.30097,0.40306,0.43160,0.79259,0.92654,0.57629,0.54037,0.81462,0.16278,0.25577,0.62755,0.39245,0.35590,0.04336,0.36093,0.87688,0.45138,0.11825,0.99853,0.52337,0.32730,0.31417,0.66618,0.99730,0.72810,0.20480,0.03688,0.19259,0.62002,0.65370,0.88382,0.91929,0.59139,0.47161,0.62921,0.23437,0.91053,0.21468,0.36189,0.42834,0.33270,0.72881,0.97116,0.68935,0.95970,0.60625,0.85554,0.33919,0.84489,0.89781,0.99438,0.61604,0.80038,0.90440,0.08618,0.91900,0.55515]') AS dis FROM poi HAVING dis<109.1342890835001 ORDER BY dis asc LIMIT 10;
```
