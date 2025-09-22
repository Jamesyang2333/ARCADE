# ARCADE: Real-Time Hybrid and Continuous Query Processing across Diverse Data Modalities

[![Paper](https://img.shields.io/badge/paper-Arxiv-red)](link-to-paper)  

ARCADE is a **real-time multimodal data system** that supports **hybrid** and **continuous** query processing across diverse data types, including **vector, spatial, text, and relational data**.  

Built on **RocksDB** (storage) and **MySQL** (query engine), ARCADE introduces:  
- A **unified disk-based secondary index** framework for multimodal data.  
- A **cost-based query optimizer** for expressive hybrid queries.  
- An **incremental materialized view** framework for efficient continuous queries.

ARCADE achieves up to **7.4× faster query performance** compared to leading real-time multimodal data systems on read-heavy workloads.

## ✨ Key Features
- **Multimodal Support**: Text, image, vector, spatial, and relational data.  
- **Unified Secondary Indexing**: Efficient disk-based indexes for vector, spatial, and text data, integrated with LSM-tree storage.  
- **Hybrid Query Optimizer**: Extends MySQL’s cost-based optimizer to support **Hybrid Search Queries** and **Hybrid Nearest Neighbor (NN) Queries** that combine vector, spatial, text, and relational ranking and filters.  
- **Continuous Queries**: Supports both **SYNC** (time-based) and **ASYNC** (event-driven) continuous queries using incremental materialized views.  
- **Semantic Operators**  

For details, please refer to the [paper](./arcade_v2.pdf).
## 📖 Example Queries
**Hybrid Search Query**  
```sql
SELECT t.content
FROM tweets t
WHERE L2_Distance(t.embedding, LLM(@query_text)) < @threshold
  AND t.content LIKE '%keyword%'
  AND ST_Contains(t.coordinate, @region);
```

**Hybrid NN Query**  
```sql
SELECT t.content
FROM tweets t
WHERE t.time BETWEEN @start_time AND @end_time
ORDER BY ST_Distance(t.coordinate, @location)
       + lambda * VECTOR_L2(t.text_embedding, LLM(@query_text))
LIMIT k;
```

**Continuous Query**
```sql
SELECT c.id, c.name, COUNT(*) as count
FROM tweets t
JOIN City c ON ST_Contains(t.coordinate, c.geom)
WHERE L2_DISTANCE(t.embedding, LLM(@query_text)) < @threshold
GROUP BY c.id
ORDER BY count DESC
SYNC 60 seconds;
```

**Semantic query**
```sql 
SELECT id, text
from poi 
where SEMANTIC_FILTER_SINGLE_COL('Is {poi.text} describing a restaurant?', text) = 1;
```


## ⚙️ Build Instructions
### Dependencies
- RocksDB **9.8.0**
- MySQL **8.0.32**
- Boost ()
- FAISS (for vector indexing)

### Compilation
```
cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_SSL=system -DWITH_ZLIB=bundled -DMYSQL_MAINTAINER_MODE=0 -DENABLED_LOCAL_INFILE=1 -DENABLE_DTRACE=0 -DCMAKE_CXX_FLAGS="-march=native" -DFORCE_INSOURCE_BUILD=1 -DDOWNLOAD_BOOST=1 -DWITH_BOOST=../boost/ -DWITH_FB_VECTORDB=1 -DWITH_NEXT_SPATIALDB=1 -DWITH_SEMANTICDB=1
make -j8
make DESTDIR=[build_dir_path] install
```
### Configuration

Example ```my.cnf```
```ini
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

## ▶️ Running ARCADE
Set environment variables:
```bash
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:[root_dir_path]/faiss/faiss
export OPENAI_API_KEY=“”
```

Initialize and start MySQL:
```
bin/mysqld --defaults-file=[build_dir_path]/usr/local/mysql/my.cnf --initialize

bin/mysqld --defaults-file=[build_dir_path]/usr/local/mysql/my.cnf &

bin/mysql -u root -p --port=3306
```

Vector index requires loading pre-trained centroids from local csv. The pre-trained centroid files can be found in [`./vector_index_centroids`](https://github.com/Jamesyang2333/spatial-x-db/edit/vector-data/vector_index_centroids). We default use the `centroids_300.csv` by default. If you are using other centroid configurations, please edit path configuration in `spatial-x-db/rocksdb/table/block_based/block_based_table_factory.h`, function `SetIndexOptions()`

## 📊 Example Usage
**Schema Definition**  
Tables are stored in separate column families using COMMENT 'cfname=...'.
```sql
CREATE TABLE `poi` (
  `coordinate` point NOT NULL SRID 4326,
  `id` int NOT NULL,
  `text` text NOT NULL,
  `text_embedding` json NOT NULL FB_VECTOR_DIMENSION 1536,
  PRIMARY KEY  (id) COMMENT 'cfname=cf1',
  Spatial INDEX key1(coordinate) NEXT_SPATIAL_INDEX_TYPE 'global' COMMENT 'cfname=cf1',
  INDEX key2(text_embedding) FB_VECTOR_INDEX_TYPE 'lsmidx' COMMENT 'cfname=cf1'
) ENGINE=ROCKSDB;
```
**Vector Index Centroids**  
Currently, Vector index requires loading pre-trained centroids.   
Default: [`centroids_300.csv`](https://github.com/Jamesyang2333/spatial-x-db/edit/vector-data/vector_index_centroids)

To change centroids, edit:
```
spatial-x-db/rocksdb/table/block_based/block_based_table_factory.h
→ function `SetIndexOptions()`
```

**Loading Data**  
For test purpose load a small portion because semantic query is expensive!
```sql
SET GLOBAL local_infile = 1;
source load_poi.sql;
shutdown;
```
(Reopen the DB to flush data to disk.)

## 📚 Citation

If you use ARCADE in academic work, please cite:
```
@article{arcade2025,
  title   = {ARCADE: A Real-Time Data System for Hybrid and Continuous Query Processing across Diverse Data Modalities},
  author  = {Yang, Jingyi and Mo, Songsong and Shi, Jiachen and Yu, Zihao and Shi, Kunhao and Ding, Xuchen and Cong, Gao},
  journal = {arXiv preprint arXiv:xxxx.xxxx},
  year    = {2025}
}
```

## 📬 Contact

For questions, please open a GitHub issue or contact:
📧 [jyang028@ntu.edu.sg]
