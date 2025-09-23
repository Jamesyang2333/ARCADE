# ARCADE

[![Paper](https://img.shields.io/badge/paper-Arxiv-red)](link-to-paper)  

ARCADE is a **real-time multimodal data system** that supports **hybrid**, **continuous** and **semantic** query processing across diverse data types, including **text, image, vector, spatial, and relational data**.  

### ✨ Key Features
- **Multimodal Support**: Text, image, vector, spatial, and relational data.  
- **Unified Secondary Indexing**: Efficient disk-based indexes for vector, spatial, and text data, integrated with LSM-tree storage.  
- **Hybrid Query Optimizer**: Extends MySQL’s cost-based optimizer to support hybrid queries with joint ranking and filters over vector, spatial, text, and relational attributes.  
- **Continuous Queries**: **SYNC** (time-based) and **ASYNC** (event-driven) continuous queries using incremental materialized views.  
- **Semantic Operators**: Allows users to embed natural language intent directly into SQL, enabling filtering, transformation, extraction, joining, and ranking with AI-powered semantic analytics.

For details, please refer to the [paper](./arcade_v2.pdf).

## 🎉 News
- [2025-09] 🔥 ARCADE now supports a set of [semantic operators](#-semantic-operators) for AI-powered query processing.

## ⚙️ Build Instructions
### Dependencies
- RocksDB **9.8.0**
- MySQL **8.0.32**
- Boost (for MySQL build)
- FAISS (for vector features)

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
# API key required for semantic operators
export OPENAI_API_KEY=“”    
```

Initialize and start MySQL:
```
bin/mysqld --defaults-file=[build_dir_path]/usr/local/mysql/my.cnf --initialize

bin/mysqld --defaults-file=[build_dir_path]/usr/local/mysql/my.cnf &

bin/mysql -u root -p --port=3306
```

## 📊 Example Usage
**Schema Definition**  
Tables are stored in separate column families using `COMMENT 'cfname=...'`.
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

## 🧠 Semantic Operators

ARCADE extends SQL with **five semantic operators** to enable natural language reasoning over your data. These operators leverage vector embeddings and LLM-backed processing to filter, extract, and rank results beyond exact keyword matching or simple similarity search.

### Available Operators

1. **`SEMANTIC_FILTER`** – Filter rows based on a natural language condition.  
2. **`SEMANTIC_MAP`** – Apply transformation to each row.  
3. **`SEMANTIC_EXTRACT`** – Extract structured information from unstructured text.  
4. **`SEMANTIC_JOIN`** – Join two tables based on semantic predicate over text columns.  
5. **`SEMANTIC_RANK`** – Rank rows by semantic relevance to a query prompt.

### Usage Examples

#### 1. Semantic Filter
Filter rows using natural language conditions:
```sql
-- Identify patient reports that mention diabetes
SELECT report_id, text
FROM patient_reports
WHERE SEMANTIC_FILTER_SINGLE_COL('Does {patient_reports.text} indicate the patient has diabetes?', text) = 1;
```

#### 2. Semantic Map
Transform unstructured text into new values:
```sql
SELECT candidate_id,
       SEMANTIC_MAP('Summarize candidate's key qualifications for the research scientist role from {resumes.content}', content) AS summary
FROM resumes;
```

#### 3. Semantic Extract
Extract structured values from text fields:
```sql
-- Extract datasets mentioned in machine learning papers
SELECT paper_id,
       SEMANTIC_EXTRACT('List the datasets used in {papers.abstract}', abstract) AS datasets
FROM papers;
```

#### 4. Semantic Rank
Rank rows by semantic similarity to a natural language query:
```sql
-- Rank climate news articles by relevance to policy impacts
SELECT id, title,
       SEMANTIC_RANK(content_embedding, 'Retrive news articles with climate change policy impact') AS score
FROM news_articles
ORDER BY score DESC
LIMIT 10;
```
`SEMANTIC_RANK` use the openai `text-embedding-3-small` model by default. 
Make sure the embedding column values is also generated with the same model.

#### 5. Semantic Join
```sql
-- Match user projects with relevant research papers
SELECT projects.id, papers.id
FROM projects
JOIN papers
  ON SEMANTIC_JOIN('Is {projects.description} relevant to {papers.abstract}?', projects.description, papers.abstract) = 1;
```

## 📖 Hybrid & Continuous Queries
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
