/*
   Copyright (c) 2023, Facebook, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */
#pragma once
#include <cstddef>
#ifdef WITH_FB_VECTORDB
#include <faiss/Index.h>
#endif
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>
#include <memory>
#include "./rdb_cmd_srv_helper.h"
#include "./rdb_global.h"
#include "rdb_utils.h"
#include "sql/item_fb_vector_func.h"
#include "sql/item_json_func.h"
#include "sql/sql_class.h"

namespace myrocks {
using faiss_ivf_list_id = int64_t;

class Rdb_key_def;
class Rdb_vector_db_iterator {
 public:
  virtual ~Rdb_vector_db_iterator() = default;
  virtual bool is_available() = 0;
  virtual void next() = 0;
  virtual uint get_key(std::string &key) = 0;
};

/** for infomation schema */
class Rdb_vector_index_info {
 public:
  /**
    total number of vectors, this is populated when
    scanning the index, not garanteed to be accurate.
   */
  int64_t m_ntotal{0};
  /**
   number of time the index is used for knn search
  */
  uint m_hit{0};

  std::size_t m_code_size{0};
  std::size_t m_nlist{0};
  uint m_pq_m{0};
  uint m_pq_nbits{0};

  /**
    stats for ivf lists.
    populated when scanning the index.
  */
  uint m_min_list_size{0};
  uint m_max_list_size{0};
  uint m_avg_list_size{0};
  uint m_median_list_size{0};
};

class Rdb_vector_search_params {
 public:
  FB_VECTOR_INDEX_METRIC m_metric = FB_VECTOR_INDEX_METRIC::NONE;
  uint m_k = 0;
  uint m_nprobe = 0;
  float m_weight = 0;
  std::string m_query_coordinate;
};

/**
  vector index assignment
*/
class Rdb_vector_index_assignment {
 public:
  faiss_ivf_list_id m_list_id;
  std::string m_codes;
};

/**
  vector index base class
*/
class Rdb_vector_index {
 public:
  Rdb_vector_index() = default;
  virtual ~Rdb_vector_index() = default;

  /**
    assign a vector to the index
  */
  virtual void assign_vector(const float *data,
                             Rdb_vector_index_assignment &assignment) = 0;

  virtual uint knn_search(
      THD *thd, const TABLE *const tbl, Item *pk_index_cond,
      const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
      Rdb_vector_search_params &params,
      std::vector<std::pair<std::string, float>> &result) = 0;

  virtual uint knn_search_with_value(
      THD *thd, const TABLE *const tbl, Item *pk_index_cond,
      const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
      Rdb_vector_search_params &params,
      std::vector<std::pair<std::string, std::pair<float, std::string>>>
          &result) {
    return HA_ERR_UNSUPPORTED;
  }

  virtual uint knn_search_hybrid_with_value(
      THD *thd, const TABLE *const tbl, Item *pk_index_cond,
      const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
      Rdb_vector_search_params &params,
      std::vector<std::pair<std::string, std::pair<float, std::string>>>
          &result) {
    return HA_ERR_UNSUPPORTED;
  }

  virtual uint index_scan(
      THD *thd, const TABLE *const tbl, Item *pk_index_cond,
      const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
      uint nprobe,
      std::unique_ptr<Rdb_vector_db_iterator> &index_scan_result_iter) = 0;

  virtual uint index_scan_with_value(
      THD *thd, const TABLE *const tbl, Item *pk_index_cond,
      const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
      uint nprobe,
      std::vector<std::pair<std::string, std::pair<float, std::string>>>
          &result) {
        return HA_ERR_UNSUPPORTED;
      }

  /**
    scans all vectors in index and populate counters
  */
  virtual uint analyze(THD *thd, uint64_t max_num_rows_scanned,
                       std::atomic<THD::killed_state> *killed) = 0;

  virtual Rdb_vector_index_info dump_info() = 0;

  virtual FB_vector_dimension dimension() const = 0;

  virtual const FB_vector_index_config &get_config() const = 0;

  virtual uint setup(const std::string &db_name [[maybe_unused]],
                     Rdb_cmd_srv_helper &cmd_srv_helper [[maybe_unused]]) {
    return HA_EXIT_SUCCESS;
  }
};

uint create_vector_index(Rdb_cmd_srv_helper &cmd_srv_helper,
                         const std::string &db_name,
                         const FB_vector_index_config index_def,
                         std::shared_ptr<rocksdb::ColumnFamilyHandle> cf_handle,
                         const Index_id index_id,
                         std::unique_ptr<Rdb_vector_index> &index);

/**
  one instance per handler, hold the vector buffers and knn results for the
  handler.
 */
class Rdb_vector_db_handler {
 public:
  Rdb_vector_db_handler();

  bool has_more_results() {
    if (m_search_type == FB_VECTOR_SEARCH_KNN_FIRST) {
      if (!m_search_result.empty()) {
        return m_vector_db_result_iter != m_search_result.cend();
      } else {
        return m_vector_db_result_with_value_iter !=
               m_search_result_with_value.cend();
      }
    } else {
      if (!m_search_result_with_value.empty()) {
        return m_vector_db_result_with_value_iter !=
               m_search_result_with_value.cend();
      } else {
        return m_index_scan_result_iter &&
              m_index_scan_result_iter->is_available();
      }
    }
  }

  void next_result() {
    if (!has_more_results()) return;

    if (m_search_type == FB_VECTOR_SEARCH_KNN_FIRST) {
      if (!m_search_result.empty()) {
        ++m_vector_db_result_iter;
      } else {
        ++m_vector_db_result_with_value_iter;
      }
    } else {
      if (!m_search_result_with_value.empty()) {
        ++m_vector_db_result_with_value_iter;
      } else {
        m_index_scan_result_iter->next();
      }
    }
  }

  uint current_key(std::string &key) const;
  uint current_value(std::string &value) const;

  uint search(THD *thd, const TABLE *const tbl, Rdb_vector_index *index,
              const Rdb_key_def *sk_descr, Item *pk_index_cond);

  uint index_scan(THD *thd, const TABLE *const tbl, Rdb_vector_index *index,
                  const Rdb_key_def *sk_descr, Item *pk_index_cond);

  uint knn_search(THD *thd, const TABLE *const tbl, Rdb_vector_index *index,
                  const Rdb_key_def *sk_descr, Item *pk_index_cond);

  uint knn_search_hybrid(THD *thd, const TABLE *const tbl, Rdb_vector_index *index,
                  const Rdb_key_def *sk_descr, Item *pk_index_cond);

  int vector_index_orderby_init(Item *sort_func) {
    // log_to_file("vector_index_orderby_init");
    Item_func_fb_vector_distance * distance_func;
    Item_func *func = (Item_func *)(sort_func);
    if ((strcmp(func->func_name(), "+") == 0)) {
      // log_to_file("vector_index_orderby_init: parsing weighted distance function ");
      Item *arg0 = (Item *)func->arguments()[0];
      arg0 = arg0->real_item();
      distance_func =
            down_cast<Item_func_fb_vector_distance *>((Item_func *)arg0);
        
    } else{
      distance_func = down_cast<Item_func_fb_vector_distance *>(sort_func);
    }
    m_limit = distance_func->m_limit;
    m_search_type = distance_func->m_search_type;
    m_nprobe = distance_func->m_nprobe;
    // log_to_file("m_search_type: " + std::to_string(m_search_type) +
    //             ", m_limit: " + std::to_string(m_limit) +
    //             ", m_nprobe: " + std::to_string(m_nprobe));
    if (m_search_type == FB_VECTOR_SEARCH_KNN_HYBRID) {
      m_weight = distance_func->m_weight;
      m_query_coordinate = distance_func->m_query_coordinate;
      // log_to_file("m_weight: " + std::to_string(m_weight) +
      //             ", m_query_coordinate: ");
      // log_to_file_raw(m_query_coordinate);
    } 

    auto functype = distance_func->functype();
    if (functype == Item_func::FB_VECTOR_L2) {
      m_metric = FB_VECTOR_INDEX_METRIC::L2;
    } else if (functype == Item_func::FB_VECTOR_IP) {
      m_metric = FB_VECTOR_INDEX_METRIC::IP;
    } else {
      // should never happen
      assert(false);
      return HA_ERR_UNSUPPORTED;
    }

    if (distance_func->get_input_vector(m_buffer)) {
      return HA_EXIT_FAILURE;
    }
    return HA_EXIT_SUCCESS;
  }

  void vector_index_orderby_end() {
    m_search_type = FB_VECTOR_SEARCH_KNN_FIRST;
    m_metric = FB_VECTOR_INDEX_METRIC::NONE;
    // reset ORDER BY related
    m_limit = 0;
    m_nprobe = 0;
    m_buffer.clear();

    if (m_index_scan_result_iter) {
      m_index_scan_result_iter = nullptr;
    }
  }

 private:
  // input vector from the USER query,
  std::vector<float> m_buffer;
  enum_fb_vector_search_type m_search_type = FB_VECTOR_SEARCH_KNN_FIRST;
  std::vector<std::pair<std::string, float>> m_search_result;
  std::vector<std::pair<std::string, std::pair<float, std::string>>> m_search_result_with_value;
  decltype(m_search_result.cbegin()) m_vector_db_result_iter;
  decltype(m_search_result_with_value.cbegin()) m_vector_db_result_with_value_iter;
  std::unique_ptr<Rdb_vector_db_iterator> m_index_scan_result_iter = nullptr;
  FB_VECTOR_INDEX_METRIC m_metric = FB_VECTOR_INDEX_METRIC::NONE;
  // LIMIT associated with the ORDER BY clause
  uint m_limit;
  uint m_nprobe;
  float m_weight;
  std::string m_query_coordinate;

  uint decode_value_to_buffer(Field *field, FB_vector_dimension dimension,
                              std::vector<float> &buffer);
};

}  // namespace myrocks
