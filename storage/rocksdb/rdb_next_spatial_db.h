/*
   Copyright (c) 2025, Yang Jingyi

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
   #include <vector>
   #ifdef WITH_NEXT_SPATIALDB
   #endif
   #include <rocksdb/slice.h>
   #include <rocksdb/write_batch.h>
   #include <memory>
   #include "rdb_utils.h"
   #include "./rdb_cmd_srv_helper.h"
   #include "./rdb_global.h"
   #include "sql/item_geofunc.h"
   #include "sql/sql_class.h"
   #include "sql/next_spatial_base.h"
   #include "ha_rocksdb.h"
   
   namespace myrocks {
   
   /** for infomation schema */
   class Rdb_next_spatial_index_info {
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
   };
   
   class Rdb_next_spatial_range_search_params {
    public:
     uint m_distance = 0;
     uint m_batch_size = 0;
   };
   
   /**
     spatial index base class
   
   */
   class Rdb_next_spatial_index {
    public:
     Rdb_next_spatial_index() = default;
     virtual ~Rdb_next_spatial_index() = default;
     /**
       add a spatial entry and its associated pk to the index. (in fact do nothing)
     */
     virtual uint add_spatial_entry(rocksdb::WriteBatchBase *write_batch,
                             const rocksdb::Slice &pk, std::vector<float> &value,
                             const rocksdb::Slice &old_pk,
                             std::vector<float> &old_value) = 0;
   
     /**
       delete a spatial and its associated pk from the index.
     */
     virtual uint delete_vector(rocksdb::WriteBatchBase *write_batch,
                                const rocksdb::Slice &pk,
                                std::vector<float> &old_value) = 0;
   
     virtual uint range_search(
         THD *thd, std::vector<double> &query_mbr,
         Rdb_next_spatial_range_search_params &params,
         std::vector<std::pair<std::string, std::string>> &result) = 0;

    //  virtual uint knn_search(
    //      THD *thd, std::vector<float> &query_vector,
    //      Rdb_vector_search_params &params,
    //      std::vector<std::pair<std::string, float>> &result) = 0;
   
     virtual Rdb_next_spatial_index_info dump_info() = 0;
   
   };
   
   uint create_next_spatial_index(const NEXT_spatial_index_config index_def,
                            std::shared_ptr<rocksdb::ColumnFamilyHandle> cf_handle,
                            const Index_id index_id,
                            std::unique_ptr<Rdb_next_spatial_index> &index);
   
   /**
     one instance per handler, hold the spatial buffers and knn results for the
     handler.
    */
   class Rdb_next_spatial_db_handler {
    public:
     Rdb_next_spatial_db_handler();
     uint decode_value(Field *field) {
       return decode_value_to_buffer(field, m_buffer);
     }
     uint decode_value2(Field *field) {
       return decode_value_to_buffer(field, m_buffer2);
     }
     /**
      get the buffer to store the vector value.
     */
     std::vector<float> &get_buffer() { return m_buffer; }
     std::vector<float> &get_buffer2() { return m_buffer2; }
   
     bool has_more_results() {
        return !m_search_result.empty() &&
              m_next_spatial_db_result_iter != m_search_result.cend();
      }
    
      void next_result() {
        if (has_more_results()) {
          ++m_next_spatial_db_result_iter;
        }
      }
   
     std::string current_pk(const Index_id pk_index_id) const;
     uint current_key(std::string &key) const;
     uint current_value(std::string &value) const;
   
    //  uint knn_search(THD *thd, Rdb_next_spatial_index *index);
     uint range_search(THD *thd, Rdb_next_spatial_index *index, double x_min, double x_max, double y_min, double y_max);
     int next_spatial_index_init(Item *sort_func, uint batch_size) {
      //  log_to_file("Rdb_next_spatial_db_handler next_spatial_index_init");

       m_batch_size = batch_size;
   
      //  input_coordinate to store input query coordinate;
       std::vector<float> input_coordinate;
       Item_func *item_func = (Item_func *)sort_func;
       Item **args = ((Item_func *)item_func)->arguments();
   
       auto func_name = item_func->func_name();
       if (func_name != "st_distance") {
         // should never happen
         assert(false);
         return HA_ERR_UNSUPPORTED;
       }
   
       // input spatial coordinate is expected as the second argument
       uint arg_idx = 1;
       String tmp_str;

        // assert(args[0]->type() == Item::FIELD_ITEM);
        // assert(args[1]->type() == Item::STRING_ITEM);

        Item_func *query_coordinate_from_text = (Item_func *)args[1];
        assert(query_coordinate_from_text->functype == GEOMFROMTEXT);

        // Create a String buffer to use as input/output
        String buffer;

        // Call the function
        String *result = query_coordinate_from_text->val_str(&buffer);
        std::string result_string(result->ptr(), result->length());

        // You can now use `result` (may or may not be the same as &buffer)
        if (result == nullptr) {
            // For example, print the result as hex
            // log_to_file("GEOMETRY WKB with SRID: ");
            // log_to_file(result_string);
        } else {
            assert(false);
        }

        // trying to parse the input coordinate and distance
        // if (parse_spatial_from_item(args, arg_idx, tmp_str, __FUNCTION__,
        //                               input_coordinate))
        //   return HA_EXIT_FAILURE;

        // m_buffer = std::move(input_coordinate.data);
       return HA_EXIT_SUCCESS;
     }
   
     void next_spatial_index_end() {
       m_limit = 0;
       m_buffer.clear();
     }
   
    private:
     // input vector from the USER query,
     // new vector for index write
     std::vector<float> m_buffer;
     // old vector for index write
     std::vector<float> m_buffer2;
     // search result string, vector of pks
     std::vector<std::pair<std::string, std::string>> m_search_result;
     decltype(m_search_result.cbegin()) m_next_spatial_db_result_iter;
     float m_distance;
   
     // LIMIT associated with the ORDER BY clause
     uint m_limit;
     uint m_batch_size;
   
     uint decode_value_to_buffer(Field *field,
                                 std::vector<float> &buffer);
   };

   class Rdb_next_spatial_iterator {
    public:
     Rdb_next_spatial_iterator(THD *thd, Index_id index_id,
                         rocksdb::ColumnFamilyHandle &cf,
                         const std::vector<double> query_coordinates)
         : m_index_id(index_id), m_query_coordinates(query_coordinates) {
       m_iterator = rdb_tx_get_iterator_next_spatial(
           thd, cf, /* snapshot */ nullptr, TABLE_TYPE::USER_TABLE, m_query_coordinates);
     }

     void seek_to_first() {m_iterator -> SeekToFirst();}

     std::string return_key_str() {return m_iterator->key().ToString();}

     std::string return_val_str() {return m_iterator->value().ToString();}
   
     bool is_available() const { return m_iterator->Valid(); }
   
     void next() { m_iterator->Next(); }
   
    private:
     Index_id m_index_id;
    //  size_t m_list_id;
     std::vector<double> m_query_coordinates;
     std::unique_ptr<rocksdb::Iterator> m_iterator;
     rocksdb::PinnableSlice m_iterator_lower_bound_key;
     rocksdb::PinnableSlice m_iterator_upper_bound_key;
   };

   
   }  // namespace myrocks