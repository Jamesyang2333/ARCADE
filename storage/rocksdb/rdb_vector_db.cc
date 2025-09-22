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

#include "./rdb_vector_db.h"
#include <sys/types.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cmath>
#include <memory>
#include <queue>
#include <string>
#include <string_view>
#include "ha_rocksdb.h"
#include "rdb_buff.h"
#include "rdb_cmd_srv_helper.h"
#include "rdb_global.h"
#include "rdb_iterator.h"
#include "rdb_utils.h"
#include "sql/next_spatial_base.h"
#ifdef WITH_FB_VECTORDB
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/utils/Heap.h>
#include <faiss/utils/distances.h>
#endif
#include <rocksdb/db.h>

namespace myrocks {

#ifdef WITH_FB_VECTORDB
namespace {
// Helper function to extract a specific field from an encoded value slice
static uint DecodeFieldFromValue(
    rocksdb::BlockBasedTableOptions::TableConfig& table_config,
    std::vector<rocksdb::BlockBasedTableOptions::FieldInfo>& field_info_list,
    std::vector<size_t>& field_indexs,
    const rocksdb::Slice& value,
    std::vector<rocksdb::Slice>* field_values) {
  // Clear the output vector first
  field_values->clear();

  // 1. Setup string reader for value slice  
  const char* pos = value.data();
  size_t remaining = value.size();

  // 2. Skip TTL bytes if configured
  if (table_config.has_ttl) {
    if (remaining < 8) {
      return HA_ERR_ROCKSDB_CORRUPT_DATA;
    }
    pos += 8;
    remaining -= 8;
  }

  // 3. Process null bytes
  const char* null_bytes = nullptr;
  if (table_config.null_bytes_length > 0) {
    if (remaining < table_config.null_bytes_length) {
      return HA_ERR_ROCKSDB_CORRUPT_DATA;
    }
    null_bytes = pos;
    pos += table_config.null_bytes_length;
    remaining -= table_config.null_bytes_length;
  }

  // 4. Skip unpack info if configured
  // temporarily commented out as unpack info set wrongly in test dataset

  // if (table_options.table_config.has_unpack_info) {
  //   if (remaining < 3) {
  //     return Status::Corruption("Value too short for unpack info header");
  //   }
  //   // Validate unpack info tag
  //   if (pos[0] != 0) { // First byte should be tag=0
  //     return Status::Corruption("Invalid unpack info tag");
  //   }
  //   // Get unpack info length from next 2 bytes
  //   uint16_t unpack_len = *reinterpret_cast<const uint16_t*>(pos + 1);
  //   if (remaining < unpack_len) {
  //     return Status::Corruption("Value too short for unpack info data");
  //   }
  //   pos += unpack_len;
  //   remaining -= unpack_len;
  // }

  // 5. Find target field info 
  // If field_indexs is empty, return an empty result
  if (field_indexs.empty()) {
    return HA_EXIT_SUCCESS;
  }
  
  // Find the maximum field index to determine how far we need to scan
  size_t max_field_index = 0;
  for (size_t idx : field_indexs) {
    if (idx >= field_info_list.size()) {
      return HA_ERR_ROCKSDB_CORRUPT_DATA;
    }
    max_field_index = std::max(max_field_index, idx);
  }

  // Pre-allocate the output vector
  field_values->resize(field_indexs.size());

  // Create a mapping from field index to position in result vector
  std::unordered_map<size_t, size_t> index_to_result_pos;
  for (size_t i = 0; i < field_indexs.size(); i++) {
    index_to_result_pos[field_indexs[i]] = i;
  }

  // Parse through all fields up to max_field_index
  for (size_t i = 0; i <= max_field_index; i++) {
    const auto& field_info = field_info_list[i];

    // Check if current field is null
    bool is_null = false;
    if (field_info.is_nullable && null_bytes != nullptr) {
      is_null = (null_bytes[i/8] & (1 << (i%8))) != 0;
    }

    // Check if this field is in our target list
    bool is_target_field = index_to_result_pos.find(i) != index_to_result_pos.end();

    if (is_null) {
      // If this is a target field, add an empty slice to the result
      if (is_target_field) {
        (*field_values)[index_to_result_pos[i]] = rocksdb::Slice();
      }
      // Skip to next field (no data to skip for null fields)
      continue;
    }

    // Handle field data based on type
    if (field_info.type == MYSQL_TYPE_VARCHAR) {
      if (remaining < field_info.length_bytes) {
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      size_t len = (field_info.length_bytes == 1) ? 
                  static_cast<uint8_t>(pos[0]) : *reinterpret_cast<const uint16_t*>(pos);
      pos += field_info.length_bytes;
      remaining -= field_info.length_bytes;
      
      if (remaining < len) {
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      
      // If this is a target field, add it to the result
      if (is_target_field) {
        (*field_values)[index_to_result_pos[i]] = rocksdb::Slice(pos, len);
      }
      
      // Move past this field's data
      pos += len;
      remaining -= len;
    } else if (field_info.type == MYSQL_TYPE_BLOB || 
               field_info.type == MYSQL_TYPE_JSON || 
               field_info.type == MYSQL_TYPE_GEOMETRY) {
      if (remaining < field_info.length_bytes) {
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      size_t len = 0;
      memcpy(&len, pos, field_info.length_bytes);
      pos += field_info.length_bytes;
      remaining -= field_info.length_bytes;
      
      if (remaining < len) {
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      
      // If this is a target field, add it to the result
      if (is_target_field) {
        (*field_values)[index_to_result_pos[i]] = rocksdb::Slice(pos, len);
      }
      
      // Move past this field's data
      pos += len;
      remaining -= len;
    } else {
      // Fixed length field
      if (remaining < field_info.pack_length) {
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      
      // If this is a target field, add it to the result
      if (is_target_field) {
        (*field_values)[index_to_result_pos[i]] = rocksdb::Slice(pos, field_info.pack_length);
      }
      
      // Move past this field's data
      pos += field_info.pack_length;
      remaining -= field_info.pack_length;
    }
  }

  return HA_EXIT_SUCCESS;
}

float l2_distance(const std::vector<float>& a, const std::vector<float>& b) {
    float sum = 0.0f;
    for (size_t i = 0; i < a.size(); ++i) {
        float diff = a[i] - b[i];
        sum += diff * diff;
    }

    return std::sqrt(sum);
}

double st_distance_simple(double lon1_deg, double lat1_deg,
                                 double lon2_deg, double lat2_deg)
{
    constexpr double R = 6'371'008.8;          // mean Earth radius (m)

    auto deg2rad = [](double d) { return d * M_PI / 180.0; };

    double lat1 = deg2rad(lat1_deg);
    double lat2 = deg2rad(lat2_deg);
    double dlat = deg2rad(lat2_deg - lat1_deg);
    double dlon = deg2rad(lon2_deg - lon1_deg);

    double sin_dlat = std::sin(dlat * 0.5);
    double sin_dlon = std::sin(dlon * 0.5);

    double a = sin_dlat * sin_dlat +
               std::cos(lat1) * std::cos(lat2) * sin_dlon * sin_dlon;

    double c = 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));
    return R * c;                             // metres (arc length)
}

template <typename T>
int ExtractVectorFromJson(const std::string_view& json_binary, std::vector<T>* result) {
  assert(result != nullptr);
  result->clear();

  // Ensure we have at least a type byte
  if (json_binary.size() < 1) {
    return HA_ERR_ROCKSDB_CORRUPT_DATA;
  }

  const uint8_t* data = reinterpret_cast<const uint8_t*>(json_binary.data());
  size_t length = json_binary.size();

  // Check JSON type - we need array (small 0x02 or large 0x03)
  uint8_t type = data[0];
  if (type != 0x02 && type != 0x03) {
    return HA_ERR_ROCKSDB_CORRUPT_DATA;
  }

  // Determine if this is large format (4-byte offsets) or small format (2-byte)
  bool large_format = (type == 0x03);
  size_t offset_size = large_format ? 4 : 2;

  // Parse header:
  // 1 byte: type
  // 2 or 4 bytes: element count
  // 2 or 4 bytes: total size
  if (length < 1 + 2 * offset_size) {
    return HA_ERR_ROCKSDB_CORRUPT_DATA;
  }

  // Read element count
  uint32_t element_count = 0;
  if (large_format) {
    element_count = static_cast<uint32_t>(data[1]) |
                   (static_cast<uint32_t>(data[2]) << 8) |
                   (static_cast<uint32_t>(data[3]) << 16) |
                   (static_cast<uint32_t>(data[4]) << 24);
  } else {
    element_count =
        static_cast<uint32_t>(data[1]) | (static_cast<uint32_t>(data[2]) << 8);
  }

  // Skip size field - we don't need it
  size_t header_size = 1 + 2 * offset_size;

  // Calculate value entries start
  size_t value_entries_start = header_size;
  size_t value_entry_size = 1 + offset_size;  // 1 byte type + offset

  // Preallocate result vector
  result->reserve(element_count);

  // Process each element
  for (uint32_t i = 0; i < element_count; i++) {
    size_t entry_pos = value_entries_start + i * value_entry_size;
    if (entry_pos + value_entry_size > length) {
      return HA_ERR_ROCKSDB_CORRUPT_DATA;
    }

    // Get value type and offset
    uint8_t value_type = data[entry_pos];
    size_t value_offset = 0;
    bool inlined = false;

    // Check if value type can be inlined
    inlined = (value_type == 0x04 ||                    // literal
              value_type == 0x05 ||                    // int16
              value_type == 0x06 ||                    // uint16
              (value_type == 0x07 && large_format) ||  // int32 in large format
              (value_type == 0x08 && large_format));  // uint32 in large format

    if (inlined) {
      // Value is inlined in the offset field
      if (large_format) {
        value_offset = static_cast<uint32_t>(data[entry_pos + 1]) |
                      (static_cast<uint32_t>(data[entry_pos + 2]) << 8) |
                      (static_cast<uint32_t>(data[entry_pos + 3]) << 16) |
                      (static_cast<uint32_t>(data[entry_pos + 4]) << 24);
      } else {
        value_offset = static_cast<uint16_t>(data[entry_pos + 1]) |
                      (static_cast<uint16_t>(data[entry_pos + 2]) << 8);
      }
    } else {
      // Value offset points to where the actual value is stored
      if (large_format) {
        value_offset = static_cast<uint32_t>(data[entry_pos + 1]) |
                      (static_cast<uint32_t>(data[entry_pos + 2]) << 8) |
                      (static_cast<uint32_t>(data[entry_pos + 3]) << 16) |
                      (static_cast<uint32_t>(data[entry_pos + 4]) << 24);
        value_offset += 1;  // absolute position
      } else {
        value_offset = static_cast<uint16_t>(data[entry_pos + 1]) |
                      (static_cast<uint16_t>(data[entry_pos + 2]) << 8);
        value_offset += 1;  // absolute position
      }

      if (value_offset >= length) {
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
    }

    // Extract value based on type and convert to T
    switch (value_type) {
      case 0x04: {  // literal
        uint8_t lit_val = static_cast<uint8_t>(value_offset);
        if constexpr (std::is_same_v<T, bool>) {
          if (lit_val == 0x01)
            result->push_back(true);
          else if (lit_val == 0x02)
            result->push_back(false);
          else
            return HA_ERR_ROCKSDB_CORRUPT_DATA;
        } else if constexpr (std::is_integral_v<T>) {
          // Convert true/false to 1/0 for numeric types
          if (lit_val == 0x01)
            result->push_back(1);
          else if (lit_val == 0x02)
            result->push_back(0);
          else
            result->push_back(0);  // null becomes 0
        } else {
          return HA_ERR_ROCKSDB_CORRUPT_DATA;
        }
        break;
      }

      case 0x05: {  // int16
        if constexpr (std::is_arithmetic_v<T>) {
          int16_t val = static_cast<int16_t>(value_offset);
          result->push_back(static_cast<T>(val));
        } else {
          return HA_ERR_ROCKSDB_CORRUPT_DATA;
        }
        break;
      }

      case 0x06: {  // uint16
        if constexpr (std::is_arithmetic_v<T>) {
          uint16_t val = static_cast<uint16_t>(value_offset);
          result->push_back(static_cast<T>(val));
        } else {
          return HA_ERR_ROCKSDB_CORRUPT_DATA;
        }
        break;
      }

      case 0x07: {  // int32
        if constexpr (std::is_arithmetic_v<T>) {
          int32_t val;
          if (inlined) {
            val = static_cast<int32_t>(value_offset);
          } else {
            val = static_cast<int32_t>(data[value_offset]) |
                 (static_cast<int32_t>(data[value_offset + 1]) << 8) |
                 (static_cast<int32_t>(data[value_offset + 2]) << 16) |
                 (static_cast<int32_t>(data[value_offset + 3]) << 24);
          }
          result->push_back(static_cast<T>(val));
        } else {
          return HA_ERR_ROCKSDB_CORRUPT_DATA;
        }
        break;
      }

      case 0x08: {  // uint32
        if constexpr (std::is_arithmetic_v<T>) {
          uint32_t val;
          if (inlined) {
            val = static_cast<uint32_t>(value_offset);
          } else {
            val = static_cast<uint32_t>(data[value_offset]) |
                 (static_cast<uint32_t>(data[value_offset + 1]) << 8) |
                 (static_cast<uint32_t>(data[value_offset + 2]) << 16) |
                 (static_cast<uint32_t>(data[value_offset + 3]) << 24);
          }
          result->push_back(static_cast<T>(val));
        } else {
          return HA_ERR_ROCKSDB_CORRUPT_DATA;
        }
        break;
      }

      case 0x09: {  // int64
        if constexpr (std::is_arithmetic_v<T>) {
          if (value_offset + 8 > length) {
            return HA_ERR_ROCKSDB_CORRUPT_DATA;
          }
          int64_t val = static_cast<int64_t>(data[value_offset]) |
                       (static_cast<int64_t>(data[value_offset + 1]) << 8) |
                       (static_cast<int64_t>(data[value_offset + 2]) << 16) |
                       (static_cast<int64_t>(data[value_offset + 3]) << 24) |
                       (static_cast<int64_t>(data[value_offset + 4]) << 32) |
                       (static_cast<int64_t>(data[value_offset + 5]) << 40) |
                       (static_cast<int64_t>(data[value_offset + 6]) << 48) |
                       (static_cast<int64_t>(data[value_offset + 7]) << 56);
          result->push_back(static_cast<T>(val));
        } else {
          return HA_ERR_ROCKSDB_CORRUPT_DATA;
        }
        break;
      }

      case 0x0A: {  // uint64
        if constexpr (std::is_arithmetic_v<T>) {
          if (value_offset + 8 > length) {
            return HA_ERR_ROCKSDB_CORRUPT_DATA;
          }
          uint64_t val = static_cast<uint64_t>(data[value_offset]) |
                        (static_cast<uint64_t>(data[value_offset + 1]) << 8) |
                        (static_cast<uint64_t>(data[value_offset + 2]) << 16) |
                        (static_cast<uint64_t>(data[value_offset + 3]) << 24) |
                        (static_cast<uint64_t>(data[value_offset + 4]) << 32) |
                        (static_cast<uint64_t>(data[value_offset + 5]) << 40) |
                        (static_cast<uint64_t>(data[value_offset + 6]) << 48) |
                        (static_cast<uint64_t>(data[value_offset + 7]) << 56);
          result->push_back(static_cast<T>(val));
        } else {
          return HA_ERR_ROCKSDB_CORRUPT_DATA;
        }
        break;
      }

      case 0x0B: {  // double
        if constexpr (std::is_arithmetic_v<T>) {
          if (value_offset + 8 > length) {
            return HA_ERR_ROCKSDB_CORRUPT_DATA;
          }

          // Read the 8 bytes as a double
          double val;
          memcpy(&val, &data[value_offset], sizeof(double));
          result->push_back(static_cast<T>(val));
        } else {
          return HA_ERR_ROCKSDB_CORRUPT_DATA;
        }
        break;
      }

      case 0x0C: {  // string
        // For strings, we need to read the variable-length format
        if constexpr (std::is_same_v<T, std::string>) {
          // Read variable length
          uint32_t str_length = 0;
          uint8_t bytes_read = 0;
          size_t pos = value_offset;

          do {
            if (pos >= length) {
              return HA_ERR_ROCKSDB_CORRUPT_DATA;
            }

            uint8_t byte = data[pos++];
            str_length |= (byte & 0x7F) << (7 * bytes_read);
            bytes_read++;

            if (!(byte & 0x80)) break;
          } while (bytes_read < 5);  // Sanity check - MySQL strings shouldn't need more than 5 bytes for length

          if (pos + str_length > length) {
            return HA_ERR_ROCKSDB_CORRUPT_DATA;
          }

          result->push_back(std::string(
              reinterpret_cast<const char*>(&data[pos]), str_length));
        } else {
          return HA_ERR_ROCKSDB_CORRUPT_DATA;
        }
        break;
      }

      default:
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
    }
  }

  return HA_EXIT_SUCCESS;
}

// vector ids are generated in read time.
// use this dummy value for apis require passing vector ids.
constexpr faiss::idx_t DUMMY_VECTOR_ID = 42;

static void write_inverted_list_key(Rdb_string_writer &writer,
                                    const Index_id index_id,
                                    const size_t list_id) {
  writer.write_index_id(index_id);
  writer.write_uint64(list_id);
}

/**
  rocksdb key for vectors
  key format is:
  index_id + list_id + pk
 */
static void write_inverted_list_item_key(Rdb_string_writer &writer,
                                         const Index_id index_id,
                                         const size_t list_id,
                                         const rocksdb::Slice &pk) {
  write_inverted_list_key(writer, index_id, list_id);
  assert(pk.size() > INDEX_NUMBER_SIZE);
  rocksdb::Slice pk_without_index_id{pk};
  pk_without_index_id.remove_prefix(INDEX_NUMBER_SIZE);
  writer.write_slice(pk_without_index_id);
}

/**
read and verify key prefix
*/
static uint read_inverted_list_key(Rdb_string_reader &reader,
                                   const Index_id index_id, size_t list_id) {
  Index_id actual_index_id;
  if (reader.read_index_id(&actual_index_id)) {
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "Failed to read index id for key in index %d", index_id);
    return HA_ERR_ROCKSDB_CORRUPT_DATA;
  }
  if (actual_index_id != index_id) {
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "Invalid index id for key in index %d, actual value %d",
                    index_id, actual_index_id);
    assert(false);
    return HA_ERR_ROCKSDB_CORRUPT_DATA;
  }

  std::uint64_t actual_list_id;
  if (reader.read_uint64(&actual_list_id)) {
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "Failed to read list id for key in index %d", index_id);
    return HA_ERR_ROCKSDB_CORRUPT_DATA;
  }
  if (actual_list_id != list_id) {
    LogPluginErrMsg(
        ERROR_LEVEL, ER_LOG_PRINTF_MSG,
        "Invalid list id for key in index %d, actual value %" PRIu64, index_id,
        actual_list_id);
    return HA_ERR_ROCKSDB_CORRUPT_DATA;
  }
  return HA_EXIT_SUCCESS;
}

/**
  context passed to inverted list.
  no need to synchronize here, as we set openmp threads to 1.
*/
class Rdb_faiss_inverted_list_context {
 public:
  explicit Rdb_faiss_inverted_list_context(THD *thd, const TABLE *const tbl,
                                           Item *pk_index_cond,
                                           const Rdb_key_def *sk_descr)
      : m_thd(thd),
        m_tbl(tbl),
        m_pk_index_cond(pk_index_cond),
        m_sk_descr(sk_descr) {}
  THD *m_thd;
  const TABLE *const m_tbl;
  Item *m_pk_index_cond;
  const Rdb_key_def *m_sk_descr;
  uint m_error = HA_EXIT_SUCCESS;
  std::size_t m_current_list_size = 0;
  // list id to list size pairs
  std::vector<std::pair<std::size_t, std::size_t>> m_list_size_stats;

  void on_iterator_end(std::size_t list_id) {
    if (!m_error) {
      // only record list size when there is no error
      m_list_size_stats.push_back({list_id, m_current_list_size});
    }
    m_current_list_size = 0;
  }

  void on_iterator_record() { m_current_list_size++; }

  faiss::idx_t add_key(const std::string &key) {
    auto vector_id = m_vector_id++;
    m_vectorid_key.emplace(vector_id, key);
    return vector_id;
  }

  uint populate_result(std::vector<faiss::idx_t> &vector_ids,
                       std::vector<float> &distances,
                       std::vector<std::pair<std::string, float>> &result) {
    for (uint i = 0; i < vector_ids.size(); i++) {
      auto vector_id = vector_ids[i];
      if (vector_id < 0) {
        break;
      }
      auto iter = m_vectorid_key.find(vector_id);
      if (iter == m_vectorid_key.end()) {
        LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                        "Failed to find matching pk for %" PRIu64, vector_id);
        return HA_EXIT_FAILURE;
      }
      result.emplace_back(iter->second, distances[i]);
    }
    return HA_EXIT_SUCCESS;
  }

 private:
  std::map<faiss::idx_t, std::string> m_vectorid_key;
  // current vector id
  faiss::idx_t m_vector_id = 1024;
};

/**
  context passed to inverted list for adding vectors.
  no need to synchronize here, as we set openmp threads to 1.
*/
class Rdb_faiss_inverted_list_write_context {
 public:
  Rdb_faiss_inverted_list_write_context(rocksdb::WriteBatchBase *wb,
                                        const rocksdb::Slice &pk)
      : m_write_batch(wb), m_pk(pk) {}
  rocksdb::WriteBatchBase *m_write_batch;
  const rocksdb::Slice &m_pk;
  rocksdb::Status m_status;
};

/**
  iterate a inverted list
*/
class Rdb_vector_iterator : public faiss::InvertedListsIterator {
 public:
  Rdb_vector_iterator(Rdb_faiss_inverted_list_context *context,
                      Index_id index_id, rocksdb::ColumnFamilyHandle &cf,
                      const uint code_size, size_t list_id)
      : m_context(context),
        m_index_id(index_id),
        m_list_id(list_id),
        m_code_size(code_size) {
    Rdb_string_writer lower_key_writer;
    write_inverted_list_key(lower_key_writer, index_id, list_id);
    m_iterator_lower_bound_key.PinSelf(lower_key_writer.to_slice());

    Rdb_string_writer upper_key_writer;
    write_inverted_list_key(upper_key_writer, index_id, list_id + 1);
    m_iterator_upper_bound_key.PinSelf(upper_key_writer.to_slice());
    m_iterator = rdb_tx_get_iterator(
        context->m_thd, cf, /* skip_bloom_filter */ true,
        m_iterator_lower_bound_key, m_iterator_upper_bound_key,
        /* snapshot */ nullptr, TABLE_TYPE::USER_TABLE);
    m_iterator->SeekToFirst();
  }

  void next() override { m_iterator->Next(); }

  bool is_available() const override {
    THD *thd = m_context->m_thd;
    std::string sk;
    std::string sk_value;
    rocksdb::Slice key_slice;
    rocksdb::Slice value_slice;

    while (m_iterator->Valid() && !m_context->m_error) {
      /* if the thread is killed, set error in context and break */
      if (thd && thd->killed) {
        m_context->m_error = HA_ERR_QUERY_INTERRUPTED;
        break;
      }

      /* if there's no PK condition to filter on, then break and return
       * status to FAISS right away
       */
      if (!m_context->m_pk_index_cond) break;

      /* get the SK tuple from rocksdb iterator */
      m_context->m_error = get_key_and_value(sk, sk_value);

      /* if there's an error, terminatate the iterator in FAISS */
      if (m_context->m_error) break;

      key_slice = rocksdb::Slice(sk);
      value_slice = rocksdb::Slice(sk_value);

      /* unpack SK tuple

       * Note: even though the SK tuple obtained above includes the list_id,
       * it gets skipped during the unpacking due to fpi->m_covered being set
       * to Rdb_key_def::KEY_NOT_COVERED for the vector column and
       * m_max_image_len (bytes that gets skipped) set to
       * sizeof(faiss_ivf_list_id)

       */
      m_context->m_error = m_context->m_sk_descr->unpack_record(
          const_cast<TABLE *>(m_context->m_tbl), m_context->m_tbl->record[0],
          &key_slice, &value_slice, false);

      /* propagate error and terminate iterator in case of unpacking error */
      if (m_context->m_error) break;

      /* evaluate PK condition and filter */
      if (m_context->m_pk_index_cond->val_int()) break;

      /* clear buffers for next round */
      sk.clear();
      sk_value.clear();

      /* move on to the next record */
      m_iterator->Next();
    }

    bool available = !m_context->m_error && m_iterator->Valid();

    if (!available) {
      m_context->on_iterator_end(m_list_id);
    }
    return available;
  }

  uint get_key_and_value(std::string &key, std::string &value,
                         bool need_value = true) const {
    assert(m_context->m_error == false);
    assert(m_iterator->Valid());

    rocksdb::Slice key_slice = m_iterator->key();
    Rdb_string_reader key_reader(&key_slice);

    // validate the key format
    uint rtn = read_inverted_list_key(key_reader, m_index_id, m_list_id);
    if (rtn) {
      return rtn;
    }
    const auto pk_size = key_reader.remaining_bytes();
    if (pk_size == 0) {
      LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "Invalid pk in index %d, list id %lu", m_index_id,
                      m_list_id);
      return HA_ERR_ROCKSDB_CORRUPT_DATA;
    }
    // copy the key bytes
    key = key_slice.ToString();

    if (!need_value) {
      return HA_EXIT_SUCCESS;
    }

    /*
      Vector index Value format:
      [DATA TAG] [VECTOR CODES] [UNPACK INFO FOR CHARS/VARCHARS etc]

      To unpack CHARs and VARCHARs, we need to have a complete VALUE
      corresponding to the KEY above. This is needed to be able to
      evaluate PK query conditions on CHAR/VARCHAR PK key parts.

      The vector codes are not expected in VALUE during the unpacking,
      and the following code recreates VALUE minus the vector codes.

     */
    rocksdb::Slice value_slice = m_iterator->value();
    unsigned long int value_bytes = value_slice.size() - m_code_size;
    if (value_bytes < 0) {
      LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "Invalid value size %lu for key in index %d, list id %lu",
                      value_slice.size(), m_index_id, m_list_id);
      return HA_ERR_ROCKSDB_CORRUPT_DATA;
    }
    if (value_bytes > 0) {
      char tag = value_slice.data()[0];
      if (!Rdb_key_def::is_unpack_data_tag(tag)) {
        LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                        "Invalid data tag for key in index %d, list id %lu",
                        m_index_id, m_list_id);
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      auto header_size = Rdb_key_def::get_unpack_header_size(tag);
      if ((size_t)value_bytes < header_size) {
        LogPluginErrMsg(
            ERROR_LEVEL, ER_LOG_PRINTF_MSG,
            "Invalid value size %lu for key in index %d, list id %lu",
            value_slice.size(), m_index_id, m_list_id);
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }

      value.reserve(value_bytes);

      for (uint i = 0; i < header_size; i++) {
        value += value_slice.data()[i];
      }

      for (uint i = header_size; i < value_bytes; i++) {
        value += value_slice.data()[i + m_code_size];
      }
    }
    assert(value.size() == value_bytes);

    return HA_EXIT_SUCCESS;
  }

  uint get_key_and_codes(std::string &key, rocksdb::Slice &codes) const {
    assert(m_context->m_error == false);
    assert(m_iterator->Valid());

    rocksdb::Slice key_slice = m_iterator->key();
    Rdb_string_reader key_reader(&key_slice);

    // validate the key format
    uint rtn = read_inverted_list_key(key_reader, m_index_id, m_list_id);
    if (rtn) {
      return rtn;
    }
    const auto pk_size = key_reader.remaining_bytes();
    if (pk_size == 0) {
      LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "Invalid pk in index %d, list id %lu", m_index_id,
                      m_list_id);
      return HA_ERR_ROCKSDB_CORRUPT_DATA;
    }
    // copy the key bytes
    key = key_slice.ToString();

    rocksdb::Slice value = m_iterator->value();
    codes = value;
    // after consolidating write logic to use Rdb_key_def::pack_record,
    // the vector codes is prefixed with data tag, and followed by
    // other column data.
    // for data written before the change, the value is exactly m_code_size.
    int extra_bytes = codes.size() - m_code_size;
    if (extra_bytes < 0) {
      LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "Invalid value size %lu for key in index %d, list id %lu",
                      codes.size(), m_index_id, m_list_id);
      return HA_ERR_ROCKSDB_CORRUPT_DATA;
    }
    if (extra_bytes > 0) {
      char tag = codes.data()[0];
      if (!Rdb_key_def::is_unpack_data_tag(tag)) {
        LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                        "Invalid data tag for key in index %d, list id %lu",
                        m_index_id, m_list_id);
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      auto header_size = Rdb_key_def::get_unpack_header_size(tag);
      if ((size_t)extra_bytes < header_size) {
        LogPluginErrMsg(
            ERROR_LEVEL, ER_LOG_PRINTF_MSG,
            "Invalid value size %lu for key in index %d, list id %lu",
            codes.size(), m_index_id, m_list_id);
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }

      codes.remove_prefix(header_size);
      codes.remove_suffix(extra_bytes - header_size);
    }
    assert(codes.size() == m_code_size);

    m_context->on_iterator_record();
    return HA_EXIT_SUCCESS;
  }

  std::pair<faiss::idx_t, const uint8_t *> get_id_and_codes() override {
    std::string key;
    rocksdb::Slice codes;
    uint rtn = get_key_and_codes(key, codes);
    if (rtn) {
      // set error to context so faiss can stop iterating
      m_context->m_error = rtn;
      // return some dummy data to faiss so it does not crash
      faiss::idx_t vector_id = 42;
      m_codes_buffer.resize(m_code_size);
      return {vector_id, m_codes_buffer.data()};
    }

    faiss::idx_t vector_id = m_context->add_key(key);
    return {vector_id, reinterpret_cast<const uint8_t *>(codes.data())};
  }

 private:
  Rdb_faiss_inverted_list_context *m_context;
  Index_id m_index_id;
  size_t m_list_id;
  uint m_code_size;
  std::unique_ptr<rocksdb::Iterator> m_iterator;
  rocksdb::PinnableSlice m_iterator_lower_bound_key;
  rocksdb::PinnableSlice m_iterator_upper_bound_key;
  std::vector<uint8_t> m_codes_buffer;
};

class Rdb_vector_list_iterator : public Rdb_vector_db_iterator {
 public:
  Rdb_vector_list_iterator(Rdb_faiss_inverted_list_context &&context,
                           Index_id index_id,
                           rocksdb::ColumnFamilyHandle *const cf,
                           const uint code_size,
                           std::vector<faiss::idx_t> &&list_ids)
      : m_index_id(index_id),
        m_code_size(code_size),
        m_cf(*cf),
        m_context(context),
        m_list_ids(list_ids) {
    m_list_id_iter = m_list_ids.begin();
  }

  bool is_available() override {
    if (m_error) return false;

    while (m_current_iterator == nullptr ||
           !m_current_iterator->is_available()) {
      if (m_error || m_list_id_iter == m_list_ids.end() ||
          *m_list_id_iter < 0) {
        break;
      }
      m_current_iterator.reset(new Rdb_vector_iterator(
          &m_context, m_index_id, m_cf, m_code_size, *m_list_id_iter));
      m_list_id_iter++;
    }

    return m_current_iterator != nullptr && m_current_iterator->is_available();
  }

  void next() override { m_current_iterator->next(); }

  uint get_key(std::string &key) override {
    std::string value;
    uint rtn = m_current_iterator->get_key_and_value(key, value, false);
    if (rtn) {
      m_error = rtn;
      return rtn;
    }
    return rtn;
  }

 private:
  Index_id m_index_id;
  uint m_code_size;
  rocksdb::ColumnFamilyHandle &m_cf;
  Rdb_faiss_inverted_list_context m_context;
  std::vector<faiss::idx_t> m_list_ids;
  std::unique_ptr<Rdb_vector_iterator> m_current_iterator = nullptr;
  std::vector<faiss::idx_t>::iterator m_list_id_iter;
  uint m_error = HA_EXIT_SUCCESS;
};

/**
  faiss inverted list implementation.
  throws exceptions for methods that are not used for our use case.
*/
class Rdb_faiss_inverted_list : public faiss::InvertedLists {
 public:
  Rdb_faiss_inverted_list(Index_id index_id, rocksdb::ColumnFamilyHandle &cf,
                          uint nlist, uint code_size)
      : InvertedLists(nlist, code_size), m_index_id(index_id), m_cf(cf) {
    use_iterator = true;
  }
  ~Rdb_faiss_inverted_list() override = default;

  Rdb_faiss_inverted_list(const Rdb_faiss_inverted_list &) = delete;
  Rdb_faiss_inverted_list &operator=(const Rdb_faiss_inverted_list &) = delete;
  Rdb_faiss_inverted_list(Rdb_faiss_inverted_list &&) = delete;
  Rdb_faiss_inverted_list &operator=(Rdb_faiss_inverted_list &&) = delete;

  size_t list_size(size_t list_no) const override {
    throw std::runtime_error(std::string("unexpected function call ") +
                             __PRETTY_FUNCTION__);
  }

  faiss::InvertedListsIterator *get_iterator(
      size_t list_no, void *inverted_list_context) const override {
    // faiss is responsible for releasing the iterator object
    assert(inverted_list_context);
    return new Rdb_vector_iterator(
        reinterpret_cast<Rdb_faiss_inverted_list_context *>(
            inverted_list_context),
        m_index_id, m_cf, code_size, list_no);
  }

  const uint8_t *get_codes(size_t list_no) const override {
    throw std::runtime_error(std::string("unexpected function call ") +
                             __PRETTY_FUNCTION__);
  }

  const faiss::idx_t *get_ids(size_t list_no) const override {
    throw std::runtime_error(std::string("unexpected function call ") +
                             __PRETTY_FUNCTION__);
  }

  size_t add_entry(size_t list_no, faiss::idx_t theid, const uint8_t *code,
                   void *inverted_list_context) override {
    assert(theid == DUMMY_VECTOR_ID);
    assert(inverted_list_context);
    Rdb_vector_index_assignment *context =
        reinterpret_cast<Rdb_vector_index_assignment *>(inverted_list_context);
    context->m_list_id = list_no;
    context->m_codes =
        std::string(reinterpret_cast<const char *>(code), code_size);
    // the return value is the offset in the list, not used for our use case.
    // always return 0 here.
    return 0;
  }

  size_t add_entries(size_t list_no, size_t n_entry, const faiss::idx_t *ids,
                     const uint8_t *code) override {
    throw std::runtime_error(std::string("unexpected function call ") +
                             __PRETTY_FUNCTION__);
  }

  void update_entries(size_t list_no, size_t offset, size_t n_entry,
                      const faiss::idx_t *ids, const uint8_t *code) override {
    throw std::runtime_error(std::string("unexpected function call ") +
                             __PRETTY_FUNCTION__);
  }

  void resize(size_t list_no, size_t new_size) override {
    throw std::runtime_error(std::string("unexpected function call ") +
                             __PRETTY_FUNCTION__);
  }

 private:
  Index_id m_index_id;
  rocksdb::ColumnFamilyHandle &m_cf;
};

class Rdb_vector_lsm_iterator {
 public:
  Rdb_vector_lsm_iterator(THD *thd, Index_id index_id,
                          rocksdb::ColumnFamilyHandle &cf, std::vector<float> &query_vector, uint k, uint nprobe)
      : m_index_id(index_id), m_query_vector(query_vector), m_k(k), m_nprobe(nprobe) {

    m_iterator = rdb_tx_get_iterator_vector(
        thd, cf, /* snapshot */ nullptr, TABLE_TYPE::USER_TABLE, m_query_vector, k, nprobe);
  }

  void seek_to_first() {m_iterator -> SeekToFirst();}

  bool is_available() const { return m_iterator->Valid(); }

  void next() { m_iterator->Next(); }

  rocksdb::Slice key() {
    return m_iterator->key();
  }

  rocksdb::Slice value() {
    return m_iterator->value();
  }

  std::string return_key_str() {return m_iterator->key().ToString();}

  std::string return_val_str() {return m_iterator->value().ToString();}

 private:
  Index_id m_index_id;
  std::unique_ptr<rocksdb::Iterator> m_iterator;
  std::vector<float> m_query_vector;
  uint m_k;
  uint m_nprobe;
  rocksdb::PinnableSlice m_iterator_lower_bound_key;
  rocksdb::PinnableSlice m_iterator_upper_bound_key;
};

class Rdb_vector_index_lsm : public Rdb_vector_index {
 public:
  Rdb_vector_index_lsm(const FB_vector_index_config index_def,
                       std::shared_ptr<rocksdb::ColumnFamilyHandle> cf_handle,
                       const Index_id index_id)
      : m_index_id{index_id},
        m_index_def{index_def},
        m_cf_handle{cf_handle} {}

  ~Rdb_vector_index_lsm() override = default;

  void assign_vector(const float *data,
                     Rdb_vector_index_assignment &assignment) override {
    assignment.m_list_id = 0;
    assignment.m_codes.assign(
        reinterpret_cast<const char *>(data),
        dimension() * sizeof(float));
  }

  uint knn_search(
      THD *thd, const TABLE *const tbl, Item *pk_index_cond,
      const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
      Rdb_vector_search_params &params,
      std::vector<std::pair<std::string, float>> &result) override {
    return HA_ERR_UNSUPPORTED;
  }

  uint knn_search_with_value(
      THD *thd, const TABLE *const tbl, Item *pk_index_cond,
      const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
      Rdb_vector_search_params &params,
      std::vector<std::pair<std::string, std::pair<float, std::string>>>
          &result) override;

  uint knn_search_hybrid_with_value(
      THD *thd, const TABLE *const tbl, Item *pk_index_cond,
      const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
      Rdb_vector_search_params &params,
      std::vector<std::pair<std::string, std::pair<float, std::string>>>
          &result) override;

  uint index_scan(
      THD *thd, const TABLE *const tbl, Item *pk_index_cond,
      const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
      uint nprobe,
      std::unique_ptr<Rdb_vector_db_iterator> &index_scan_result_iter)
      override {
        return HA_ERR_UNSUPPORTED;
      };

  uint index_scan_with_value(
      THD *thd, const TABLE *const tbl, Item *pk_index_cond,
      const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
      uint nprobe,
      std::vector<std::pair<std::string, std::pair<float, std::string>>>
          &result)
      override;

  uint analyze(THD *thd, uint64_t max_num_rows_scanned,
               std::atomic<THD::killed_state> *killed) override {
    return HA_EXIT_SUCCESS;
  }

  Rdb_vector_index_info dump_info() override {
    return {.m_ntotal = m_ntotal, .m_hit = m_hit};
  }

  FB_vector_dimension dimension() const override {
    return m_index_def.dimension();
  }

  const FB_vector_index_config &get_config() const override {              
    return m_index_def; }     

  virtual uint setup(const std::string &db_name,
                     Rdb_cmd_srv_helper &cmd_srv_helper) override {
    // // load pre-trained centroids and quantizer from the database                  
    // std::unique_ptr<Rdb_vector_index_data> index_data;
    // const std::string trained_index_table =
    //     to_string(m_index_def.trained_index_table());
    // auto status = cmd_srv_helper.load_index_data(
    //     db_name, trained_index_table,
    //     to_string(m_index_def.trained_index_id()), index_data);
    // if (status.error()) {
    //   LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
    //                   "Failed to load vector index data. %s",
    //                   status.message().c_str());
    //   return HA_EXIT_FAILURE;
    // }
    // if (index_data->m_nlist <= 0) {
    //   LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, "Invalid nlist %d",
    //                   index_data->m_nlist);
    //   return HA_EXIT_FAILURE;
    // }

    // rtn = create_index(m_index_l2, index_data.get(), faiss::METRIC_L2);
    // if (rtn) {
    //   return rtn;
    // }
    return HA_EXIT_SUCCESS;
  }

 private:
  Index_id m_index_id;
  FB_vector_index_config m_index_def;
  std::shared_ptr<rocksdb::ColumnFamilyHandle> m_cf_handle;
  std::atomic<uint> m_hit{0};
  std::atomic<int64_t> m_ntotal{0};
};

uint Rdb_vector_index_lsm::knn_search_with_value(
    THD *thd, const TABLE *const tbl, Item *pk_index_cond,
    const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
    Rdb_vector_search_params &params,
    std::vector<std::pair<std::string, std::pair<float, std::string>>> &result) {
  m_hit++;
  result.clear();

  uint k = params.m_k;

  using PqElement = std::pair<float, std::pair<std::string, std::string>>;
  auto cmp = [](const PqElement &a, const PqElement &b) {
    return a.first < b.first;
  };
  std::priority_queue<PqElement, std::vector<PqElement>, decltype(cmp)>
      top_k_heap(cmp);

  // log_to_file("knn search with value, query_vector size: " + std::to_string(query_vector.size()) +
  //             " elements, k: " + std::to_string(k) +
  //             ", nprobe: " + std::to_string(params.m_nprobe));

  Rdb_vector_lsm_iterator iter(thd, m_index_id, *m_cf_handle.get(),
                               query_vector, params.m_k, params.m_nprobe);
  // log_to_file("iterator initialized");
  int64_t keys_scanned = 0;

  std::vector<rocksdb::BlockBasedTableOptions::FieldInfo> field_info_list;
  rocksdb::BlockBasedTableOptions::TableConfig table_config;

  int s = get_table_info(m_cf_handle.get()->GetName(), &field_info_list, &table_config);
  if (s) {
    // log_to_file("Failed to get table info for column family: " +
    //             m_cf_handle.get()->GetName());
    return HA_ERR_ROCKSDB_CORRUPT_DATA;
  }
  // log_to_file("table info retrieved, field_info_list size: " +
  //             std::to_string(field_info_list.size()));

  // iter.seek_to_first();
  // log_to_file("iterator seeked to first");
  for (iter.seek_to_first(); iter.is_available(); iter.next()) {
    // log_to_file("inside loop, iterator is available");
    keys_scanned++;

    if (iter.return_val_str().size() != 0) {
      // log_to_file("iterator results: ");
      // log_to_file("key size: " + std::to_string(iter.return_key_str().size()) +
      //             ", value size: " +
      //             std::to_string(iter.return_val_str().size()));
      // log_to_file_raw(iter.return_key_str());
      // log_to_file_raw(iter.return_val_str());
      std::vector<rocksdb::Slice> index_fields;
      std::vector<size_t> field_indexes_to_extract = {8};
      int s_decode = DecodeFieldFromValue(
          table_config, field_info_list, field_indexes_to_extract,
          iter.value(), &index_fields);
      if (s_decode) {
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      std::vector<float> vector_data;
      rocksdb::Slice index_field = index_fields[0];
      // log_to_file("geting binary from index field, size: " +
      //             std::to_string(index_field.size()));
      std::string_view json_binary(index_field.data(), index_field.size());
      // log_to_file("starting to extract vector from json, size: " +
      //             std::to_string(json_binary.size()));
      int s =
          ExtractVectorFromJson<float>(json_binary, &vector_data);
      if (s) {
        // log_to_file("Failed to extract vector from json: " +
        //             std::to_string(s));
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      // log_to_file("vector_data:" +
      //     std::to_string(vector_data.size()) + " elements, status: " +
      //     std::to_string(s));
        
      // float distance = l2_distance(query_vector, vector_data);
      float distance = faiss::fvec_L2sqr(
          query_vector.data(), vector_data.data(), query_vector.size());
      // log_to_file("distance: " + std::to_string(distance));

      if (top_k_heap.size() < k) {
        top_k_heap.push({distance,
                         {iter.return_key_str(), iter.return_val_str()}});
      } else if (distance < top_k_heap.top().first) {
        top_k_heap.pop();
        top_k_heap.push({distance,
                         {iter.return_key_str(), iter.return_val_str()}});
      }
    }
  }

  result.reserve(top_k_heap.size());
  while (!top_k_heap.empty()) {
    const auto &top = top_k_heap.top();
    result.emplace_back(top.second.first,
                        std::make_pair(top.first, top.second.second));
    top_k_heap.pop();
  }
  std::reverse(result.begin(), result.end());

  // log_to_file("number of records from iterator: " +
  //             std::to_string(keys_scanned));
  // log_to_file("size of iterator result: " + std::to_string(result.size()));

  return HA_EXIT_SUCCESS;
}

uint Rdb_vector_index_lsm::knn_search_hybrid_with_value(
    THD *thd, const TABLE *const tbl, Item *pk_index_cond,
    const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
    Rdb_vector_search_params &params,
    std::vector<std::pair<std::string, std::pair<float, std::string>>> &result) {
  m_hit++;
  result.clear();

  uint k = params.m_k;

  using PqElement = std::pair<float, std::pair<std::string, std::string>>;
  auto cmp = [](const PqElement &a, const PqElement &b) {
    return a.first < b.first;
  };
  std::priority_queue<PqElement, std::vector<PqElement>, decltype(cmp)>
      top_k_heap(cmp);

  // log_to_file("knn search with value, query_vector size: " + std::to_string(query_vector.size()) +
  //             " elements, k: " + std::to_string(k) +
  //             ", nprobe: " + std::to_string(params.m_nprobe));

  Rdb_vector_lsm_iterator iter(thd, m_index_id, *m_cf_handle.get(),
                               query_vector, params.m_k, params.m_nprobe);
  // log_to_file("iterator initialized");
  int64_t keys_scanned = 0;

  std::vector<rocksdb::BlockBasedTableOptions::FieldInfo> field_info_list;
  rocksdb::BlockBasedTableOptions::TableConfig table_config;

  int s = get_table_info(m_cf_handle.get()->GetName(), &field_info_list, &table_config);
  if (s) {
    // log_to_file("Failed to get table info for column family: " +
    //             m_cf_handle.get()->GetName());
    return HA_ERR_ROCKSDB_CORRUPT_DATA;
  }
  // log_to_file("table info retrieved, field_info_list size: " +
  //             std::to_string(field_info_list.size()));
  std::vector<size_t> field_indexes_to_extract = {};
  int spatial_field_index;
  int vector_field_index;
  int count = 0;
    // check if field contains spatial index
    for (const auto& field_info : field_info_list) {
      if (field_info.type == 255) {
        spatial_field_index = 0;
        field_indexes_to_extract.push_back(count);
        break;
      }
      count++;
    }

    // check if field contains vector index
    count = 0;
    for (const auto& field_info : field_info_list) {
      if (field_info.type == 245) {
        if (!field_indexes_to_extract.empty()) {
          vector_field_index = 1;
        } else {
          vector_field_index = 0;
        }
        field_indexes_to_extract.push_back(count);
        break;
      }
      count++;
    }

  std::vector<float> query_coordinates;

  double lon_query = *reinterpret_cast<const double*>(params.m_query_coordinate.data() + 9);
  double lat_query = *reinterpret_cast<const double*>(params.m_query_coordinate.data() + 17);
  query_coordinates.push_back(lon_query);
  query_coordinates.push_back(lat_query);
  // log_to_file("query_coordinates: " +
  //             std::to_string(query_coordinates.size()) + " elements, ymin: " +
  //             std::to_string(lon_query) + ", xmin: " + std::to_string(lat_query));


  // iter.seek_to_first();
  // log_to_file("iterator seeked to first");
  for (iter.seek_to_first(); iter.is_available(); iter.next()) {
    // log_to_file("inside loop, iterator is available");
    keys_scanned++;

    if (iter.return_val_str().size() != 0) {
      // log_to_file("iterator results: ");
      // log_to_file("key size: " + std::to_string(iter.return_key_str().size()) +
      //             ", value size: " +
      //             std::to_string(iter.return_val_str().size()));
      // log_to_file_raw(iter.return_key_str());
      // log_to_file_raw(iter.return_val_str());
      std::vector<rocksdb::Slice> index_fields;
      int s_decode = DecodeFieldFromValue(
          table_config, field_info_list, field_indexes_to_extract,
          iter.value(), &index_fields);
      if (s_decode) {
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      std::vector<float> vector_data;
      rocksdb::Slice index_field = index_fields[vector_field_index];
      // log_to_file("geting binary from index field, size: " +
      //             std::to_string(index_field.size()));
      std::string_view json_binary(index_field.data(), index_field.size());
      // log_to_file("starting to extract vector from json, size: " +
      //             std::to_string(json_binary.size()));
      int s =
          ExtractVectorFromJson<float>(json_binary, &vector_data);
      if (s) {
        // log_to_file("Failed to extract vector from json: " +
        //             std::to_string(s));
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      // log_to_file("vector_data:" +
      //     std::to_string(vector_data.size()) + " elements, status: " +
      //     std::to_string(s));

      std::vector<double> spatial_coordinate;
      rocksdb::Slice index_field_spatial = index_fields[spatial_field_index];
      double lon = *reinterpret_cast<const double*>(index_field_spatial.data() + 9);
      double lat = *reinterpret_cast<const double*>(index_field_spatial.data() + 17);
      // log_to_file("spatial coordinate: lon: " + std::to_string(lon) +
      //             ", lat: " + std::to_string(lat));
      

      float distance_spatial = st_distance_simple(query_coordinates[0], query_coordinates[1], lon, lat);
      float distance = faiss::fvec_L2sqr(
          query_vector.data(), vector_data.data(), query_vector.size());
      // log_to_file("vector distance: " + std::to_string(distance));
      // log_to_file("spatial distance: " + std::to_string(distance_spatial));
      // log_to_file("combined distance: " +
      //             std::to_string(distance + params.m_weight*distance_spatial));

      if (top_k_heap.size() < k) {
        top_k_heap.push({distance + params.m_weight*distance_spatial,
                         {iter.return_key_str(), iter.return_val_str()}});
      } else if (distance < top_k_heap.top().first) {
        top_k_heap.pop();
        top_k_heap.push({distance,
                         {iter.return_key_str(), iter.return_val_str()}});
      }
    }
  }

  result.reserve(top_k_heap.size());
  while (!top_k_heap.empty()) {
    const auto &top = top_k_heap.top();
    result.emplace_back(top.second.first,
                        std::make_pair(top.first, top.second.second));
    top_k_heap.pop();
  }
  std::reverse(result.begin(), result.end());

  // log_to_file("number of records from iterator: " +
  //             std::to_string(keys_scanned));
  // log_to_file("size of iterator result: " + std::to_string(result.size()));

  return HA_EXIT_SUCCESS;
}

uint Rdb_vector_index_lsm::index_scan_with_value(
    THD *thd, const TABLE *const tbl, Item *pk_index_cond,
    const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
    uint nprobe,
    std::vector<std::pair<std::string, std::pair<float, std::string>>> &result) {
  m_hit++;
  result.clear();

  uint k = 100; // default k value for index scan

  using PqElement = std::pair<float, std::pair<std::string, std::string>>;
  auto cmp = [](const PqElement &a, const PqElement &b) {
    return a.first < b.first;
  };
  std::priority_queue<PqElement, std::vector<PqElement>, decltype(cmp)>
      top_k_heap(cmp);

  // log_to_file("query_vector size: " + std::to_string(query_vector.size()) +
  //             " elements, k: " + std::to_string(k) +
  //             ", nprobe: " + std::to_string(params.m_nprobe));

  Rdb_vector_lsm_iterator iter(thd, m_index_id, *m_cf_handle.get(),
                               query_vector, 500, nprobe);
  // log_to_file("iterator initialized");
  int64_t keys_scanned = 0;

  std::vector<rocksdb::BlockBasedTableOptions::FieldInfo> field_info_list;
  rocksdb::BlockBasedTableOptions::TableConfig table_config;

  int s = get_table_info(m_cf_handle.get()->GetName(), &field_info_list, &table_config);
  if (s) {
    // log_to_file("Failed to get table info for column family: " +
                // m_cf_handle.get()->GetName());
    return HA_ERR_ROCKSDB_CORRUPT_DATA;
  }
  // log_to_file("table info retrieved, field_info_list size: " +
  //             std::to_string(field_info_list.size()));

  for (iter.seek_to_first(); iter.is_available(); iter.next()) {
    keys_scanned++;

    // log_to_file("iterator results: ");
    // log_to_file("key size: " + std::to_string(iter.return_key_str().size()) +
    //             ", value size: " +
    //             std::to_string(iter.return_val_str().size()));
    // log_to_file_raw(iter.return_key_str());
    // log_to_file_raw(iter.return_val_str());

    if (iter.return_val_str().size() != 0) {
      std::vector<rocksdb::Slice> index_fields;
      std::vector<size_t> field_indexes_to_extract = {8};
      int s_decode = DecodeFieldFromValue(
          table_config, field_info_list, field_indexes_to_extract,
          iter.value(), &index_fields);
      if (s_decode) {
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      std::vector<float> vector_data;
      rocksdb::Slice index_field = index_fields[0];
      std::string_view json_binary(index_field.data(), index_field.size());
      int s =
          ExtractVectorFromJson<float>(json_binary, &vector_data);
      if (s) {
        // log_to_file("Failed to extract vector from json: " +
        //             std::to_string(s));
        return HA_ERR_ROCKSDB_CORRUPT_DATA;
      }
      // log_to_file("vector_data:" +
      //     std::to_string(vector_data.size()) + " elements, status: " +
      //     std::to_string(s));
        
      // float distance = l2_distance(query_vector, vector_data);
      float distance = faiss::fvec_L2sqr(
          query_vector.data(), vector_data.data(), query_vector.size());
      // log_to_file("distance: " + std::to_string(distance));

      if (top_k_heap.size() < k) {
        top_k_heap.push({distance,
                         {iter.return_key_str(), iter.return_val_str()}});
      } else if (distance < top_k_heap.top().first) {
        top_k_heap.pop();
        top_k_heap.push({distance,
                         {iter.return_key_str(), iter.return_val_str()}});
      }
    }
  }

  result.reserve(top_k_heap.size());
  while (!top_k_heap.empty()) {
    const auto &top = top_k_heap.top();
    result.emplace_back(top.second.first,
                        std::make_pair(top.first, top.second.second));
    top_k_heap.pop();
  }
  std::reverse(result.begin(), result.end());

  // log_to_file("number of records from iterator: " +
  //             std::to_string(keys_scanned));
  // log_to_file("size of iterator result: " + std::to_string(result.size()));

  return HA_EXIT_SUCCESS;
}

class Rdb_vector_index_ivf : public Rdb_vector_index {
 public:
  Rdb_vector_index_ivf(const FB_vector_index_config index_def,
                       std::shared_ptr<rocksdb::ColumnFamilyHandle> cf_handle,
                       const Index_id index_id)
      : m_index_id{index_id}, m_index_def{index_def}, m_cf_handle{cf_handle} {}

  virtual ~Rdb_vector_index_ivf() override = default;

  void assign_vector(const float *data,
                     Rdb_vector_index_assignment &assignment) override {
    faiss_ivf_list_id list_id = get_list_id(data);
    constexpr faiss::idx_t vector_count = 1;
    // vector id is not actually used, use a dummy value here
    m_index_l2->add_core(vector_count, data, &DUMMY_VECTOR_ID, &list_id,
                         &assignment);
  }

  FB_vector_dimension dimension() const override {
    return m_index_def.dimension();
  }

  const FB_vector_index_config &get_config() const override {             
    return m_index_def; }     

  virtual uint index_scan(THD *thd, const TABLE *const tbl, Item *pk_index_cond,
                          const Rdb_key_def *sk_descr,
                          std::vector<float> &query_vector, uint nprobe,
                          std::unique_ptr<Rdb_vector_db_iterator>
                              &index_scan_result_iter) override {
    m_hit++;

    constexpr faiss::idx_t vector_count = 1;
    std::vector<faiss::idx_t> vector_ids(nprobe);
    std::vector<float> distances(nprobe);

    m_quantizer->search(vector_count, query_vector.data(), nprobe,
                        distances.data(), vector_ids.data());

    Rdb_faiss_inverted_list_context context(thd, tbl, pk_index_cond, sk_descr);
    index_scan_result_iter.reset(new Rdb_vector_list_iterator(
        std::move(context), m_index_id, m_cf_handle.get(),
        m_index_l2->code_size, std::move(vector_ids)));

    return HA_EXIT_SUCCESS;
  }

  virtual uint knn_search(
      THD *thd, const TABLE *const tbl, Item *pk_index_cond,
      const Rdb_key_def *sk_descr, std::vector<float> &query_vector,
      Rdb_vector_search_params &params,
      std::vector<std::pair<std::string, float>> &result) override {
    m_hit++;
    faiss::IndexIVF *index = m_index_l2.get();
    if (params.m_metric == FB_VECTOR_INDEX_METRIC::IP) {
      index = m_index_ip.get();
    }
    faiss::idx_t k = params.m_k;
    std::vector<faiss::idx_t> vector_ids(k);
    std::vector<float> distances(k);
    constexpr faiss::idx_t vector_count = 1;
    faiss::IVFSearchParameters search_params;

    search_params.nprobe = params.m_nprobe;
    Rdb_faiss_inverted_list_context context(thd, tbl, pk_index_cond, sk_descr);
    search_params.inverted_list_context = &context;
    index->search(vector_count, query_vector.data(), k, distances.data(),
                  vector_ids.data(), &search_params);
    if (context.m_error) {
      return context.m_error;
    }
    auto rtn = context.populate_result(vector_ids, distances, result);
    if (rtn) {
      return rtn;
    }

    // update counters
    for (auto &list_size_entry : context.m_list_size_stats) {
      m_list_size_stats[list_size_entry.first] = list_size_entry.second;
    }
    return HA_EXIT_SUCCESS;
  }

  virtual uint analyze(THD *thd, uint64_t max_num_rows_scanned,
                       std::atomic<THD::killed_state> *killed) override {
    assert(thd);
    std::string key;
    rocksdb::Slice codes;
    uint64_t ntotal = 0;
    for (std::size_t i = 0; i < m_list_size_stats.size(); i++) {
      std::size_t list_size = 0;
      Rdb_faiss_inverted_list_context context(thd, nullptr, nullptr, nullptr);
      Rdb_vector_iterator vector_iter(&context, m_index_id, *m_cf_handle,
                                      m_index_l2->code_size, i);
      while (vector_iter.is_available()) {
        uint rtn = vector_iter.get_key_and_codes(key, codes);
        if (rtn) {
          return rtn;
        }
        list_size++;
        ntotal++;
        if (max_num_rows_scanned > 0 && ntotal > max_num_rows_scanned) {
          return HA_EXIT_SUCCESS;
        }
        if (killed && *killed) {
          return HA_EXIT_FAILURE;
        }
        vector_iter.next();
      }
      m_list_size_stats[i] = list_size;
    }
    return HA_EXIT_SUCCESS;
  }

  virtual uint setup(const std::string &db_name,
                     Rdb_cmd_srv_helper &cmd_srv_helper) override {
    std::unique_ptr<Rdb_vector_index_data> index_data;
    if (m_index_def.type() == FB_VECTOR_INDEX_TYPE::FLAT) {
      // flat is ivf flat with 1 list
      index_data = std::make_unique<Rdb_vector_index_data>();
      index_data->m_nlist = 1;
      index_data->m_quantizer_codes.resize(m_index_def.dimension(), 0.0);
    } else {
      const std::string trained_index_table =
          to_string(m_index_def.trained_index_table());
      auto status = cmd_srv_helper.load_index_data(
          db_name, trained_index_table,
          to_string(m_index_def.trained_index_id()), index_data);
      if (status.error()) {
        LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                        "Failed to load vector index data. %s",
                        status.message().c_str());
        return HA_EXIT_FAILURE;
      }
    }
    if (index_data->m_nlist <= 0) {
      LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, "Invalid nlist %d",
                      index_data->m_nlist);
      return HA_EXIT_FAILURE;
    }
    if (m_index_def.type() == FB_VECTOR_INDEX_TYPE::IVFPQ) {
      if (index_data->m_pq_m <= 0 || index_data->m_pq_nbits <= 0) {
        LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                        "Invalid pq m %d, pq nbits %d", index_data->m_pq_m,
                        index_data->m_pq_nbits);
        return HA_EXIT_FAILURE;
      }
      if (index_data->m_pq_codes.empty()) {
        LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                        "pq codes is required for IVFPQ");
        return HA_EXIT_FAILURE;
      }
    }
    uint rtn = setup_quantizer(index_data.get());
    if (rtn) {
      return rtn;
    }

    rtn = create_index(m_index_l2, index_data.get(), faiss::METRIC_L2);
    if (rtn) {
      return rtn;
    }
    rtn =
        create_index(m_index_ip, index_data.get(), faiss::METRIC_INNER_PRODUCT);
    if (rtn) {
      return rtn;
    }

    // create inverted list
    m_inverted_list = std::make_unique<Rdb_faiss_inverted_list>(
        m_index_id, *m_cf_handle, m_index_l2->nlist, m_index_l2->code_size);
    m_index_l2->replace_invlists(m_inverted_list.get());
    m_index_ip->replace_invlists(m_inverted_list.get());

    // initialize the list size stats. does not allow resize here
    // because atomic is not move insertable
    m_list_size_stats = std::vector<std::atomic<long>>(m_index_l2->nlist);
    for (auto &list_size : m_list_size_stats) {
      list_size.store(-1);
    }
    return HA_EXIT_SUCCESS;
  }

  Rdb_vector_index_info dump_info() override {
    uint ntotal = 0;
    std::optional<uint> min_list_size;
    std::optional<uint> max_list_size;
    std::vector<uint> list_size_stats;
    list_size_stats.reserve(m_list_size_stats.size());
    for (const auto &list_size : m_list_size_stats) {
      const auto list_size_value = list_size.load();
      if (list_size_value >= 0) {
        ntotal += list_size_value;
        list_size_stats.push_back(list_size_value);
        if (!min_list_size.has_value() ||
            list_size_value < min_list_size.value()) {
          min_list_size = list_size_value;
        }
        if (!max_list_size.has_value() ||
            list_size_value > max_list_size.value()) {
          max_list_size = list_size_value;
        }
      }
    }
    uint avg_list_size =
        list_size_stats.empty() ? 0 : ntotal / list_size_stats.size();
    // compute median value of list size
    std::sort(list_size_stats.begin(), list_size_stats.end());
    uint median_list_size = list_size_stats.empty()
                                ? 0
                                : list_size_stats[list_size_stats.size() / 2];
    uint pq_m = 0;
    uint pq_nbits = 0;
    if (m_index_def.type() == FB_VECTOR_INDEX_TYPE::IVFPQ) {
      faiss::IndexIVFPQ *index_ivfpq =
          dynamic_cast<faiss::IndexIVFPQ *>(m_index_l2.get());
      pq_m = index_ivfpq->pq.M;
      pq_nbits = index_ivfpq->pq.nbits;
    }
    return {.m_ntotal = ntotal,
            .m_hit = m_hit,
            .m_code_size = m_index_l2->code_size,
            .m_nlist = m_index_l2->nlist,
            .m_pq_m = pq_m,
            .m_pq_nbits = pq_nbits,
            .m_min_list_size = min_list_size.value_or(0),
            .m_max_list_size = max_list_size.value_or(0),
            .m_avg_list_size = avg_list_size,
            .m_median_list_size = median_list_size};
  }

 private:
  Index_id m_index_id;
  FB_vector_index_config m_index_def;
  std::shared_ptr<rocksdb::ColumnFamilyHandle> m_cf_handle;
  std::atomic<uint> m_hit{0};
  std::unique_ptr<faiss::IndexFlatL2> m_quantizer;
  std::unique_ptr<faiss::IndexIVF> m_index_l2;
  std::unique_ptr<faiss::IndexIVF> m_index_ip;
  std::unique_ptr<Rdb_faiss_inverted_list> m_inverted_list;
  std::vector<std::atomic<long>> m_list_size_stats;

  uint delete_vector_from_list(rocksdb::WriteBatchBase *write_batch,
                               const uint64 list_id, const rocksdb::Slice &pk) {
    Rdb_string_writer key_writer;
    write_inverted_list_item_key(key_writer, m_index_id, list_id, pk);
    auto status = write_batch->Delete(m_cf_handle.get(), key_writer.to_slice());
    if (!status.ok()) {
      LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "Failed to write codes for index %d", m_index_id);
      return ha_rocksdb::rdb_error_to_mysql(status);
    }
    return HA_EXIT_SUCCESS;
  }

  uint64 get_list_id(const float *data) const {
    if (m_index_l2->nlist == 1) {
      return 0;
    }
    faiss::idx_t list_id = 0;
    constexpr faiss::idx_t vector_count = 1;
    m_index_l2->quantizer->assign(vector_count, data, &list_id);
    return list_id;
  }

  uint setup_quantizer(Rdb_vector_index_data *index_data) {
    m_quantizer = std::make_unique<faiss::IndexFlatL2>(m_index_def.dimension());
    const auto total_code_size =
        index_data->m_quantizer_codes.size() * sizeof(float);
    const auto ncentroids = index_data->m_nlist;
    if (total_code_size != ncentroids * m_quantizer->code_size) {
      LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                      "Invalid codes, total code size %lu.", total_code_size);
      return HA_EXIT_FAILURE;
    }
    m_quantizer->add(ncentroids, index_data->m_quantizer_codes.data());
    return HA_EXIT_SUCCESS;
  }

  uint create_index(std::unique_ptr<faiss::IndexIVF> &index,
                    Rdb_vector_index_data *index_data,
                    faiss::MetricType metric_type) {
    const auto ncentroids = index_data->m_nlist;
    if (m_index_def.type() == FB_VECTOR_INDEX_TYPE::FLAT ||
        m_index_def.type() == FB_VECTOR_INDEX_TYPE::IVFFLAT) {
      index = std::make_unique<faiss::IndexIVFFlat>(
          m_quantizer.get(), m_index_def.dimension(), ncentroids, metric_type);
    } else {
      auto ivfpq_index = std::make_unique<faiss::IndexIVFPQ>(
          m_quantizer.get(), m_index_def.dimension(), ncentroids,
          index_data->m_pq_m, index_data->m_pq_nbits, metric_type);
      // pq centroids is already resized to the correct size
      if (ivfpq_index->pq.centroids.size() != index_data->m_pq_codes.size()) {
        LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                        "Invalid pq codes, expected code size %lu.",
                        ivfpq_index->pq.centroids.size());
        return HA_EXIT_FAILURE;
      }

      ivfpq_index->pq.centroids = index_data->m_pq_codes;
      ivfpq_index->precompute_table();

      index = std::move(ivfpq_index);
    }
    index->is_trained = true;
    return HA_EXIT_SUCCESS;
  }
};

}  // anonymous namespace

uint create_vector_index(Rdb_cmd_srv_helper &cmd_srv_helper,
                         const std::string &db_name,
                         const FB_vector_index_config index_def,
                         std::shared_ptr<rocksdb::ColumnFamilyHandle> cf_handle,
                         const Index_id index_id,
                         std::unique_ptr<Rdb_vector_index> &index) {
  if (index_def.type() == FB_VECTOR_INDEX_TYPE::FLAT ||
      index_def.type() == FB_VECTOR_INDEX_TYPE::IVFFLAT ||
      index_def.type() == FB_VECTOR_INDEX_TYPE::IVFPQ) {
    index =
        std::make_unique<Rdb_vector_index_ivf>(index_def, cf_handle, index_id);
  } else if (index_def.type() == FB_VECTOR_INDEX_TYPE::LSMIDX) {
    index =
        std::make_unique<Rdb_vector_index_lsm>(index_def, cf_handle, index_id);
  } else {
    assert(false);
    return HA_ERR_UNSUPPORTED;
  }
  return index->setup(db_name, cmd_srv_helper);
}

#else

// dummy implementation for non-fbvectordb builds
uint create_vector_index(Rdb_cmd_srv_helper &cmd_srv_helper [[maybe_unused]],
                         const std::string &db_name [[maybe_unused]],
                         const FB_vector_index_config index_def
                         [[maybe_unused]],
                         std::shared_ptr<rocksdb::ColumnFamilyHandle> cf_handle
                         [[maybe_unused]],
                         const Index_id index_id [[maybe_unused]],
                         std::unique_ptr<Rdb_vector_index> &index) {
  index = nullptr;
  return HA_ERR_UNSUPPORTED;
}

#endif

Rdb_vector_db_handler::Rdb_vector_db_handler() {}

uint Rdb_vector_db_handler::search(THD *thd, const TABLE *const tbl,
                                   Rdb_vector_index *index,
                                   const Rdb_key_def *sk_descr,
                                   Item *pk_index_cond) {
  assert((m_search_type == FB_VECTOR_SEARCH_INDEX_SCAN) ||
         (m_search_type == FB_VECTOR_SEARCH_KNN_FIRST) || 
         (m_search_type == FB_VECTOR_SEARCH_KNN_HYBRID));
  

  if (m_search_type == FB_VECTOR_SEARCH_KNN_FIRST) {
    // log_to_file("m_search_type == FB_VECTOR_SEARCH_KNN_FIRST, m_limit: " + std::to_string(m_limit) + "; m_nprobe = " + std::to_string(m_nprobe));
    return knn_search(thd, tbl, index, sk_descr, pk_index_cond);
  } else if (m_search_type == FB_VECTOR_SEARCH_KNN_HYBRID) {
    return knn_search_hybrid(thd, tbl, index, sk_descr, pk_index_cond);
  }
  else {
    // log_to_file("m_search_type == FB_VECTOR_SEARCH_INDEX_SCAN");
    return index_scan(thd, tbl, index, sk_descr, pk_index_cond);
  }
}

uint Rdb_vector_db_handler::index_scan(THD *thd, const TABLE *const tbl,
                                       Rdb_vector_index *index,
                                       const Rdb_key_def *sk_descr,
                                       Item *pk_index_cond) {
  if (!m_buffer.size()) return HA_ERR_END_OF_FILE;

  if (m_buffer.size() < index->dimension()) {
    m_buffer.resize(index->dimension(), 0.0);
  } else if (m_buffer.size() > index->dimension()) {
    LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                    "query vector dimension is too big for vector index");
    return HA_EXIT_FAILURE;
  }

  uint rtn = index->index_scan(thd, tbl, pk_index_cond, sk_descr, m_buffer,
                           m_nprobe, m_index_scan_result_iter);

  if (rtn == HA_ERR_UNSUPPORTED) {
    rtn = index->index_scan_with_value(thd, tbl, pk_index_cond, sk_descr, m_buffer, m_nprobe,
                            m_search_result_with_value);
    if (rtn) {
      return rtn;
    }
    m_vector_db_result_with_value_iter = m_search_result_with_value.cbegin();
  }

  return rtn;
}

uint Rdb_vector_db_handler::knn_search(THD *thd, const TABLE *const tbl,
                                       Rdb_vector_index *index,
                                       const Rdb_key_def *sk_descr,
                                       Item *pk_index_cond) {
  m_search_result.clear();
  m_search_result_with_value.clear();

  m_vector_db_result_iter = m_search_result.cend();
  m_vector_db_result_with_value_iter = m_search_result_with_value.cend();

  if (!m_buffer.size() || !m_limit) return HA_ERR_END_OF_FILE;

  if (m_buffer.size() < index->dimension()) {
    m_buffer.resize(index->dimension(), 0.0);
  } else if (m_buffer.size() > index->dimension()) {
    LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                    "query vector dimension is too big for vector index");
    return HA_EXIT_FAILURE;
  }

  Rdb_vector_search_params params{
      .m_metric = m_metric, .m_k = m_limit, .m_nprobe = m_nprobe};
  uint rtn = index->knn_search_with_value(thd, tbl, pk_index_cond, sk_descr,
                                          m_buffer, params,
                                          m_search_result_with_value);
  if (rtn == HA_ERR_UNSUPPORTED) {
    rtn = index->knn_search(thd, tbl, pk_index_cond, sk_descr, m_buffer, params,
                            m_search_result);
  }

  if (rtn) {
    return rtn;
  }
  m_vector_db_result_iter = m_search_result.cbegin();
  m_vector_db_result_with_value_iter = m_search_result_with_value.cbegin();

  return rtn;
}

uint Rdb_vector_db_handler::knn_search_hybrid(THD *thd, const TABLE *const tbl,
                                       Rdb_vector_index *index,
                                       const Rdb_key_def *sk_descr,
                                       Item *pk_index_cond) {
  m_search_result_with_value.clear();

  m_vector_db_result_with_value_iter = m_search_result_with_value.cend();

  if (!m_buffer.size() || !m_limit) return HA_ERR_END_OF_FILE;

  if (m_buffer.size() < index->dimension()) {
    m_buffer.resize(index->dimension(), 0.0);
  } else if (m_buffer.size() > index->dimension()) {
    LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                    "query vector dimension is too big for vector index");
    return HA_EXIT_FAILURE;
  }

  Rdb_vector_search_params params{
      .m_metric = m_metric, .m_k = m_limit * 5, .m_nprobe = m_nprobe, .m_weight = m_weight, .m_query_coordinate = m_query_coordinate};
  uint rtn = index->knn_search_hybrid_with_value(thd, tbl, pk_index_cond, sk_descr,
                                          m_buffer, params,
                                          m_search_result_with_value);

  if (rtn) {
    return rtn;
  }
  m_vector_db_result_with_value_iter = m_search_result_with_value.cbegin();

  return rtn;
}

uint Rdb_vector_db_handler::current_key(std::string &key) const {
  if (m_search_type == FB_VECTOR_SEARCH_KNN_FIRST) {
    if (!m_search_result.empty()) {
      key = m_vector_db_result_iter->first;
    } else {
      key = m_vector_db_result_with_value_iter->first;
    }
    return HA_EXIT_SUCCESS;
  } else {
    if(!m_search_result_with_value.empty()) {
      key = m_vector_db_result_with_value_iter->first;
      return HA_EXIT_SUCCESS;
    } else {
      return m_index_scan_result_iter->get_key(key);
    }
  }
}

uint Rdb_vector_db_handler::current_value(std::string &value) const {
  if (!m_search_result_with_value.empty()) {
    value = m_vector_db_result_with_value_iter->second.second;
    return HA_EXIT_SUCCESS;
  }
  return HA_ERR_UNSUPPORTED;
}

}  // namespace myrocks
