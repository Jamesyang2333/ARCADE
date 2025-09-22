/*
   Copyright (c) 2025, James Yang

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

#include "sql/item_semantic_func.h"
#include <stdexcept>
// #ifdef WITH_SEMANTICDB
// #endif
#include "sql-common/json_dom.h"
#include "sql/item_json_func.h"
#include "sql/sql_exception_handler.h"
#include "sql/semantic_base.h"

namespace {
#define SEMANTICDB_DISABLED_ERR                                            \
  do {                                                                      \
    my_error(ER_FEATURE_DISABLED, MYF(0), "semantic db", "WITH_SEMANTICDB"); \
    return error_real();                                                    \
  } while (0)
}  // anonymous namespace

bool parse_string_from_item(Item **args, uint arg_idx, String &str,
                     const char *func_name, std::string &value, std::string *field_name) {
  if (args[arg_idx]->data_type() == MYSQL_TYPE_VARCHAR) {
    String *tmp_str = args[arg_idx]->val_str(&str);  // Evaluate the item into `str`
    if (!tmp_str) {
        my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name);
        return true;
    }
    // Copy value into std::string
    value.assign(tmp_str->ptr(), tmp_str->length());
    if (field_name != nullptr) {
      field_name->clear();
    }
    return false;
 }
  if (args[arg_idx]->data_type() == MYSQL_TYPE_BLOB &&
      args[arg_idx]->type() == Item::FIELD_ITEM) {
    const Item_field *fi = down_cast<const Item_field *>(args[arg_idx]);
    if (parse_string_from_blob(fi->field, value)) {
      my_error(ER_INCORRECT_TYPE, MYF(0), std::to_string(arg_idx).c_str(),
               func_name);
      return true;
    }
    *field_name = std::string(fi->table_name) + "." + fi->field_name;
    return false;
  }

  return true;
}

Item_func_semantic_filter::Item_func_semantic_filter(THD * /* thd */,
                                                           const POS &pos,
                                                           PT_item_list *a)
    : Item_int_func(pos, a) {}

bool Item_func_semantic_filter::resolve_type(THD *thd) {
  if (args[1]->data_type() != MYSQL_TYPE_BLOB) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    return true;
  }
  if (param_type_is_default(thd, 1, 2, MYSQL_TYPE_BLOB)) return true;
  set_nullable(true);

  return false;
}

// double Item_func_semantic_filter::val_real() {
//   if (args[0]->null_value || args[1]->null_value) {
//     return error_real();
//   }

//   try {
//     std::string prompt;
//     std::string value1;
//     std::string field_name1;
//     if (parse_string_from_item(args, 0, m_value, func_name(), prompt, nullptr) ||
//         parse_string_from_item(args, 1, m_value, func_name(), value1, &field_name1)) {
//       return error_bool();
//     }
//     std::map<std::string, std::string> value_dict;
//     if (!field_name1.empty()) {
//       value_dict[field_name1] = value1;
//     }
//     else {
//       value_dict["value1"] = value1;
//     }
//     if (arg_count == 3) {
//       if (args[2]->null_value) {
//         return error_real();
//       }
//       std::string value2;
//       std::string field_name2;
//       if (parse_string_from_item(args, 2, m_value, func_name(), value2, &field_name2)) {
//         return error_bool();
//       }
//       if (!field_name2.empty()) {
//         value_dict[field_name2] = value2;
//       }
//       else {
//         value_dict["value2"] = value2;
//       }
//     }

//     return compute_result(value_dict, prompt);
//   } catch (...) {
//     handle_std_exception(func_name());
//     return error_real();
//   }

//   return 0.0;
// }

longlong Item_func_semantic_filter::val_int() {
  if (args[0]->null_value || args[1]->null_value) {
    return error_real();
  }

  try {
    std::string prompt;
    std::string value1;
    std::string field_name1;
    if (parse_string_from_item(args, 0, m_value, func_name(), prompt, nullptr) ||
        parse_string_from_item(args, 1, m_value, func_name(), value1, &field_name1)) {
      return error_bool();
    }
    std::map<std::string, std::string> value_dict;
    if (!field_name1.empty()) {
      value_dict[field_name1] = value1;
    }
    else {
      value_dict["value1"] = value1;
    }
    if (arg_count == 3) {
      if (args[2]->null_value) {
        return error_real();
      }
      std::string value2;
      std::string field_name2;
      if (parse_string_from_item(args, 2, m_value, func_name(), value2, &field_name2)) {
        return error_bool();
      }
      if (!field_name2.empty()) {
        value_dict[field_name2] = value2;
      }
      else {
        value_dict["value2"] = value2;
      }
    }

    return compute_result(value_dict, prompt);
  } catch (...) {
    handle_std_exception(func_name());
    return error_real();
  }

  return 0.0;
}

Item_func_semantic_filter_single_col::Item_func_semantic_filter_single_col(THD *thd, const POS &pos,
                                               PT_item_list *a)
    : Item_func_semantic_filter(thd, pos, a) {}

const char *Item_func_semantic_filter_single_col::func_name() const { return "semantic_filter_single_col"; }

enum Item_func::Functype Item_func_semantic_filter_single_col::functype() const {
  return SEMANTIC_FILTER_SINGLE_COL;
}

Item_func_semantic_filter_two_col::Item_func_semantic_filter_two_col(THD *thd, const POS &pos,
                                               PT_item_list *a)
    : Item_func_semantic_filter(thd, pos, a) {}

const char *Item_func_semantic_filter_two_col::func_name() const { return "semantic_filter_two_col"; }

enum Item_func::Functype Item_func_semantic_filter_two_col::functype() const {
  return SEMANTIC_FILTER_TWO_COL;
}

Item_func_semantic_map::Item_func_semantic_map(THD * /* thd */,
                                                           const POS &pos,
                                                           PT_item_list *a)
    : Item_str_func(pos, a) {}

bool Item_func_semantic_map::resolve_type(THD *thd) {
  if (args[1]->data_type() != MYSQL_TYPE_BLOB) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    return true;
  }
  if (param_type_is_default(thd, 1, 2, MYSQL_TYPE_BLOB)) return true;
  set_nullable(true);

  return false;
}

String *Item_func_semantic_map::val_str(String *str) {
  if (args[0]->null_value || args[1]->null_value) {
    return error_str();
  }

  try {
    std::string prompt;
    std::string value1;
    std::string field_name1;
    if (parse_string_from_item(args, 0, m_value, func_name(), prompt, nullptr) ||
        parse_string_from_item(args, 1, m_value, func_name(), value1, &field_name1)) {
      return error_str();
    }
    std::map<std::string, std::string> value_dict;
    if (!field_name1.empty()) {
      value_dict[field_name1] = value1;
    }
    else {
      value_dict["value1"] = value1;
    }
    if (arg_count == 3) {
      if (args[2]->null_value) {
        return error_str();
      }
      std::string value2;
      std::string field_name2;
      if (parse_string_from_item(args, 2, m_value, func_name(), value2, &field_name2)) {
        return error_str();
      }
      if (!field_name2.empty()) {
        value_dict[field_name2] = value2;
      }
      else {
        value_dict["value2"] = value2;
      }
    }

    std::string result = compute_result(value_dict, prompt);
    if (!str) return error_str();  
    const CHARSET_INFO* cs = &my_charset_utf8mb4_bin;
    str->set_charset(cs);
    if (str->copy(result.data(), result.size(), cs)) return error_str(); 
    return str;
  } catch (...) {
    handle_std_exception(func_name());
    return error_str();
  }

}

const char *Item_func_semantic_map::func_name() const { return "semantic_map"; }

enum Item_func::Functype Item_func_semantic_map::functype() const {
  return SEMANTIC_MAP;
}

Item_func_semantic_extract::Item_func_semantic_extract(THD *thd, const POS &pos,
                                               PT_item_list *a)
    : Item_func_semantic_map(thd, pos, a) {}

const char *Item_func_semantic_extract::func_name() const { return "semantic_extract"; }

enum Item_func::Functype Item_func_semantic_extract::functype() const {
  return SEMANTIC_EXTRACT;
}



#ifdef WITH_SEMANTICDB
bool Item_func_semantic_filter_single_col::compute_result(std::map<std::string, std::string> &value_dict, std::string &prompt) {
  std::string context="";
  context += (prompt + "\n");
  for (const auto& pair : value_dict) {
    context += (pair.first + ": " + pair.second + "\n");
  }
  bool result;
  if (semantic_filter_openai(context, &result)) {
    return error_real();
  }
  return result;
}

bool Item_func_semantic_filter_two_col::compute_result(std::map<std::string, std::string> &value_dict, std::string &prompt) {
  std::string context="";
  context += (prompt + "\n");
  for (const auto& pair : value_dict) {
    context += (pair.first + ": " + pair.second + "\n");
  }
  bool result;
  if (semantic_filter_openai(context, &result)) {
    return error_real();
  }
  return result;
}

std::string Item_func_semantic_map::compute_result(std::map<std::string, std::string> &value_dict, std::string &prompt) {
  std::string context="";
  context += (prompt + "\n");
  for (const auto& pair : value_dict) {
    context += (pair.first + ": " + pair.second + "\n");
  }
  std::string result;
  if (semantic_map_openai(context, &result)) {
    return "";
  }
  return result;
}

std::string Item_func_semantic_extract::compute_result(std::map<std::string, std::string> &value_dict, std::string &prompt) {
  std::string context="";
  context += (prompt + "\n");
  for (const auto& pair : value_dict) {
    context += (pair.first + ": " + pair.second + "\n");
  }
  std::string result;
  if (semantic_extract_openai(context, &result)) {
    return "";
  }
  return result;
}

#else

// dummy implementation when not compiled with semanticdb

bool Item_func_semantic_filter_single_col::compute_result(std::map<std::string, std::string> &value_dict [[maybe_unused]],
                                               std::string &prompt [[maybe_unused]]) {
  SEMANTICDB_DISABLED_ERR;
}

bool Item_func_semantic_filter_two_col::compute_result(std::map<std::string, std::string> &value_dict [[maybe_unused]],
                                               std::string &prompt [[maybe_unused]]) {
  SEMANTICDB_DISABLED_ERR;
}

std::string Item_func_semantic_map::compute_result(std::map<std::string, std::string> &value_dict [[maybe_unused]],
                                               std::string &prompt [[maybe_unused]]) {
  SEMANTICDB_DISABLED_ERR;
}

std::string Item_func_semantic_extract::compute_result(std::map<std::string, std::string> &value_dict [[maybe_unused]],
                                               std::string &prompt [[maybe_unused]]) {
  SEMANTICDB_DISABLED_ERR;
}
#endif