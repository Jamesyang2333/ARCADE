/*
   Copyright (c) 2025 James Yang.

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

#include "sql/item_func.h"
#include "sql/item_strfunc.h"
#include "sql/system_variables.h"

/**
  parent class of semantic filter functions
*/
class Item_func_semantic_filter : public Item_int_func {
 public:
  Item_func_semantic_filter(THD *thd, const POS &pos, PT_item_list *a);

  bool resolve_type(THD *thd) override;

  // double val_real() override;

  longlong val_int() override;

 protected:
  /// String used when reading JSON binary values or JSON text values.
  String m_value;

  virtual bool compute_result(std::map<std::string, std::string> &value_dict, std::string &prompt) = 0;
};

/**
  Represents the function SEMANTIC_FILTER_SINGLE_COL()
*/
class Item_func_semantic_filter_single_col final : public Item_func_semantic_filter {
 public:
  Item_func_semantic_filter_single_col(THD *thd, const POS &pos, PT_item_list *a);

  const char *func_name() const override;
  enum Functype functype() const override;

 protected:
  bool compute_result(std::map<std::string, std::string> &value_dict, std::string &prompt) override;
};

/**
  Represents the function SEMANTIC_FILTER_TWO_COL()
*/
class Item_func_semantic_filter_two_col final : public Item_func_semantic_filter {
 public:
  Item_func_semantic_filter_two_col(THD *thd, const POS &pos, PT_item_list *a);

  const char *func_name() const override;
  enum Functype functype() const override;

 protected:
  bool compute_result(std::map<std::string, std::string> &value_dict, std::string &prompt) override;
};

/**
  class of semantic map functions
*/
class Item_func_semantic_map : public Item_str_func {
 public:
  Item_func_semantic_map(THD *thd, const POS &pos, PT_item_list *a);

  bool resolve_type(THD *thd);

  // double val_real() override;

  String *val_str(String *str) override;

  const char *func_name() const override;
  enum Functype functype() const override;

 protected:
  /// String used when reading JSON binary values or JSON text values.
  String m_value;

  virtual std::string compute_result(std::map<std::string, std::string> &value_dict, std::string &prompt);
};

/**
  Represents the function SEMANTIC_EXTRACT()
*/
class Item_func_semantic_extract final : public Item_func_semantic_map {
 public:
  Item_func_semantic_extract(THD *thd, const POS &pos, PT_item_list *a);

  const char *func_name() const override;
  enum Functype functype() const override;

 protected:
  std::string compute_result(std::map<std::string, std::string> &value_dict, std::string &prompt) override;
};

bool parse_string_from_item(Item **args, uint arg_idx, String &str,
                     const char *func_name, std::string &value, std::string *field_name);