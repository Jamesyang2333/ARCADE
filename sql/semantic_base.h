/*
   Copyright (c) 2025, James Yang.

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

#include <string>
#include <vector>
#include "lex_string.h"
#include "sql-common/json_dom.h"
#include "sql_const.h"

#ifdef WITH_SEMANTICDB
constexpr bool SEMANTICDB_ENABLED = true;
#else
constexpr bool SEMANTICDB_ENABLED = false;
#endif

class Field;

// Parse field containing blob values into data_view in data
bool parse_string_from_blob(Field *field, std::string &data);

bool semantic_filter_openai(std::string &context, bool* result);

bool semantic_map_openai(std::string &context, std::string* result);

bool semantic_extract_openai(std::string &context, std::string* result);

bool semantic_embed_openai(const std::string &text, std::vector<float>* result);