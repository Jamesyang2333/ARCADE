/*
   Copyright (c) 2025, Jingyi Yang
 */

   #pragma once

   #include "lex_string.h"
   #include "sql-common/json_dom.h"
   #include "sql_const.h"
   
   #ifdef WITH_NEXT_SPATIALDB
   constexpr bool NEXT_SPATIALDB_ENABLED = true;
   #else
   constexpr bool NEXT_SPATIALDB_ENABLED = false;
   #endif
   
   class Field;
   
   enum class NEXT_SPATIAL_INDEX_TYPE { NONE, NO_GLOBAL_INDEX, GLOBAL_INDEX};

   std::string ToString(NEXT_SPATIAL_INDEX_TYPE v);
   
   class NEXT_spatial_index_config {
    public:
     NEXT_spatial_index_config() {}
   
     NEXT_spatial_index_config(NEXT_SPATIAL_INDEX_TYPE type)
         : m_type(type) {}

     NEXT_SPATIAL_INDEX_TYPE type() const { return m_type; }
   
    private:
    NEXT_SPATIAL_INDEX_TYPE m_type = NEXT_SPATIAL_INDEX_TYPE::NONE;
   };
   
   /**
       return true on error
   */
   bool parse_next_spatial_index_type(LEX_CSTRING str, NEXT_SPATIAL_INDEX_TYPE &val);
   
   std::string_view next_spatial_index_type_to_string(NEXT_SPATIAL_INDEX_TYPE val);

    
   