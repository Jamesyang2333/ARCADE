/*
   Copyright (c) 2025, Jingyi Yang
*/

   #include "sql/next_spatial_base.h"
   #include <cassert>
   #include <map>
   #include <string_view>
   #include <iostream>
   #include <fstream>
   #include "next_spatial_base.h"
   #include "field.h"
   #include "item.h"
   #include "item_func.h"
   #include "sql/error_handler.h"
   #include <iomanip>
   
   static const std::map<std::string_view, NEXT_SPATIAL_INDEX_TYPE>
       next_spatial_index_types{{"nogobal", NEXT_SPATIAL_INDEX_TYPE::NO_GLOBAL_INDEX},
                             {"global", NEXT_SPATIAL_INDEX_TYPE::GLOBAL_INDEX}};
   
   /**
       return true on error
   */
   bool parse_next_spatial_index_type(LEX_CSTRING str, NEXT_SPATIAL_INDEX_TYPE &val) {
     auto str_view = to_string_view(str);
     auto iter = next_spatial_index_types.find(str_view);
     if (iter == next_spatial_index_types.cend()) {
       return true;
     }
     val = iter->second;
     return false;
   }
   
   std::string_view next_spatial_index_type_to_string(NEXT_SPATIAL_INDEX_TYPE val) {
     for (const auto &pair : next_spatial_index_types) {
       if (pair.second == val) {
         return pair.first;
       }
     }
     // this is impossible
     assert(false);
     return "";
   }

   std::string ToString(NEXT_SPATIAL_INDEX_TYPE v)
   {
       switch (v)
       {
           case NEXT_SPATIAL_INDEX_TYPE::NONE:   return "none";
           case NEXT_SPATIAL_INDEX_TYPE::NO_GLOBAL_INDEX:   return "noglobal";
           case NEXT_SPATIAL_INDEX_TYPE::GLOBAL_INDEX: return "global";
           default:      return "[Unknown index_type]";
       }
   }
   
   