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

#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <string>
#include <sstream>
#include <iostream>
#include "sql/semantic_base.h"
#include <cassert>
#include <map>
#include <string_view>
#include "field.h"
#include "item.h"
#include "item_func.h"
#include "sql/error_handler.h"
#include "sql/next_spatial_base.h"

std::string get_openai_api_key();

std::string call_openai_api(const std::string& prompt, const std::string& api_key);

bool parse_string_from_blob(Field *field, std::string &data) {
  const Field_blob *field_blob = down_cast<const Field_blob *>(field);
  const uint32 blob_length = field_blob->get_length();
  const uchar *const blob_data = field_blob->get_blob_data();
  data.assign(reinterpret_cast<const char*>(blob_data), blob_length);
  return false;
}

using json = nlohmann::json;

bool semantic_filter_openai(std::string &context, bool* result) {
  std::string api_key = get_openai_api_key();
  if (api_key.empty()) {
    std::cerr << "Error: OPENAI_API_KEY environment variable is not set.\n";
    return 1;
  }
  std::string prompt = "Answer the following question with only one word: \"true\" or \"false\".\nQuestion: " + context + "\nAnswer:";
  std::string api_result = call_openai_api(prompt, api_key);
  // Convert the result to lowercase for comparison
  std::transform(api_result.begin(), api_result.end(), api_result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
  if (api_result == "true") {
    *result = true;
  } else if (api_result == "false") {
    *result = false;
  } else {
    std::cerr << "Error: " << api_result << "\n";
    return 1;
  }

  return 0;
}

bool semantic_map_openai(std::string &context, std::string* result) {
  std::string api_key = get_openai_api_key();
  if (api_key.empty()) {
    std::cerr << "Error: OPENAI_API_KEY environment variable is not set.\n";
    return 1;
  }
  std::string prompt = "Answer the following question. Provide only the answer directly and concisely.\nQuestion: " + context + "\nAnswer:";
  std::string api_result = call_openai_api(prompt, api_key);
  if (!api_result.empty()) {
    *result = api_result;
  } else {
    std::cerr << "Error: " << api_result << "\n";
    return 1;
  }

  return 0;
}

bool semantic_extract_openai(std::string &context, std::string* result) {
  std::string api_key = get_openai_api_key();
  if (api_key.empty()) {
    std::cerr << "Error: OPENAI_API_KEY environment variable is not set.\n";
    return 1;
  }
  std::string prompt = "Extract the relevant entity/entities according to the given question. Output only the answer in json format, output \"{}\" if no relevant entity found.\nQuestion: " + context + "\nAnswer:";
  std::string api_result = call_openai_api(prompt, api_key);
  if (!api_result.empty()) {
    *result = api_result;
  } else {
    std::cerr << "Error: " << api_result << "\n";
    return 1;
  }

  return 0;
}

static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

std::string call_openai_api(const std::string& prompt, const std::string& api_key) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;

    json payload = {
        {"model", "gpt-4"},
        {"messages", {
            {{"role", "user"}, {"content", prompt}}
        }}
    };

    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    if (curl) {
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, ("Authorization: Bearer " + api_key).c_str());
        headers = curl_slist_append(headers, "Content-Type: application/json");

        curl_easy_setopt(curl, CURLOPT_URL, "https://api.openai.com/v1/chat/completions");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        std::string payload_str = payload.dump();
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload_str.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

        res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << "\n";
        }

        curl_easy_cleanup(curl);
        curl_slist_free_all(headers);
    }
    curl_global_cleanup();

    // Parse response
    auto response_json = json::parse(readBuffer);
    try {
        return response_json["choices"][0]["message"]["content"];
    } catch (...) {
        return "Failed to parse response.";
    }
}

bool semantic_embed_openai(const std::string &text, std::vector<float>* result) {
  std::string api_key = get_openai_api_key();
  if (api_key.empty()) {
    std::cerr << "Error: OPENAI_API_KEY environment variable is not set.\n";
    return true;
  }

  CURL* curl;
  CURLcode res;
  std::string readBuffer;

  json payload = {
      {"model", "text-embedding-3-small"},
      {"input", text}
  };

  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl = curl_easy_init();
  if (curl) {
      struct curl_slist* headers = NULL;
      headers = curl_slist_append(headers, ("Authorization: Bearer " + api_key).c_str());
      headers = curl_slist_append(headers, "Content-Type: application/json");

      curl_easy_setopt(curl, CURLOPT_URL, "https://api.openai.com/v1/embeddings");
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

      std::string payload_str = payload.dump();
      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload_str.c_str());
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

      res = curl_easy_perform(curl);
      if (res != CURLE_OK) {
          std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << "\n";
          curl_easy_cleanup(curl);
          curl_slist_free_all(headers);
          curl_global_cleanup();
          return true;
      }

      curl_easy_cleanup(curl);
      curl_slist_free_all(headers);
  }
  curl_global_cleanup();

  // Parse response
  try {
    auto response_json = json::parse(readBuffer);
    auto embedding = response_json["data"][0]["embedding"];
    result->clear();
    result->reserve(embedding.size());
    for (const auto& val : embedding) {
      result->push_back(val.get<float>());
    }
    return false;
  } catch (...) {
    std::cerr << "Failed to parse embedding response.\n";
    return true;
  }
}

std::string get_openai_api_key() {
    const char* key = std::getenv("OPENAI_API_KEY");
    return key ? std::string(key) : "";
}