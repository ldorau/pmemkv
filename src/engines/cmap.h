/*
 * Copyright 2017-2019, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#pragma once

#include "../engine.h"
#include "../polymorphic_string.h"

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#define LIBPMEMOBJ_CPP_USE_TBB_RW_MUTEX 1
#include <libpmemobj++/experimental/concurrent_hash_map.hpp>

namespace std {
template<>
struct hash<pmemkv::polymorphic_string> {
    /* hash multiplier used by fibonacci hashing */
    static const size_t hash_multiplier = 11400714819323198485ULL;

    size_t operator()(const pmemkv::polymorphic_string &str) {
        size_t h = 0;
        for (size_t i = 0; i < str.size(); ++i) {
            h = static_cast<size_t>(str[i]) ^ (h * hash_multiplier);
        }
        return h;
    }
};
}

namespace pmemkv {
namespace cmap {

const std::string ENGINE = "cmap";

class CMap : public EngineBase {
  public:
    CMap(void* context, const std::string& path, size_t size);
    ~CMap();

    CMap(const CMap&) = delete;
    CMap& operator=(const CMap&) = delete;

    std::string Engine() final { return ENGINE; }
    void* EngineContext() { return engine_context; }
    void All(void* context, AllCallback* callback) final;
    void AllAbove(void* context, const std::string& key, AllCallback* callback) final {}
    void AllBelow(void* context, const std::string& key, AllCallback* callback) final {}
    void AllBetween(void* context, const std::string& key1, const std::string& key2, AllCallback* callback) final {}
    int64_t Count() final;
    int64_t CountAbove(const std::string& key) final { return 0; }
    int64_t CountBelow(const std::string& key) final { return 0; }
    int64_t CountBetween(const std::string& key1, const std::string& key2) final { return 0; }
    void Each(void* context, EachCallback* callback) final;
    void EachAbove(void* context, const std::string& key, EachCallback* callback) final {}
    void EachBelow(void* context, const std::string& key, EachCallback* callback) final {}
    void EachBetween(void* context, const std::string& key1, const std::string& key2, EachCallback* callback) final {}
    Status Exists(const std::string& key) final;
    void Get(void* context, const std::string& key, GetCallback* callback) final;
    Status Put(const std::string& key, const std::string& value) final;
    Status Remove(const std::string& key) final;
  private:
    using string_t = pmemkv::polymorphic_string;
    using map_t = pmem::obj::experimental::concurrent_hash_map<string_t, string_t>;

    struct RootData {
        pmem::obj::persistent_ptr<map_t> map_ptr;
    };
    using pool_t = pmem::obj::pool<RootData>;

    void Recover();
    void* engine_context;
    pool_t pmpool;
    map_t* container;
};

} // namespace cmap
} // namespace pmemkv
