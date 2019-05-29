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

#include <iostream>
#include <rapidjson/document.h>

#include "libpmemkv.h"

#include "engines/blackhole.h"
#include "engine.h"
#include "engines/vsmap.h"
#include "engines/vcmap.h"
#include "engines/cmap.h"
#ifdef EXPERIMENTAL
#include "engines-experimental/tree3.h"
#include "engines-experimental/stree.h"
#include "engines-experimental/caching.h"
#endif
#include <unordered_map>

typedef struct PmemkvConfigValue PmemkvConfigValue;
typedef std::unordered_map<std::string, PmemkvConfigValue> umap_t;

struct PmemkvConfigValue {
	char *buffer;
	int32_t size;
};

struct PmemkvConfig {
	umap_t umap;
};

PmemkvConfig*
pmemkv_config_new(void)
{
	return new PmemkvConfig;
}

void
pmemkv_config_delete(PmemkvConfig* config)
{
	for (auto element : config->umap)
		delete[] element.second.buffer;

	delete config;
}

int8_t
pmemkv_config_put(PmemkvConfig* config, const char* key,
			const void* value, int32_t value_len)
{
	std::string mkey(key);

	PmemkvConfigValue mvalue;
	mvalue.size = value_len;
	mvalue.buffer = new char [mvalue.size];
	memcpy(mvalue.buffer, value, mvalue.size);

	config->umap[mkey] = mvalue;

	return 0;
}

int32_t
pmemkv_config_get(PmemkvConfig* config, const char* key,
			void* buffer, int32_t buffer_len,
			int32_t *value_len)
{
	std::string mkey(key);

	if (config->umap.find(mkey) == config->umap.end())
		return -1;

	PmemkvConfigValue mvalue = config->umap[mkey];
	int32_t len = 0;

	if (buffer) {
		len = std::min(buffer_len, mvalue.size);
		memcpy(buffer, mvalue.buffer, len);
	}

	if (value_len)
		*value_len = mvalue.size;

	return len;
}

// XXX: remove start failure callback - instead use out parameter for
// kvengine_engine and return status.
PmemkvEngine* kvengine_start(void* context, const char* engine, PmemkvConfig* config,
                                    KVStartFailureCallback* callback) {
    try {
        if (engine == pmemkv::blackhole::ENGINE) {
            return reinterpret_cast<PmemkvEngine*>(new pmemkv::blackhole::Blackhole());
#ifdef EXPERIMENTAL
        } else if (engine == "caching") {
            return reinterpret_cast<PmemkvEngine*>(new caching::CachingEngine(context, config));
#endif
        } else {  // handle traditional engines expecting path & size params
            int32_t length;
            if (pmemkv_config_get(config, "path", NULL, 0, &length))
                    throw std::runtime_error("Config does not include 'path' entry");
            char path[length];
            if (pmemkv_config_get(config, "path", path, length, NULL) != length)
                    throw std::runtime_error("Cannot get the 'path' entry");

            if (pmemkv_config_get(config, "size", NULL, 0, &length))
                    throw std::runtime_error("Config does not include 'size'");
            if (length != sizeof(size_t))
                    throw std::runtime_error("Wrong size of the 'size' entry");
            size_t size;
            if (pmemkv_config_get(config, "size", &size, length, NULL) != length)
                    throw std::runtime_error("Cannot get the 'size' entry");

#ifdef EXPERIMENTAL
            if (engine == pmemkv::tree3::ENGINE) {
                return reinterpret_cast<PmemkvEngine*>(new tree3::Tree(context, path, size));
            } else if (engine == pmemkv::stree::ENGINE) {
                return reinterpret_cast<PmemkvEngine*>(new stree::STree(context, path, size));
            } else if ((engine == pmemkv::vsmap::ENGINE) || (engine == pmemkv::vcmap::ENGINE)) {
#else
	    if ((engine == pmemkv::vsmap::ENGINE) || (engine == pmemkv::vcmap::ENGINE)) {
#endif
		struct stat info;
                if ((stat(path, &info) < 0) || !S_ISDIR(info.st_mode)) {
                    std::cout << "Value of PATH = <" << path << ">\n";
                    throw std::runtime_error("Config path is not an existing directory");
                } else if (engine == pmemkv::vsmap::ENGINE) {
                    return reinterpret_cast<PmemkvEngine*>(new pmemkv::vsmap::VSMap(context, path, size));
                } else if (engine == pmemkv::vcmap::ENGINE) {
                    return reinterpret_cast<PmemkvEngine*>(new pmemkv::vcmap::VCMap(context, path, size));
                }
            } else if (engine == pmemkv::cmap::ENGINE) {
                return reinterpret_cast<PmemkvEngine*>(new pmemkv::cmap::CMap(context, path, size));
            }
        }
        throw std::runtime_error("Unknown engine name");
    } catch (std::exception& e) {
        callback(context, engine, config, e.what());
        return nullptr;
    }
}

void kvengine_stop(PmemkvEngine* kv) {
    // XXX: move dangerous work to stop() method
    delete reinterpret_cast<pmemkv::EngineBase*>(kv);
}

void kvengine_all(PmemkvEngine* kv, void* context, KVAllCallback* c) {
    reinterpret_cast<pmemkv::EngineBase*>(kv)->All(context, c);
}

void kvengine_all_above(PmemkvEngine* kv, void* context, int32_t kb, const char* k, KVAllCallback* c) {
    reinterpret_cast<pmemkv::EngineBase*>(kv)->AllAbove(context, std::string(k, (size_t) kb), c);
}

void kvengine_all_below(PmemkvEngine* kv, void* context, int32_t kb, const char* k, KVAllCallback* c) {
    reinterpret_cast<pmemkv::EngineBase*>(kv)->AllBelow(context, std::string(k, (size_t) kb), c);
}

void kvengine_all_between(PmemkvEngine* kv, void* context, int32_t kb1, const char* k1,
                                     int32_t kb2, const char* k2, KVAllCallback* c) {
    reinterpret_cast<pmemkv::EngineBase*>(kv)->AllBetween(context, std::string(k1, (size_t) kb1), std::string(k2, (size_t) kb2), c);
}

int64_t kvengine_count(PmemkvEngine* kv) {
    return reinterpret_cast<pmemkv::EngineBase*>(kv)->Count();
}

int64_t kvengine_count_above(PmemkvEngine* kv, int32_t kb, const char* k) {
    return reinterpret_cast<pmemkv::EngineBase*>(kv)->CountAbove(std::string(k, (size_t) kb));
}

int64_t kvengine_count_below(PmemkvEngine* kv, int32_t kb, const char* k) {
    return reinterpret_cast<pmemkv::EngineBase*>(kv)->CountBelow(std::string(k, (size_t) kb));
}

int64_t kvengine_count_between(PmemkvEngine* kv, int32_t kb1, const char* k1, int32_t kb2, const char* k2) {
    return reinterpret_cast<pmemkv::EngineBase*>(kv)->CountBetween(std::string(k1, (size_t) kb1), std::string(k2, (size_t) kb2));
}

void kvengine_each(PmemkvEngine* kv, void* context, KVEachCallback* c) {
    reinterpret_cast<pmemkv::EngineBase*>(kv)->Each(context, c);
}

void kvengine_each_above(PmemkvEngine* kv, void* context, int32_t kb, const char* k, KVEachCallback* c) {
    reinterpret_cast<pmemkv::EngineBase*>(kv)->EachAbove(context, std::string(k, (size_t) kb), c);
}

void kvengine_each_below(PmemkvEngine* kv, void* context, int32_t kb, const char* k, KVEachCallback* c) {
    reinterpret_cast<pmemkv::EngineBase*>(kv)->EachBelow(context, std::string(k, (size_t) kb), c);
}

void kvengine_each_between(PmemkvEngine* kv, void* context, int32_t kb1, const char* k1,
                                      int32_t kb2, const char* k2, KVEachCallback* c) {
    reinterpret_cast<pmemkv::EngineBase*>(kv)->EachBetween(context, std::string(k1, (size_t) kb1), std::string(k2, (size_t) kb2), c);
}

int8_t kvengine_exists(PmemkvEngine* kv, int32_t kb, const char* k) {
    return reinterpret_cast<pmemkv::EngineBase*>(kv)->Exists(std::string(k, (size_t) kb));
}

void kvengine_get(PmemkvEngine* kv, void* context, const int32_t kb, const char* k, KVGetCallback* c) {
    reinterpret_cast<pmemkv::EngineBase*>(kv)->Get(context, std::string(k, (size_t) kb), c);
}

struct GetCopyCallbackContext {
    KVStatus result;
    int32_t maxvaluebytes;
    char* value;
};

int8_t kvengine_get_copy(PmemkvEngine* kv, int32_t kb, const char* k, int32_t maxvaluebytes, char* value) {
    GetCopyCallbackContext cxt = {NOT_FOUND, maxvaluebytes, value};
    auto cb = [](void* context, int32_t vb, const char* v) {
        const auto c = ((GetCopyCallbackContext*) context);
        if (vb < c->maxvaluebytes) {
            c->result = OK;
            memcpy(c->value, v, vb);
        } else {
            c->result = FAILED;
        }
    };
    memset(value, 0, maxvaluebytes);
    reinterpret_cast<pmemkv::EngineBase*>(kv)->Get(&cxt, std::string(k, (size_t) kb), cb);
    return cxt.result;
}

int8_t kvengine_put(PmemkvEngine* kv, const int32_t kb, const char* k, const int32_t vb, const char* v) {
    return reinterpret_cast<pmemkv::EngineBase*>(kv)->Put(std::string(k, (size_t) kb), std::string(v, (size_t) vb));
}

int8_t kvengine_remove(PmemkvEngine* kv, const int32_t kb, const char* k) {
    return reinterpret_cast<pmemkv::EngineBase*>(kv)->Remove(std::string(k, (size_t) kb));
}
