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

#ifdef EXPERIMENTAL

#pragma once

#include <vector>
#include <memory>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/pool.hpp>

#include "../libpmemkv.h"

using std::move;
using std::unique_ptr;
using std::vector;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::make_persistent;
using pmem::obj::transaction;
using pmem::obj::delete_persistent;
using pmem::obj::pool;

namespace pmemkv {
namespace tree3 {

const std::string ENGINE = "tree3";                        // engine identifier

#define INNER_KEYS 4                                       // maximum keys for inner nodes
#define INNER_KEYS_MIDPOINT (INNER_KEYS / 2)               // halfway point within the node
#define INNER_KEYS_UPPER ((INNER_KEYS / 2) + 1)            // index where upper half of keys begins
#define LEAF_KEYS 48                                       // maximum keys in tree nodes
#define LEAF_KEYS_MIDPOINT (LEAF_KEYS / 2)                 // halfway point within the node

class KVSlot {
  public:
    uint8_t hash() const { return get_ph(); }
    uint8_t hash_direct(char *p) const { return *((uint8_t *)(p + sizeof(uint32_t) + sizeof(uint32_t))); }
    const char* key() const { return ((char *)(kv.get()) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t)); }
    const char* key_direct(char *p) const { return (p + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t)); }
    const uint32_t keysize() const { return get_ks(); }
    const uint32_t keysize_direct(char *p) const { return *((uint32_t *)(p)); }
    const char* val() const { return ((char *)(kv.get()) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t) + get_ks() + 1); }
    const char* val_direct(char *p) const { return (p + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t) + *((uint32_t *)(p)) + 1); }
    const uint32_t valsize() const { return get_vs(); }
    const uint32_t valsize_direct(char *p) const { return *((uint32_t *)(p + sizeof(uint32_t))); }
    void clear();
    void set(const uint8_t hash, const std::string& key, const std::string& value);
    void set_ph(uint8_t v) {*((uint8_t *)((char *)(kv.get()) + sizeof(uint32_t) + sizeof(uint32_t))) = v;}
    void set_ph_direct(char *p, uint8_t v) {*((uint8_t *)(p + sizeof(uint32_t) + sizeof(uint32_t))) = v;}
    void set_ks(uint32_t v) {*((uint32_t *)(kv.get())) = v;}
    void set_ks_direct(char * p, uint32_t v) {*((uint32_t *)(p)) = v;}
    void set_vs(uint32_t v) {*((uint32_t *)((char *)(kv.get()) + sizeof(uint32_t))) = v;}
    void set_vs_direct(char *p, uint32_t v) {*((uint32_t *)((char *)(p) + sizeof(uint32_t))) = v;}
    uint8_t get_ph() const {return *((uint8_t *)((char *)(kv.get()) + sizeof(uint32_t) + sizeof(uint32_t)));}
    uint8_t get_ph_direct(char *p) const {return *((uint8_t *)((char *)(p) + sizeof(uint32_t) + sizeof(uint32_t)));}
    uint32_t get_ks() const {return *((uint32_t *)(kv.get()));}
    uint32_t get_ks_direct(char *p) const {return *((uint32_t *)(p));}
    uint32_t get_vs() const {return *((uint32_t *)((char *)(kv.get()) + sizeof(uint32_t)));}
    uint32_t get_vs_direct(char *p) const {return *((uint32_t *)((char *)(p) + sizeof(uint32_t)));}
    bool empty();
  private:
    persistent_ptr<char[]> kv;                             // buffer for key & value
};

struct KVLeaf {
    p<KVSlot> slots[LEAF_KEYS];                            // array of slot containers
    persistent_ptr<KVLeaf> next;                           // next leaf in unsorted list
};

struct KVRoot {                                            // persistent root object
    persistent_ptr<KVLeaf> head;                           // head of linked list of leaves
};

struct KVInnerNode;

struct KVNode {                                            // volatile nodes of the tree
    bool is_leaf = false;                                  // indicate inner or leaf node
    KVInnerNode* parent;                                   // parent of this node (null if top)
    virtual ~KVNode() = default;
};

struct KVInnerNode final : KVNode {                        // volatile inner nodes of the tree
    uint8_t keycount;                                      // count of keys in this node
    std::string keys[INNER_KEYS + 1];                      // child keys plus one overflow slot
    unique_ptr<KVNode> children[INNER_KEYS + 2];           // child nodes plus one overflow slot
    void assert_invariants();
};

struct KVLeafNode final : KVNode {                         // volatile leaf nodes of the tree
    uint8_t hashes[LEAF_KEYS];                             // Pearson hashes of keys
    std::string keys[LEAF_KEYS];                           // keys stored in this leaf
    persistent_ptr<KVLeaf> leaf;                           // pointer to persistent leaf
};

struct KVRecoveredLeaf {                                   // temporary wrapper used for recovery
    unique_ptr<KVLeafNode> leafnode;                       // leaf node being recovered
    std::string max_key;                                   // highest sorting key present
};

class Tree : public KVEngine {                             // hybrid B+ tree engine
  public:
    Tree(void* context, const std::string& path, size_t size);
    ~Tree();

    std::string Engine() final { return ENGINE; }
    void* EngineContext() { return engine_context; }
    void All(void* context, KVAllCallback* callback) final;
    void AllAbove(void* context, const std::string& key, KVAllCallback* callback) final {};
    void AllBelow(void* context, const std::string& key, KVAllCallback* callback) final {};
    void AllBetween(void* context, const std::string& key1, const std::string& key2, KVAllCallback* callback) final {};
    int64_t Count() final;
    int64_t CountAbove(const std::string& key) final { return 0; };
    int64_t CountBelow(const std::string& key) final { return 0; };
    int64_t CountBetween(const std::string& key1, const std::string& key2) final { return 0; };
    void Each(void* context, KVEachCallback* callback) final;
    void EachAbove(void* context, const std::string& key, KVEachCallback* callback) final {};
    void EachBelow(void* context, const std::string& key, KVEachCallback* callback) final {};
    void EachBetween(void* context, const std::string& key1, const std::string& key2, KVEachCallback* callback) final {};
    KVStatus Exists(const std::string& key) final;
    void Get(void* context, const std::string& key, KVGetCallback* callback) final;
    KVStatus Put(const std::string& key, const std::string& value) final;
    KVStatus Remove(const std::string& key) final;

    using KVEngine::All;
    using KVEngine::AllAbove;
    using KVEngine::AllBelow;
    using KVEngine::AllBetween;
    using KVEngine::Each;
    using KVEngine::EachAbove;
    using KVEngine::EachBelow;
    using KVEngine::EachBetween;
    using KVEngine::Get;
  protected:
    KVLeafNode* LeafSearch(const std::string& key);
    void LeafFillEmptySlot(KVLeafNode* leafnode, uint8_t hash, const std::string& key, const std::string& value);
    bool LeafFillSlotForKey(KVLeafNode* leafnode, uint8_t hash, const std::string& key, const std::string& value);
    void LeafFillSpecificSlot(KVLeafNode* leafnode, uint8_t hash, const std::string& key, const std::string& value, int slot);
    void LeafSplitFull(KVLeafNode* leafnode, uint8_t hash, const std::string& key, const std::string& value);
    void InnerUpdateAfterSplit(KVNode* node, unique_ptr<KVNode> newnode, std::string* split_key);
    uint8_t PearsonHash(const char* data, size_t size);
    void Recover();
  private:
    Tree(const Tree&);                                     // prevent copying
    void operator=(const Tree&);                           // prevent assigning
    void* engine_context;                                  // context when started
    vector<persistent_ptr<KVLeaf>> leaves_prealloc;        // persisted but unused leaves
    pool<KVRoot> pmpool;                                   // pool for persistent root
    unique_ptr<KVNode> tree_top;                           // pointer to uppermost inner node
};

} // namespace tree3
} // namespace pmemkv

#endif
