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

#include "../../src/libpmemkv.hpp"
#include "lib_acl.hpp"
#include "gtest/gtest.h"
#include <libmemcached/memcached.h>
#include <libpmemobj.h>
#include <libpmemobj/base.h>
using namespace pmem::kv;

// const string ENGINE = "stree";
const std::string ENGINE = "tree3";
const std::string PATH = "/dev/shm/pmemkv";

// const string ENGINE = "vcmap";
// const string ENGINE = "vsmap";
// const string PATH = "/dev/shm";

class CachingTest : public testing::Test {
public:
	CachingTest()
	{
		std::remove(PATH.c_str());
	}
	~CachingTest()
	{
		if (kv)
			delete kv;
	}
	db *kv;
};

TEST_F(CachingTest, PutKeyValue)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->exists("key1") == status::OK);
}

TEST_F(CachingTest, PutUpdateValue)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	std::string value;
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->exists("key1") == status::OK);
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
	ASSERT_TRUE(kv->put("key1", "value11") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->exists("key1") == status::OK);
	value = "";
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value11");
}

TEST_F(CachingTest, PutKeywithinTTL)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	std::string value;
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	sleep(1);
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
	sleep(1);
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->exists("key1") == status::OK);
}

TEST_F(CachingTest, PutKeyExpiredTTL)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	sleep(2);
	ASSERT_TRUE(kv->exists("key1") == status::NOT_FOUND);
}

TEST_F(CachingTest, EmptyKeyTest)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->put("", "empty") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put(" ", "single-space") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("\t\t", "two-tab") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("&*", " ") == status::OK) << pmemobj_errormsg();
	std::string value1, value2, value3, value4;
	ASSERT_TRUE(status::OK == kv->exists(""));
	ASSERT_TRUE(kv->get("", &value1) == status::OK && value1 == "empty");
	ASSERT_TRUE(status::OK == kv->exists(" "));
	ASSERT_TRUE(kv->get(" ", &value2) == status::OK && value2 == "single-space");
	ASSERT_TRUE(status::OK == kv->exists("\t\t"));
	ASSERT_TRUE(kv->get("\t\t", &value3) == status::OK && value3 == "two-tab");
	ASSERT_TRUE(status::OK == kv->exists("&*"));
	ASSERT_TRUE(kv->get("&*", &value4) == status::OK && value4 == " ");
}

TEST_F(CachingTest, EmptyValueTest)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->put("empty", "") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("single-space", " ") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("two-tab", "\t\t") == status::OK) << pmemobj_errormsg();
	std::string value1, value2, value3;
	ASSERT_TRUE(kv->get("empty", &value1) == status::OK && value1 == "");
	ASSERT_TRUE(kv->get("single-space", &value2) == status::OK && value2 == " ");
	ASSERT_TRUE(kv->get("two-tab", &value3) == status::OK && value3 == "\t\t");
}

TEST_F(CachingTest, SimpleMemcached)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":11211,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Memcached\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);

	memcached_server_st *servers = NULL;
	memcached_st *memc;
	memcached_return rc;
	const char *key = "key1";
	const char *value1 = "value1";

	memcached_server_st *memcached_servers_parse(char *server_strings);
	memc = memcached_create(NULL);
	servers = memcached_server_list_append(servers, (char *)"127.0.0.1", 11211, &rc);
	rc = memcached_server_push(memc, servers);
	if (rc == MEMCACHED_SUCCESS)
		rc = memcached_set(memc, key, strlen(key), value1, strlen(value1),
				   (time_t)0, (uint32_t)0);

	std::string value;
	ASSERT_TRUE(kv->get("key1", &value) == status::OK &&
		    value == "value1"); // getting the key from remote
	ASSERT_TRUE(kv->exists("key1") == status::OK);
}

TEST_F(CachingTest, SimpleRedis)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);

	int conn_timeout = 30, rw_timeout = 10;
	acl::string addr("127.0.0.1:6379"), passwd;
	acl::redis_client client(addr.c_str(), conn_timeout, rw_timeout);
	acl::redis cmd(&client);
	cmd.set("key1", "value1");

	std::string value;
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
}

TEST_F(CachingTest, UnknownLocalMemcachedKey)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":11211,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Memcached\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);

	memcached_server_st *servers = NULL;
	memcached_st *memc;
	memcached_return rc;
	const char *key = "key1";
	char *return_value;
	uint32_t flags;
	size_t return_value_length;

	memcached_server_st *memcached_servers_parse(char *server_strings);
	memc = memcached_create(NULL);
	servers = memcached_server_list_append(servers, (char *)"127.0.0.1", 11211, &rc);
	rc = memcached_server_push(memc, servers);
	if (rc == MEMCACHED_SUCCESS) {
		memcached_delete(memc, key, strlen(key), (time_t)0);
		return_value = memcached_get(memc, key, strlen(key), &return_value_length,
					     &flags, &rc);
	}
	ASSERT_TRUE(return_value == NULL); // key is not present in memcached

	std::string val;
	ASSERT_TRUE(kv->get("key1", &val) == status::NOT_FOUND);
}

TEST_F(CachingTest, UnknownLocalRedisKey)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);

	int conn_timeout = 30, rw_timeout = 10;
	acl::string addr("127.0.0.1:6379"), passwd;
	acl::redis_client client(addr.c_str(), conn_timeout, rw_timeout);
	acl::redis cmd(&client);
	cmd.del("key1");
	ASSERT_TRUE(cmd.exists("key1") == 0);

	std::string value;
	ASSERT_TRUE(kv->get("key1", &value) == status::NOT_FOUND);
}

TEST_F(CachingTest, SimpleEachTest)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);

	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key2", "value2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key3", "value3") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key4", "value4") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 4);

	std::string result;
	kv->each(
		[](const char *k, size_t kb, const char *v, size_t vb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,<");
			c->append(std::string(v, vb));
			c->append(">|");
		},
		&result);

	if (ENGINE == "tree3")
		ASSERT_TRUE(
			result ==
			"<key4>,<value4>|<key3>,<value3>|<key2>,<value2>|<key1>,<value1>|");
	else if (ENGINE == "vcmap")
		ASSERT_TRUE(
			result ==
			"<key1>,<value1>|<key4>,<value4>|<key3>,<value3>|<key2>,<value2>|");
	else if (ENGINE == "vsmap")
		ASSERT_TRUE(
			result ==
			"<key1>,<value1>|<key2>,<value2>|<key3>,<value3>|<key4>,<value4>|");
	else
		ASSERT_TRUE(
			result ==
			"<key1>,<value1>|<key2>,<value2>|<key3>,<value3>|<key4>,<value4>|");
}

TEST_F(CachingTest, EachTTLValidExpired)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);

	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key2", "value2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key3", "value3") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key4", "value4") == status::OK) << pmemobj_errormsg();
	sleep(2);
	ASSERT_TRUE(kv->put("key5", "value5") == status::OK) << pmemobj_errormsg();

	std::string result;
	kv->each(
		[](const char *k, size_t kb, const char *v, size_t vb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,<");
			c->append(std::string(v, vb));
			c->append(">|");
		},
		&result);
	ASSERT_TRUE(result == "<key5>,<value5>|");
	ASSERT_TRUE(kv->count() == 1);
}

TEST_F(CachingTest, EachEmptyCache)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);
	std::string result;
	kv->each(
		[](const char *k, size_t kb, const char *v, size_t vb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,<");
			c->append(std::string(v, vb));
			c->append(">|");
		},
		&result);
	ASSERT_TRUE(result == "");
	ASSERT_TRUE(kv->count() == 0);
}

TEST_F(CachingTest, EachZeroTTL)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);

	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key2", "value2") == status::OK) << pmemobj_errormsg();
	sleep(1);
	ASSERT_TRUE(kv->put("key3", "value3") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key4", "value4") == status::OK) << pmemobj_errormsg();
	sleep(1);
	ASSERT_TRUE(kv->count() == 4);

	std::string result;
	kv->each(
		[](const char *k, size_t kb, const char *v, size_t vb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,<");
			c->append(std::string(v, vb));
			c->append(">|");
		},
		&result);

	if (ENGINE == "tree3")
		ASSERT_TRUE(
			result ==
			"<key4>,<value4>|<key3>,<value3>|<key2>,<value2>|<key1>,<value1>|");
	else if (ENGINE == "vcmap")
		ASSERT_TRUE(
			result ==
			"<key1>,<value1>|<key4>,<value4>|<key3>,<value3>|<key2>,<value2>|");
	else if (ENGINE == "vsmap")
		ASSERT_TRUE(
			result ==
			"<key1>,<value1>|<key2>,<value2>|<key3>,<value3>|<key4>,<value4>|");
	else
		ASSERT_TRUE(
			result ==
			"<key1>,<value1>|<key2>,<value2>|<key3>,<value3>|<key4>,<value4>|");
}

TEST_F(CachingTest, SimpleCount)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	sleep(2);
	ASSERT_TRUE(kv->put("key2", "value2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key3", "value3") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 2);
}

TEST_F(CachingTest, SimpleZeroTTLCount)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key2", "value2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key3", "value3") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 3);
	sleep(1);
	ASSERT_TRUE(kv->put("key4", "value4") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key5", "value5") == status::OK) << pmemobj_errormsg();
	sleep(1);
	ASSERT_TRUE(kv->count() == 5);
}

TEST_F(CachingTest, SimpleAll)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key2", "value2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 2);
	sleep(2);
	ASSERT_TRUE(kv->put("key3", "value3") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key4", "value4") == status::OK) << pmemobj_errormsg();

	std::string result;
	kv->all(
		[](const char *k, size_t kb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,");
		},
		&result);

	if (ENGINE == "tree3")
		ASSERT_TRUE(result == "<key4>,<key3>,");
	else if (ENGINE == "vcmap")
		ASSERT_TRUE(result == "<key4>,<key3>,");
	else if (ENGINE == "vsmap")
		ASSERT_TRUE(result == "<key3>,<key4>,");
	else
		ASSERT_TRUE(result == "<key3>,<key4>,");

	ASSERT_TRUE(kv->count() == 2);
}

TEST_F(CachingTest, SimpleZeroTTLAll)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key2", "value2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 2);
	sleep(1);
	ASSERT_TRUE(kv->put("key3", "value3") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key4", "value4") == status::OK) << pmemobj_errormsg();

	std::string result;
	kv->all(
		[](const char *k, size_t kb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,");
		},
		&result);

	if (ENGINE == "tree3")
		ASSERT_TRUE(result == "<key4>,<key3>,<key2>,<key1>,");
	else if (ENGINE == "vcmap")
		ASSERT_TRUE(result == "<key1>,<key4>,<key3>,<key2>,");
	else if (ENGINE == "vsmap")
		ASSERT_TRUE(result == "<key1>,<key2>,<key3>,<key4>,");
	else
		ASSERT_TRUE(result == "<key1>,<key2>,<key3>,<key4>,");
}

TEST_F(CachingTest, SimpleRemovekey)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->exists("key1") == status::OK);
	ASSERT_TRUE(kv->remove("key1") == status::OK);
	ASSERT_TRUE(kv->exists("key1") == status::NOT_FOUND);
	ASSERT_TRUE(kv->remove("key1") == status::NOT_FOUND);
	ASSERT_TRUE(kv->put("key2", "value2") == status::OK) << pmemobj_errormsg();
	sleep(2);
	ASSERT_TRUE(kv->remove("key2") == status::OK);
}

TEST_F(CachingTest, SimpleExistsKey)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(kv->exists("key1") == status::NOT_FOUND);
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	sleep(1);
	ASSERT_TRUE(kv->exists("key1") == status::OK);
	sleep(1);
	// key1 not expired even after 1+1 sec sleep as Exists above updated on local
	// cache
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->exists("key1") == status::OK);
	sleep(2);
	ASSERT_TRUE(kv->exists("key1") == status::NOT_FOUND);
}

TEST_F(CachingTest, Redis_Integration)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);

	std::string value;
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
	ASSERT_TRUE(kv->remove("key1") == status::OK);
	ASSERT_TRUE(kv->exists("key1") == status::NOT_FOUND);

	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	sleep(1);
	ASSERT_TRUE(kv->exists("key1") == status::OK);
	sleep(1);
	ASSERT_TRUE(kv->exists("key1") ==
		    status::OK); // key1 is not expired though the sleep is 1+1 sec
	// as Exists API above updated the timestamp

	sleep(2);
	ASSERT_TRUE(kv->exists("key1") == status::NOT_FOUND);
	ASSERT_TRUE(kv->exists("key2") == status::NOT_FOUND);
	ASSERT_TRUE(kv->exists("key3") == status::NOT_FOUND);
	ASSERT_TRUE(kv->count() == 0);

	// Remote redis connection
	int conn_timeout = 30, rw_timeout = 10;
	acl::string addr("127.0.0.1:6379"), passwd;
	acl::redis_client client(addr.c_str(), conn_timeout, rw_timeout);
	acl::redis cmd(&client);

	cmd.set("key1", "value1");
	cmd.set("key2", "value2");
	ASSERT_TRUE(kv->put("key3", "value3") == status::OK) << pmemobj_errormsg();
	value = "";
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
	ASSERT_TRUE(kv->exists("key1") == status::OK);
	value = "";
	ASSERT_TRUE(kv->get("key2", &value) == status::OK && value == "value2");
	ASSERT_TRUE(kv->exists("key2") == status::OK);
	value = "";
	ASSERT_TRUE(kv->get("key3", &value) == status::OK && value == "value3");
	ASSERT_TRUE(kv->exists("key3") == status::OK);

	std::string result;
	kv->each(
		[](const char *k, size_t kb, const char *v, size_t vb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,<");
			c->append(std::string(v, vb));
			c->append(">|");
		},
		&result);

	if (ENGINE == "tree3")
		ASSERT_TRUE(result == "<key2>,<value2>|<key1>,<value1>|<key3>,<value3>|");
	else if (ENGINE == "vcmap")
		ASSERT_TRUE(result == "<key3>,<value3>|<key2>,<value2>|<key1>,<value1>|");
	else if (ENGINE == "vsmap")
		ASSERT_TRUE(result == "<key1>,<value1>|<key2>,<value2>|<key3>,<value3>|");
	else
		ASSERT_TRUE(result == "<key1>,<value1>|<key2>,<value2>|<key3>,<value3>|");

	sleep(2);
	result = "";
	kv->each(
		[](const char *k, size_t kb, const char *v, size_t vb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,<");
			c->append(std::string(v, vb));
			c->append(">|");
		},
		&result);
	ASSERT_TRUE(result == "");
	ASSERT_TRUE(kv->count() == 0);

	value = "";
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
	value = "";
	ASSERT_TRUE(kv->get("key2", &value) == status::OK && value == "value2");
	value = "";
	ASSERT_TRUE(kv->get("key3", &value) == status::NOT_FOUND);

	result = "";
	kv->all(
		[](const char *k, size_t kb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,");
		},
		&result);

	if (ENGINE == "tree3")
		ASSERT_TRUE(result == "<key2>,<key1>,");
	else if (ENGINE == "vcmap")
		ASSERT_TRUE(result == "<key2>,<key1>,");
	else if (ENGINE == "vsmap")
		ASSERT_TRUE(result == "<key1>,<key2>,");
	else
		ASSERT_TRUE(result == "<key1>,<key2>,");

	sleep(2);
	result = "";
	kv->all(
		[](const char *k, size_t kb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,");
		},
		&result);
	ASSERT_TRUE(result == "");
	ASSERT_TRUE(kv->count() == 0);

	cmd.del("key1");
	cmd.del("key2");
	ASSERT_TRUE(cmd.exists("key1") == 0);
	ASSERT_TRUE(cmd.exists("key2") == 0);
	value = "";
	ASSERT_TRUE(kv->get("key1", &value) == status::NOT_FOUND);
	ASSERT_TRUE(kv->get("key2", &value) == status::NOT_FOUND);

	cmd.set("key1", "value1");
	ASSERT_TRUE(kv->exists("key1") == status::NOT_FOUND);
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
	ASSERT_TRUE(kv->exists("key1") == status::OK);

	cmd.del("key1");
}

TEST_F(CachingTest, Memcached_Integration)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":11211,\"attempts\":5,\"ttl\":1,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Memcached\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);

	std::string value;
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
	ASSERT_TRUE(kv->remove("key1") == status::OK);
	ASSERT_TRUE(kv->exists("key1") == status::NOT_FOUND);

	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	sleep(1);
	ASSERT_TRUE(kv->exists("key1") == status::OK);
	sleep(1);
	ASSERT_TRUE(kv->exists("key1") ==
		    status::OK); // key1 is not expired though the sleep is 1+1 sec
	// as Exists API above updated the timestamp

	sleep(2); // TTL is 1 sec
	ASSERT_TRUE(kv->exists("key1") == status::NOT_FOUND);
	ASSERT_TRUE(kv->exists("key2") == status::NOT_FOUND);
	ASSERT_TRUE(kv->exists("key3") == status::NOT_FOUND);
	ASSERT_TRUE(kv->count() == 0);

	// Remote memcached connection
	memcached_server_st *servers = NULL;
	memcached_st *memc;
	memcached_return rc;
	size_t return_value_length;
	uint32_t flags;
	char *return_value1, *return_value2;
	const int key_length = 4;
	const int value_length = 6;

	memcached_server_st *memcached_servers_parse(char *server_strings);
	memc = memcached_create(NULL);
	servers = memcached_server_list_append(servers, (char *)"127.0.0.1", 11211, &rc);
	rc = memcached_server_push(memc, servers);
	if (rc == MEMCACHED_SUCCESS) {
		rc = memcached_set(memc, "key1", key_length, "value1", value_length,
				   (time_t)0, (uint32_t)0);
	}

	value = "";
	ASSERT_TRUE(kv->get("key1", &value) == status::OK &&
		    value == "value1"); // getting the key from remote
	ASSERT_TRUE(kv->exists("key1") == status::OK);
	memcached_set(memc, "key2", key_length, "value2", value_length, (time_t)0,
		      (uint32_t)0);
	ASSERT_TRUE(kv->put("key3", "value3") == status::OK) << pmemobj_errormsg();
	value = "";
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
	ASSERT_TRUE(kv->exists("key1") == status::OK);
	value = "";
	ASSERT_TRUE(kv->get("key2", &value) == status::OK && value == "value2");
	ASSERT_TRUE(kv->exists("key2") == status::OK);
	value = "";
	ASSERT_TRUE(kv->get("key3", &value) == status::OK && value == "value3");
	ASSERT_TRUE(kv->exists("key3") == status::OK);

	std::string result;
	kv->each(
		[](const char *k, size_t kb, const char *v, size_t vb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,<");
			c->append(std::string(v, vb));
			c->append(">|");
		},
		&result);

	if (ENGINE == "tree3")
		ASSERT_TRUE(result == "<key2>,<value2>|<key3>,<value3>|<key1>,<value1>|");
	else if (ENGINE == "vcmap")
		ASSERT_TRUE(result == "<key3>,<value3>|<key2>,<value2>|<key1>,<value1>|");
	else if (ENGINE == "vsmap")
		ASSERT_TRUE(result == "<key1>,<value1>|<key2>,<value2>|<key3>,<value3>|");
	else
		ASSERT_TRUE(result == "<key1>,<value1>|<key2>,<value2>|<key3>,<value3>|");

	sleep(2);
	result = "";
	kv->each(
		[](const char *k, size_t kb, const char *v, size_t vb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,<");
			c->append(std::string(v, vb));
			c->append(">|");
		},
		&result);
	ASSERT_TRUE(result == "");
	ASSERT_TRUE(kv->count() == 0);

	value = "";
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
	value = "";
	ASSERT_TRUE(kv->get("key2", &value) == status::OK && value == "value2");
	value = "";
	ASSERT_TRUE(kv->get("key3", &value) == status::NOT_FOUND);

	result = "";
	kv->all(
		[](const char *k, size_t kb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,");
		},
		&result);

	if (ENGINE == "tree3")
		ASSERT_TRUE(result == "<key2>,<key1>,");
	else if (ENGINE == "vcmap")
		ASSERT_TRUE(result == "<key2>,<key1>,");
	else if (ENGINE == "vsmap")
		ASSERT_TRUE(result == "<key1>,<key2>,");
	else
		ASSERT_TRUE(result == "<key1>,<key2>,");

	sleep(2);
	result = "";
	kv->all(
		[](const char *k, size_t kb, void *arg) {
			const auto c = ((std::string *)arg);
			c->append("<");
			c->append(std::string(k, kb));
			c->append(">,");
		},
		&result);
	ASSERT_TRUE(result == "");
	ASSERT_TRUE(kv->count() == 0);

	memcached_delete(memc, "key1", key_length, (time_t)0);
	memcached_delete(memc, "key2", key_length, (time_t)0);
	return_value1 = memcached_get(memc, "key1", key_length, &return_value_length,
				      &flags, &rc);
	ASSERT_TRUE(return_value1 == NULL); // key is not present in memcached
	return_value2 = memcached_get(memc, "key2", key_length, &return_value_length,
				      &flags, &rc);
	ASSERT_TRUE(return_value2 == NULL); // key is not present in memcached
	value = "";
	ASSERT_TRUE(kv->get("key1", &value) == status::NOT_FOUND);
	ASSERT_TRUE(kv->get("key2", &value) == status::NOT_FOUND);

	memcached_set(memc, "key1", key_length, "value1", value_length, (time_t)0,
		      (uint32_t)0);
	ASSERT_TRUE(kv->exists("key1") == status::NOT_FOUND);
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
	ASSERT_TRUE(kv->exists("key1") == status::OK);
	memcached_delete(memc, "key1", key_length, (time_t)0);
}

// TEST_F(CachingTest, NegativeTTL) {
//    ASSERT_TRUE(!(kv = new db("caching",
//    "{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":-10,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\",
//    \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" + ENGINE +
//    "\",\"subengine_config\":{\"path\":\"" + PATH + "\"}}"));
//}

TEST_F(CachingTest, LargeTTL)
{
	ASSERT_TRUE(
		kv = new db(
			"caching",
			"{\"host\":\"127.0.0.1\",\"port\":6379,\"attempts\":5,\"ttl\":999999999,\"path\":\"/dev/shm/pmemkv\",\"remote_type\":\"Redis\",\"remote_user\":\"xxx\", \"remote_pwd\":\"yyy\", \"remote_url\":\"...\", \"subengine\":\"" +
				ENGINE + "\",\"subengine_config\":{\"path\":\"" + PATH +
				"\"}}"));
	std::string value;
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	sleep(1);
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
	sleep(1);
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->exists("key1") == status::OK);
}
