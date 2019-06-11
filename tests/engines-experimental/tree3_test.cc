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

#include "../../src/engines-experimental/tree3.h"
#include "../../src/libpmemkv.hpp"
#include "../mock_tx_alloc.h"
#include "gtest/gtest.h"
#include <libpmemobj.h>

using namespace pmem::kv;

const std::string PATH = "/dev/shm/pmemkv";
const std::string PATH_CACHED = "/tmp/pmemkv";
const size_t SIZE = ((size_t)(1024 * 1024 * 1104));

static std::unique_ptr<pmemkv_config, decltype(&pmemkv_config_delete)>
getConfig(const std::string &path, size_t size)
{
	int ret = 0;

	pmemkv_config *cfg = pmemkv_config_new();

	if (cfg == nullptr)
		throw std::runtime_error("creating config failed");

	ret += pmemkv_config_put(cfg, "path", path.c_str(), path.size() + 1);
	ret += pmemkv_config_put(cfg, "size", &size, sizeof(size));

	if (ret != 0)
		throw std::runtime_error("putting value to config failed");

	return std::unique_ptr<pmemkv_config, decltype(&pmemkv_config_delete)>(
		cfg, &pmemkv_config_delete);
}

class TreeEmptyTest : public testing::Test {
public:
	TreeEmptyTest()
	{
		std::remove(PATH.c_str());
	}
};

class TreeTest : public testing::Test {
public:
public:
	db *kv;

	TreeTest()
	{
		std::remove(PATH.c_str());
		Start();
	}

	~TreeTest()
	{
		delete kv;
	}
	void Restart()
	{
		delete kv;
		Start();
	}

protected:
	void Start()
	{
		kv = new db("tree3", getConfig(PATH, SIZE).get());
	}
};

// =============================================================================================
// TEST EMPTY TREE
// =============================================================================================

TEST_F(TreeEmptyTest, CreateInstanceTest)
{
	db *kv = new db("tree3", getConfig(PATH, PMEMOBJ_MIN_POOL).get());
	delete kv;
}

struct Context {
	int64_t count;
};

TEST_F(TreeEmptyTest, CreateInstanceWithContextTest)
{
	Context cxt = {42};
	db *kv = new db(&cxt, "tree3", getConfig(PATH, PMEMOBJ_MIN_POOL).get(), nullptr);
	ASSERT_TRUE(((Context *)(kv->engine_context()))->count == 42);
	delete kv;
}

TEST_F(TreeEmptyTest, FailsToCreateInstanceWithInvalidPath)
{
	try {
		new db("tree3",
		       getConfig("/tmp/123/234/345/456/567/678/nope.nope",
				 PMEMOBJ_MIN_POOL)
			       .get());
		FAIL();
	} catch (...) {
		// do nothing, expected to happen
	}
}

TEST_F(TreeEmptyTest, FailsToCreateInstanceWithHugeSize)
{
	try {
		new db("tree3",
		       getConfig(PATH, 9223372036854775807).get()); // 9.22 exabytes
		FAIL();
	} catch (...) {
		// do nothing, expected to happen
	}
}

TEST_F(TreeEmptyTest, FailsToCreateInstanceWithTinySize)
{
	try {
		new db("tree3", getConfig(PATH, PMEMOBJ_MIN_POOL - 1).get()); // too small
		FAIL();
	} catch (...) {
		// do nothing, expected to happen
	}
}

// =============================================================================================
// TEST SINGLE-LEAF TREE
// =============================================================================================

TEST_F(TreeTest, SimpleTest)
{
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(status::NOT_FOUND == kv->exists("key1"));
	std::string value;
	ASSERT_TRUE(kv->get("key1", &value) == status::NOT_FOUND);
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(status::OK == kv->exists("key1"));
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");
	value = "";
	kv->get("key1", [&](const char *v, int vb) { value.append(v, vb); });
	ASSERT_TRUE(value == "value1");
	value = "";
	kv->get("key1", [&](const std::string &v) { value.append(v); });
	ASSERT_TRUE(value == "value1");
}

TEST_F(TreeTest, BinaryKeyTest)
{
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(status::NOT_FOUND == kv->exists("a"));
	ASSERT_TRUE(kv->put("a", "should_not_change") == status::OK)
		<< pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(status::OK == kv->exists("a"));
	std::string key1 = std::string("a\0b", 3);
	ASSERT_TRUE(status::NOT_FOUND == kv->exists(key1));
	ASSERT_TRUE(kv->put(key1, "stuff") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 2);
	ASSERT_TRUE(status::OK == kv->exists("a"));
	ASSERT_TRUE(status::OK == kv->exists(key1));
	std::string value;
	ASSERT_TRUE(kv->get(key1, &value) == status::OK);
	ASSERT_EQ(value, "stuff");
	std::string value2;
	ASSERT_TRUE(kv->get("a", &value2) == status::OK);
	ASSERT_EQ(value2, "should_not_change");
	ASSERT_TRUE(kv->remove(key1) == status::OK);
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(status::OK == kv->exists("a"));
	ASSERT_TRUE(status::NOT_FOUND == kv->exists(key1));
	std::string value3;
	ASSERT_TRUE(kv->get(key1, &value3) == status::NOT_FOUND);
	ASSERT_TRUE(kv->get("a", &value3) == status::OK && value3 == "should_not_change");
}

TEST_F(TreeTest, BinaryValueTest)
{
	std::string value("A\0B\0\0C", 6);
	ASSERT_TRUE(kv->put("key1", value) == status::OK) << pmemobj_errormsg();
	std::string value_out;
	ASSERT_TRUE(kv->get("key1", &value_out) == status::OK &&
		    (value_out.length() == 6) && (value_out == value));
}

TEST_F(TreeTest, EmptyKeyTest)
{
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(kv->put("", "empty") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->put(" ", "single-space") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 2);
	ASSERT_TRUE(kv->put("\t\t", "two-tab") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 3);
	std::string value1;
	std::string value2;
	std::string value3;
	ASSERT_TRUE(status::OK == kv->exists(""));
	ASSERT_TRUE(kv->get("", &value1) == status::OK && value1 == "empty");
	ASSERT_TRUE(status::OK == kv->exists(" "));
	ASSERT_TRUE(kv->get(" ", &value2) == status::OK && value2 == "single-space");
	ASSERT_TRUE(status::OK == kv->exists("\t\t"));
	ASSERT_TRUE(kv->get("\t\t", &value3) == status::OK && value3 == "two-tab");
}

TEST_F(TreeTest, EmptyValueTest)
{
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(kv->put("empty", "") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->put("single-space", " ") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 2);
	ASSERT_TRUE(kv->put("two-tab", "\t\t") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 3);
	std::string value1;
	std::string value2;
	std::string value3;
	ASSERT_TRUE(kv->get("empty", &value1) == status::OK && value1 == "");
	ASSERT_TRUE(kv->get("single-space", &value2) == status::OK && value2 == " ");
	ASSERT_TRUE(kv->get("two-tab", &value3) == status::OK && value3 == "\t\t");
}

TEST_F(TreeTest, GetAppendToExternalValueTest)
{
	ASSERT_TRUE(kv->put("key1", "cool") == status::OK) << pmemobj_errormsg();
	std::string value = "super";
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "supercool");
}

TEST_F(TreeTest, GetHeadlessTest)
{
	ASSERT_TRUE(status::NOT_FOUND == kv->exists("waldo"));
	std::string value;
	ASSERT_TRUE(kv->get("waldo", &value) == status::NOT_FOUND);
}

TEST_F(TreeTest, GetMultipleTest)
{
	ASSERT_TRUE(kv->put("abc", "A1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("def", "B2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("hij", "C3") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("jkl", "D4") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("mno", "E5") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 5);
	ASSERT_TRUE(status::OK == kv->exists("abc"));
	std::string value1;
	ASSERT_TRUE(kv->get("abc", &value1) == status::OK && value1 == "A1");
	ASSERT_TRUE(status::OK == kv->exists("def"));
	std::string value2;
	ASSERT_TRUE(kv->get("def", &value2) == status::OK && value2 == "B2");
	ASSERT_TRUE(status::OK == kv->exists("hij"));
	std::string value3;
	ASSERT_TRUE(kv->get("hij", &value3) == status::OK && value3 == "C3");
	ASSERT_TRUE(status::OK == kv->exists("jkl"));
	std::string value4;
	ASSERT_TRUE(kv->get("jkl", &value4) == status::OK && value4 == "D4");
	ASSERT_TRUE(status::OK == kv->exists("mno"));
	std::string value5;
	ASSERT_TRUE(kv->get("mno", &value5) == status::OK && value5 == "E5");
}

TEST_F(TreeTest, GetMultiple2Test)
{
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key2", "value2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key3", "value3") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->remove("key2") == status::OK);
	ASSERT_TRUE(kv->put("key3", "VALUE3") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 2);
	std::string value1;
	ASSERT_TRUE(kv->get("key1", &value1) == status::OK && value1 == "value1");
	std::string value2;
	ASSERT_TRUE(kv->get("key2", &value2) == status::NOT_FOUND);
	std::string value3;
	ASSERT_TRUE(kv->get("key3", &value3) == status::OK && value3 == "VALUE3");
}

TEST_F(TreeTest, GetNonexistentTest)
{
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(status::NOT_FOUND == kv->exists("waldo"));
	std::string value;
	ASSERT_TRUE(kv->get("waldo", &value) == status::NOT_FOUND);
}

TEST_F(TreeTest, PutTest)
{
	ASSERT_TRUE(kv->count() == 0);

	std::string value;
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");

	std::string new_value;
	ASSERT_TRUE(kv->put("key1", "VALUE1") == status::OK)
		<< pmemobj_errormsg(); // same size
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->get("key1", &new_value) == status::OK && new_value == "VALUE1");

	std::string new_value2;
	ASSERT_TRUE(kv->put("key1", "new_value") == status::OK)
		<< pmemobj_errormsg(); // longer size
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->get("key1", &new_value2) == status::OK &&
		    new_value2 == "new_value");

	std::string new_value3;
	ASSERT_TRUE(kv->put("key1", "?") == status::OK)
		<< pmemobj_errormsg(); // shorter size
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->get("key1", &new_value3) == status::OK && new_value3 == "?");
}

TEST_F(TreeTest, PutKeysOfDifferentSizesTest)
{
	std::string value;
	ASSERT_TRUE(kv->put("123456789ABCDE", "A") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->get("123456789ABCDE", &value) == status::OK && value == "A");

	std::string value2;
	ASSERT_TRUE(kv->put("123456789ABCDEF", "B") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 2);
	ASSERT_TRUE(kv->get("123456789ABCDEF", &value2) == status::OK && value2 == "B");

	std::string value3;
	ASSERT_TRUE(kv->put("12345678ABCDEFG", "C") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 3);
	ASSERT_TRUE(kv->get("12345678ABCDEFG", &value3) == status::OK && value3 == "C");

	std::string value4;
	ASSERT_TRUE(kv->put("123456789", "D") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 4);
	ASSERT_TRUE(kv->get("123456789", &value4) == status::OK && value4 == "D");

	std::string value5;
	ASSERT_TRUE(kv->put("123456789ABCDEFGHI", "E") == status::OK)
		<< pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 5);
	ASSERT_TRUE(kv->get("123456789ABCDEFGHI", &value5) == status::OK &&
		    value5 == "E");
}

TEST_F(TreeTest, PutValuesOfDifferentSizesTest)
{
	std::string value;
	ASSERT_TRUE(kv->put("A", "123456789ABCDE") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->get("A", &value) == status::OK && value == "123456789ABCDE");

	std::string value2;
	ASSERT_TRUE(kv->put("B", "123456789ABCDEF") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 2);
	ASSERT_TRUE(kv->get("B", &value2) == status::OK && value2 == "123456789ABCDEF");

	std::string value3;
	ASSERT_TRUE(kv->put("C", "12345678ABCDEFG") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 3);
	ASSERT_TRUE(kv->get("C", &value3) == status::OK && value3 == "12345678ABCDEFG");

	std::string value4;
	ASSERT_TRUE(kv->put("D", "123456789") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 4);
	ASSERT_TRUE(kv->get("D", &value4) == status::OK && value4 == "123456789");

	std::string value5;
	ASSERT_TRUE(kv->put("E", "123456789ABCDEFGHI") == status::OK)
		<< pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 5);
	ASSERT_TRUE(kv->get("E", &value5) == status::OK &&
		    value5 == "123456789ABCDEFGHI");
}

TEST_F(TreeTest, RemoveAllTest)
{
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(kv->put("tmpkey", "tmpvalue1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->remove("tmpkey") == status::OK);
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(status::NOT_FOUND == kv->exists("tmpkey"));
	std::string value;
	ASSERT_TRUE(kv->get("tmpkey", &value) == status::NOT_FOUND);
}

TEST_F(TreeTest, RemoveAndInsertTest)
{
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(kv->put("tmpkey", "tmpvalue1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->remove("tmpkey") == status::OK);
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(status::NOT_FOUND == kv->exists("tmpkey"));
	std::string value;
	ASSERT_TRUE(kv->get("tmpkey", &value) == status::NOT_FOUND);
	ASSERT_TRUE(kv->put("tmpkey1", "tmpvalue1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(status::OK == kv->exists("tmpkey1"));
	ASSERT_TRUE(kv->get("tmpkey1", &value) == status::OK && value == "tmpvalue1");
	ASSERT_TRUE(kv->remove("tmpkey1") == status::OK);
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(status::NOT_FOUND == kv->exists("tmpkey1"));
	ASSERT_TRUE(kv->get("tmpkey1", &value) == status::NOT_FOUND);
}

TEST_F(TreeTest, RemoveExistingTest)
{
	ASSERT_TRUE(kv->count() == 0);
	ASSERT_TRUE(kv->put("tmpkey1", "tmpvalue1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->put("tmpkey2", "tmpvalue2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 2);
	ASSERT_TRUE(kv->remove("tmpkey1") == status::OK);
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->remove("tmpkey1") == status::NOT_FOUND);
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(status::NOT_FOUND == kv->exists("tmpkey1"));
	std::string value;
	ASSERT_TRUE(kv->get("tmpkey1", &value) == status::NOT_FOUND);
	ASSERT_TRUE(status::OK == kv->exists("tmpkey2"));
	ASSERT_TRUE(kv->get("tmpkey2", &value) == status::OK && value == "tmpvalue2");
}

TEST_F(TreeTest, RemoveHeadlessTest)
{
	ASSERT_TRUE(kv->remove("nada") == status::NOT_FOUND);
}

TEST_F(TreeTest, RemoveNonexistentTest)
{
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->remove("nada") == status::NOT_FOUND);
	ASSERT_TRUE(status::OK == kv->exists("key1"));
}

TEST_F(TreeTest, UsesAllTest)
{
	ASSERT_TRUE(kv->put("记!", "RR") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->put("2", "1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 2);

	std::string result;
	kv->all([&result](const char *k, int kb) {
		result.append("<");
		result.append(std::string(k, kb));
		result.append(">,");
	});
	ASSERT_TRUE(result == "<2>,<记!>,");
}

TEST_F(TreeTest, UsesEachTest)
{
	ASSERT_TRUE(kv->put("RR", "记!") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 1);
	ASSERT_TRUE(kv->put("1", "2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->count() == 2);

	std::string result;
	kv->each([&result](const char *k, int kb, const char *v, int vb) {
		result.append("<");
		result.append(std::string(k, kb));
		result.append(">,<");
		result.append(std::string(v, vb));
		result.append(">|");
	});
	ASSERT_TRUE(result == "<1>,<2>|<RR>,<记!>|");
}

// =============================================================================================
// TEST RECOVERY OF SINGLE-LEAF TREE
// =============================================================================================

TEST_F(TreeTest, GetHeadlessAfterRecoveryTest)
{
	Restart();
	std::string value;
	ASSERT_TRUE(kv->get("waldo", &value) == status::NOT_FOUND);
}

TEST_F(TreeTest, GetMultipleAfterRecoveryTest)
{
	ASSERT_TRUE(kv->put("abc", "A1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("def", "B2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("hij", "C3") == status::OK) << pmemobj_errormsg();
	Restart();
	ASSERT_TRUE(kv->put("jkl", "D4") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("mno", "E5") == status::OK) << pmemobj_errormsg();
	std::string value1;
	ASSERT_TRUE(kv->get("abc", &value1) == status::OK && value1 == "A1");
	std::string value2;
	ASSERT_TRUE(kv->get("def", &value2) == status::OK && value2 == "B2");
	std::string value3;
	ASSERT_TRUE(kv->get("hij", &value3) == status::OK && value3 == "C3");
	std::string value4;
	ASSERT_TRUE(kv->get("jkl", &value4) == status::OK && value4 == "D4");
	std::string value5;
	ASSERT_TRUE(kv->get("mno", &value5) == status::OK && value5 == "E5");
}

TEST_F(TreeTest, GetMultiple2AfterRecoveryTest)
{
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key2", "value2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("key3", "value3") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->remove("key2") == status::OK);
	ASSERT_TRUE(kv->put("key3", "VALUE3") == status::OK) << pmemobj_errormsg();
	Restart();
	std::string value1;
	ASSERT_TRUE(kv->get("key1", &value1) == status::OK && value1 == "value1");
	std::string value2;
	ASSERT_TRUE(kv->get("key2", &value2) == status::NOT_FOUND);
	std::string value3;
	ASSERT_TRUE(kv->get("key3", &value3) == status::OK && value3 == "VALUE3");
}

TEST_F(TreeTest, GetNonexistentAfterRecoveryTest)
{
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	Restart();
	std::string value;
	ASSERT_TRUE(kv->get("waldo", &value) == status::NOT_FOUND);
}

TEST_F(TreeTest, PutAfterRecoveryTest)
{
	std::string value;
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->get("key1", &value) == status::OK && value == "value1");

	std::string new_value;
	ASSERT_TRUE(kv->put("key1", "VALUE1") == status::OK)
		<< pmemobj_errormsg(); // same size
	ASSERT_TRUE(kv->get("key1", &new_value) == status::OK && new_value == "VALUE1");
	Restart();

	std::string new_value2;
	ASSERT_TRUE(kv->put("key1", "new_value") == status::OK)
		<< pmemobj_errormsg(); // longer size
	ASSERT_TRUE(kv->get("key1", &new_value2) == status::OK &&
		    new_value2 == "new_value");

	std::string new_value3;
	ASSERT_TRUE(kv->put("key1", "?") == status::OK)
		<< pmemobj_errormsg(); // shorter size
	ASSERT_TRUE(kv->get("key1", &new_value3) == status::OK && new_value3 == "?");
}

TEST_F(TreeTest, RemoveAllAfterRecoveryTest)
{
	ASSERT_TRUE(kv->put("tmpkey", "tmpvalue1") == status::OK) << pmemobj_errormsg();
	Restart();
	ASSERT_TRUE(kv->remove("tmpkey") == status::OK);
	std::string value;
	ASSERT_TRUE(kv->get("tmpkey", &value) == status::NOT_FOUND);
}

TEST_F(TreeTest, RemoveAndInsertAfterRecoveryTest)
{
	ASSERT_TRUE(kv->put("tmpkey", "tmpvalue1") == status::OK) << pmemobj_errormsg();
	Restart();
	ASSERT_TRUE(kv->remove("tmpkey") == status::OK);
	std::string value;
	ASSERT_TRUE(kv->get("tmpkey", &value) == status::NOT_FOUND);
	ASSERT_TRUE(kv->put("tmpkey1", "tmpvalue1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->get("tmpkey1", &value) == status::OK && value == "tmpvalue1");
	ASSERT_TRUE(kv->remove("tmpkey1") == status::OK);
	ASSERT_TRUE(kv->get("tmpkey1", &value) == status::NOT_FOUND);
}

TEST_F(TreeTest, RemoveExistingAfterRecoveryTest)
{
	ASSERT_TRUE(kv->put("tmpkey1", "tmpvalue1") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->put("tmpkey2", "tmpvalue2") == status::OK) << pmemobj_errormsg();
	ASSERT_TRUE(kv->remove("tmpkey1") == status::OK);
	Restart();
	ASSERT_TRUE(kv->remove("tmpkey1") == status::NOT_FOUND);
	std::string value;
	ASSERT_TRUE(kv->get("tmpkey1", &value) == status::NOT_FOUND);
	ASSERT_TRUE(kv->get("tmpkey2", &value) == status::OK && value == "tmpvalue2");
}

TEST_F(TreeTest, RemoveHeadlessAfterRecoveryTest)
{
	Restart();
	ASSERT_TRUE(kv->remove("nada") == status::NOT_FOUND);
}

TEST_F(TreeTest, RemoveNonexistentAfterRecoveryTest)
{
	ASSERT_TRUE(kv->put("key1", "value1") == status::OK) << pmemobj_errormsg();
	Restart();
	ASSERT_TRUE(kv->remove("nada") == status::NOT_FOUND);
}

// =============================================================================================
// TEST TREE WITH SINGLE INNER NODE
// =============================================================================================

const int SINGLE_INNER_LIMIT = LEAF_KEYS * (INNER_KEYS - 1);

TEST_F(TreeTest, SingleInnerNodeAscendingTest)
{
	for (int i = 10000; i < (10000 + SINGLE_INNER_LIMIT); i++) {
		std::string istr = std::to_string(i);
		ASSERT_TRUE(kv->put(istr, istr) == status::OK) << pmemobj_errormsg();
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == istr);
	}
	for (int i = 10000; i < (10000 + SINGLE_INNER_LIMIT); i++) {
		std::string istr = std::to_string(i);
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == istr);
	}
	ASSERT_TRUE(kv->count() == SINGLE_INNER_LIMIT);
}

TEST_F(TreeTest, SingleInnerNodeAscendingTest2)
{
	for (int i = 0; i < SINGLE_INNER_LIMIT; i++) {
		std::string istr = std::to_string(i);
		ASSERT_TRUE(kv->put(istr, istr) == status::OK) << pmemobj_errormsg();
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == istr);
	}
	for (int i = 0; i < SINGLE_INNER_LIMIT; i++) {
		std::string istr = std::to_string(i);
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == istr);
	}
	ASSERT_TRUE(kv->count() == SINGLE_INNER_LIMIT);
}

TEST_F(TreeTest, SingleInnerNodeDescendingTest)
{
	for (int i = (10000 + SINGLE_INNER_LIMIT); i > 10000; i--) {
		std::string istr = std::to_string(i);
		ASSERT_TRUE(kv->put(istr, istr) == status::OK) << pmemobj_errormsg();
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == istr);
	}
	for (int i = (10000 + SINGLE_INNER_LIMIT); i > 10000; i--) {
		std::string istr = std::to_string(i);
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == istr);
	}
	ASSERT_TRUE(kv->count() == SINGLE_INNER_LIMIT);
}

TEST_F(TreeTest, SingleInnerNodeDescendingTest2)
{
	for (int i = SINGLE_INNER_LIMIT; i > 0; i--) {
		std::string istr = std::to_string(i);
		ASSERT_TRUE(kv->put(istr, istr) == status::OK) << pmemobj_errormsg();
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == istr);
	}
	for (int i = SINGLE_INNER_LIMIT; i > 0; i--) {
		std::string istr = std::to_string(i);
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == istr);
	}
	ASSERT_TRUE(kv->count() == SINGLE_INNER_LIMIT);
}

// =============================================================================================
// TEST RECOVERY OF TREE WITH SINGLE INNER NODE
// =============================================================================================

TEST_F(TreeTest, SingleInnerNodeAscendingAfterRecoveryTest)
{
	for (int i = 10000; i < (10000 + SINGLE_INNER_LIMIT); i++) {
		std::string istr = std::to_string(i);
		ASSERT_TRUE(kv->put(istr, istr) == status::OK) << pmemobj_errormsg();
	}
	Restart();
	for (int i = 10000; i < (10000 + SINGLE_INNER_LIMIT); i++) {
		std::string istr = std::to_string(i);
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == istr);
	}
	ASSERT_TRUE(kv->count() == SINGLE_INNER_LIMIT);
}

TEST_F(TreeTest, SingleInnerNodeAscendingAfterRecoveryTest2)
{
	for (int i = 0; i < SINGLE_INNER_LIMIT; i++) {
		std::string istr = std::to_string(i);
		ASSERT_TRUE(kv->put(istr, istr) == status::OK) << pmemobj_errormsg();
	}
	Restart();
	for (int i = 0; i < SINGLE_INNER_LIMIT; i++) {
		std::string istr = std::to_string(i);
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == istr);
	}
	ASSERT_TRUE(kv->count() == SINGLE_INNER_LIMIT);
}

TEST_F(TreeTest, SingleInnerNodeDescendingAfterRecoveryTest)
{
	for (int i = (10000 + SINGLE_INNER_LIMIT); i > 10000; i--) {
		std::string istr = std::to_string(i);
		ASSERT_TRUE(kv->put(istr, istr) == status::OK) << pmemobj_errormsg();
	}
	Restart();
	for (int i = (10000 + SINGLE_INNER_LIMIT); i > 10000; i--) {
		std::string istr = std::to_string(i);
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == istr);
	}
	ASSERT_TRUE(kv->count() == SINGLE_INNER_LIMIT);
}

TEST_F(TreeTest, SingleInnerNodeDescendingAfterRecoveryTest2)
{
	for (int i = SINGLE_INNER_LIMIT; i > 0; i--) {
		std::string istr = std::to_string(i);
		ASSERT_TRUE(kv->put(istr, istr) == status::OK) << pmemobj_errormsg();
	}
	Restart();
	for (int i = SINGLE_INNER_LIMIT; i > 0; i--) {
		std::string istr = std::to_string(i);
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == istr);
	}
	ASSERT_TRUE(kv->count() == SINGLE_INNER_LIMIT);
}

// =============================================================================================
// TEST LARGE TREE
// =============================================================================================

const int LARGE_LIMIT = 4000000;

TEST_F(TreeTest, LargeAscendingTest)
{
	for (int i = 1; i <= LARGE_LIMIT; i++) {
		std::string istr = std::to_string(i);
		ASSERT_TRUE(kv->put(istr, (istr + "!")) == status::OK)
			<< pmemobj_errormsg();
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == (istr + "!"));
	}
	for (int i = 1; i <= LARGE_LIMIT; i++) {
		std::string istr = std::to_string(i);
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == (istr + "!"));
	}
	ASSERT_TRUE(kv->count() == LARGE_LIMIT);
}

TEST_F(TreeTest, LargeDescendingTest)
{
	for (int i = LARGE_LIMIT; i >= 1; i--) {
		std::string istr = std::to_string(i);
		ASSERT_TRUE(kv->put(istr, ("ABC" + istr)) == status::OK)
			<< pmemobj_errormsg();
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK &&
			    value == ("ABC" + istr));
	}
	for (int i = LARGE_LIMIT; i >= 1; i--) {
		std::string istr = std::to_string(i);
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK &&
			    value == ("ABC" + istr));
	}
	ASSERT_TRUE(kv->count() == LARGE_LIMIT);
}

// =============================================================================================
// TEST RECOVERY OF LARGE TREE
// =============================================================================================

TEST_F(TreeTest, LargeAscendingAfterRecoveryTest)
{
	for (int i = 1; i <= LARGE_LIMIT; i++) {
		std::string istr = std::to_string(i);
		ASSERT_TRUE(kv->put(istr, (istr + "!")) == status::OK)
			<< pmemobj_errormsg();
	}
	Restart();
	for (int i = 1; i <= LARGE_LIMIT; i++) {
		std::string istr = std::to_string(i);
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK && value == (istr + "!"));
	}
	ASSERT_TRUE(kv->count() == LARGE_LIMIT);
}

TEST_F(TreeTest, LargeDescendingAfterRecoveryTest)
{
	for (int i = LARGE_LIMIT; i >= 1; i--) {
		std::string istr = std::to_string(i);
		ASSERT_TRUE(kv->put(istr, ("ABC" + istr)) == status::OK)
			<< pmemobj_errormsg();
	}
	Restart();
	for (int i = LARGE_LIMIT; i >= 1; i--) {
		std::string istr = std::to_string(i);
		std::string value;
		ASSERT_TRUE(kv->get(istr, &value) == status::OK &&
			    value == ("ABC" + istr));
	}
	ASSERT_TRUE(kv->count() == LARGE_LIMIT);
}

// =============================================================================================
// TEST RUNNING OUT OF SPACE
// =============================================================================================

class TreeFullTest : public testing::Test {
public:
	db *kv;

	TreeFullTest()
	{
		std::remove(PATH.c_str());
		Start();
	}

	~TreeFullTest()
	{
		delete kv;
	}

	void Restart()
	{
		delete kv;
		kv = new db("tree3", getConfig(PATH, SIZE).get());
	}

	void Validate()
	{
		for (int i = 1; i <= LARGE_LIMIT; i++) {
			std::string istr = std::to_string(i);
			std::string value;
			ASSERT_TRUE(kv->get(istr, &value) == status::OK &&
				    value == (istr + "!"));
		}

		Restart();

		ASSERT_TRUE(kv->put("1", "!1") == status::OK);
		std::string value;
		ASSERT_TRUE(kv->get("1", &value) == status::OK && value == ("!1"));
		ASSERT_TRUE(kv->put("1", "1!") == status::OK);
		std::string value2;
		ASSERT_TRUE(kv->get("1", &value2) == status::OK && value2 == ("1!"));

		for (int i = 1; i <= LARGE_LIMIT; i++) {
			std::string istr = std::to_string(i);
			std::string value3;
			ASSERT_TRUE(kv->get(istr, &value3) == status::OK &&
				    value3 == (istr + "!"));
		}
	}

private:
	void Start()
	{
		if (access(PATH_CACHED.c_str(), F_OK) == 0) {
			ASSERT_TRUE(std::system(("cp -f " + PATH_CACHED + " " + PATH)
							.c_str()) == 0);
		} else {
			std::cout << "!!! creating cached copy at " << PATH_CACHED
				  << "\n";
			db *kvt = new db("tree3", getConfig(PATH, SIZE).get());
			for (int i = 1; i <= LARGE_LIMIT; i++) {
				std::string istr = std::to_string(i);
				ASSERT_TRUE(kvt->put(istr, (istr + "!")) == status::OK)
					<< pmemobj_errormsg();
			}
			delete kvt;
			ASSERT_TRUE(std::system(("cp -f " + PATH + " " + PATH_CACHED)
							.c_str()) == 0);
		}
		kv = new db("tree3", getConfig(PATH, SIZE).get());
	}
};

const std::string LONGSTR =
	"123456789A123456789A123456789A123456789A123456789A123456789A123456789A";

TEST_F(TreeFullTest, OutOfSpace1Test)
{
	tx_alloc_should_fail = true;
	ASSERT_TRUE(kv->put("100", "?") == status::FAILED);
	tx_alloc_should_fail = false;
	Validate();
}

TEST_F(TreeFullTest, OutOfSpace2aTest)
{
	ASSERT_TRUE(kv->remove("100") == status::OK);
	tx_alloc_should_fail = true;
	ASSERT_TRUE(kv->put("100", LONGSTR) == status::FAILED);
	tx_alloc_should_fail = false;
	ASSERT_TRUE(kv->put("100", "100!") == status::OK) << pmemobj_errormsg();
	Validate();
}

TEST_F(TreeFullTest, OutOfSpace2bTest)
{
	ASSERT_TRUE(kv->remove("100") == status::OK);
	ASSERT_TRUE(kv->put("100", "100!") == status::OK) << pmemobj_errormsg();
	tx_alloc_should_fail = true;
	ASSERT_TRUE(kv->put("100", LONGSTR) == status::FAILED);
	tx_alloc_should_fail = false;
	Validate();
}

TEST_F(TreeFullTest, OutOfSpace3aTest)
{
	tx_alloc_should_fail = true;
	ASSERT_TRUE(kv->put("100", LONGSTR) == status::FAILED);
	tx_alloc_should_fail = false;
	Validate();
}

TEST_F(TreeFullTest, OutOfSpace3bTest)
{
	tx_alloc_should_fail = true;
	for (int i = 0; i <= 99999; i++) {
		ASSERT_TRUE(kv->put("123456", LONGSTR) == status::FAILED);
	}
	tx_alloc_should_fail = false;
	ASSERT_TRUE(kv->remove("4567") == status::OK);
	ASSERT_TRUE(kv->put("4567", "4567!") == status::OK) << pmemobj_errormsg();
	Validate();
}

TEST_F(TreeFullTest, OutOfSpace4aTest)
{
	tx_alloc_should_fail = true;
	ASSERT_TRUE(kv->put(std::to_string(LARGE_LIMIT + 1), "1") == status::FAILED);
	tx_alloc_should_fail = false;
	Validate();
}

TEST_F(TreeFullTest, OutOfSpace4bTest)
{
	tx_alloc_should_fail = true;
	for (int i = 0; i <= 99999; i++) {
		ASSERT_TRUE(kv->put(std::to_string(LARGE_LIMIT + 1), "1") ==
			    status::FAILED);
	}
	tx_alloc_should_fail = false;
	ASSERT_TRUE(kv->remove("98765") == status::OK);
	ASSERT_TRUE(kv->put("98765", "98765!") == status::OK) << pmemobj_errormsg();
	Validate();
}

TEST_F(TreeFullTest, OutOfSpace5aTest)
{
	tx_alloc_should_fail = true;
	ASSERT_TRUE(kv->put(LONGSTR, "1") == status::FAILED);
	ASSERT_TRUE(kv->put(LONGSTR, LONGSTR) == status::FAILED);
	tx_alloc_should_fail = false;
	Validate();
}

TEST_F(TreeFullTest, OutOfSpace5bTest)
{
	tx_alloc_should_fail = true;
	for (int i = 0; i <= 99999; i++) {
		ASSERT_TRUE(kv->put(LONGSTR, "1") == status::FAILED);
		ASSERT_TRUE(kv->put(LONGSTR, LONGSTR) == status::FAILED);
	}
	tx_alloc_should_fail = false;
	ASSERT_TRUE(kv->remove("34567") == status::OK);
	ASSERT_TRUE(kv->put("34567", "34567!") == status::OK) << pmemobj_errormsg();
	Validate();
}

TEST_F(TreeFullTest, OutOfSpace6Test)
{
	tx_alloc_should_fail = true;
	ASSERT_TRUE(kv->put(LONGSTR, "?") == status::FAILED);
	tx_alloc_should_fail = false;
	std::string str;
	ASSERT_TRUE(kv->get(LONGSTR, &str) == status::NOT_FOUND);
	Validate();
}

TEST_F(TreeFullTest, RepeatedRecoveryTest)
{
	for (int i = 1; i <= 100; i++)
		Restart();
	Validate();
}
