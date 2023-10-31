/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE
#include "share/index_usage/ob_index_usage_info_mgr.h"
#include <gtest/gtest.h>

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share::schema;

class TestIndexUsage : public ::testing::Test
{
public:
  TestIndexUsage() {}
  ~TestIndexUsage() {}
  virtual void SetUp() {}
  virtual void TearDown() {}

private:
  DISALLOW_COPY_AND_ASSIGN(TestIndexUsage);

};

TEST_F(TestIndexUsage, sample_test) {
  int64_t map_size = 1000;
  int64_t ratio = 25;
  int64_t sample_count = map_size * ratio / 100;

  for(int64_t i=0; i<map_size; i++) {
    if (sample_count < map_size - i && common::ObRandom::rand(0, map_size - i) > sample_count) {
      continue;
    }
    sample_count--;
  }
  ASSERT_EQ(sample_count, 0);
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}