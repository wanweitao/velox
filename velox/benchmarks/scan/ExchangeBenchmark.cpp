/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <chrono>

#include <fcntl.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>

#include "velox/benchmarks/scan/ScanQueryBuilder.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/core/QueryConfig.h"
#include "velox/dwio/common/Options.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/PlanNodeIdGenerator.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;

DEFINE_int32(num_workers, 4, "Number of workers");
DEFINE_int32(drivers_per_task, 16, "Max dirvers per task");

namespace {

int64_t toDate(std::string_view stringDate) {
  return DATE()->toDays(stringDate);
}

/// DWRF does not support Date type and Varchar is used.
/// Return the Date filter expression as per data format.
std::string formatDateFilter(
    const std::string& stringDate,
    const RowTypePtr& rowType,
    const std::string& lowerBound,
    const std::string& upperBound) {
  bool isDwrf = rowType->findChild(stringDate)->isVarchar();
  auto suffix = isDwrf ? "" : "::DATE";

  if (!lowerBound.empty() && !upperBound.empty()) {
    return fmt::format(
        "{} between {}{} and {}{}",
        stringDate,
        lowerBound,
        suffix,
        upperBound,
        suffix);
  } else if (!lowerBound.empty()) {
    return fmt::format("{} > {}{}", stringDate, lowerBound, suffix);
  } else if (!upperBound.empty()) {
    return fmt::format("{} < {}{}", stringDate, upperBound, suffix);
  }

  VELOX_FAIL(
      "Date range check expression must have either a lower or an upper bound");
}

std::vector<std::string> mergeColumnNames(
    const std::vector<std::string>& firstColumnVector,
    const std::vector<std::string>& secondColumnVector) {
  std::vector<std::string> mergedColumnVector = std::move(firstColumnVector);
  mergedColumnVector.insert(
      mergedColumnVector.end(),
      secondColumnVector.begin(),
      secondColumnVector.end());
  return mergedColumnVector;
};

} // namespace

void ensureTaskCompletion(exec::Task* task) {
  // ASSERT_TRUE requires a function with return type void.
  ASSERT_TRUE(waitForTaskCompletion(task)) << task->taskId();
}

std::string printRow(std::shared_ptr<RowVector> vector, vector_size_t row_id) {
  std::stringstream out;
  out << "{";
  for (int32_t col_id = 0; col_id < vector->childrenSize(); ++col_id) {
    if (col_id > 0) {
      out << ", ";
    }
    auto col_vector = vector->childAt(col_id);
    std::string str = col_vector ? col_vector->toString(row_id) : "<not set>";
    if (col_vector && col_vector->type()->isDate()) {
      str = DATE()->toString(
          col_vector->asFlatVector<int32_t>()->valueAt(row_id));
    }
    out << str;
  }
  out << "}\n";

  return out.str();
}

void printResults(const std::vector<RowVectorPtr>& results, std::ostream& out) {
  out << "Results:" << std::endl;
  bool printType = true;
  for (const auto& vector : results) {
    // Print RowType only once.
    if (printType) {
      out << vector->type()->asRow().toString() << std::endl;
      printType = false;
    }
    for (vector_size_t i = 0; i < vector->size(); ++i) {
      out << printRow(vector, i);
    }
  }
}

// auto dirvers = std::make_shared<folly::CPUThreadPoolExecutor>(
//     std::thread::hardware_concurrency());

std::vector<std::shared_ptr<folly::CPUThreadPoolExecutor>> drivers;

auto ioExecutor = std::make_unique<folly::IOThreadPoolExecutor>(32);

std::unordered_map<std::string, std::string> query_configs = {
    {"exchange.max_buffer_size", "1048576"}};
// {"max_page_partitioning_buffer_size", "536870912"},
// {"max_output_buffer_size", "536870912"}};

std::shared_ptr<Task> makeTask(
    const std::string& taskId,
    const core::PlanNodePtr& planNode,
    int destination = 0,
    Consumer consumer = nullptr,
    int64_t maxMemory = memory::kMaxMemory) {
  // auto configCopy = configSettings_;
  auto queryCtx = std::make_shared<core::QueryCtx>(
      drivers[destination].get(),
      facebook::velox::core::QueryConfig(query_configs));
  queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
      queryCtx->queryId(), maxMemory, MemoryReclaimer::create()));
  core::PlanFragment planFragment{planNode};
  return Task::create(
      taskId,
      std::move(planFragment),
      destination,
      std::move(queryCtx),
      std::move(consumer));
}

void demo() {
  // Add new values into the hive configuration...
  auto configurationValues = std::unordered_map<std::string, std::string>();
  auto properties =
      std::make_shared<const core::MemConfig>(configurationValues);

  // Create hive connector with config...
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, properties, ioExecutor.get());
  connector::registerConnector(hiveConnector);

  auto pool = memory::memoryManager()->addLeafPool();
  ScanQueryBuilder builder(FileFormat::PARQUET);
  builder.initialize("/scorpio/home/wanweitao/debug_tpch");

  std::vector<std::string> supplier_columns = {"s_suppkey", "s_name"};
  const auto supplier_selected_rowtype =
      builder.getRowType(ScanQueryBuilder::kSupplier, supplier_columns);
  const auto& supplier_file_columns =
      builder.getFileColumnNames(ScanQueryBuilder::kSupplier);
  // auto supplier_cols
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId supplierPlanNodeId;
  auto supplier = PlanBuilder(planNodeIdGenerator, pool.get())
                      .tableScan(
                          ScanQueryBuilder::kSupplier,
                          supplier_selected_rowtype,
                          supplier_file_columns,
                          {})
                      .capturePlanNodeId(supplierPlanNodeId)
                      .partitionedOutput({"s_suppkey"}, 2, supplier_columns)
                      .planNode();

  std::vector<std::string> supplier_files =
      builder.getTableFilePaths(ScanQueryBuilder::kSupplier);

  auto leafTask = makeTask("local://scan-0", supplier, 0);
  auto addSplits = [&](exec::Task* task) {
    for (const auto& file : supplier_files) {
      auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
          file, 1, FileFormat::PARQUET);
      for (const auto& split : splits) {
        task->addSplit(supplierPlanNodeId, exec::Split(split));
      }
    }
    task->noMoreSplits(supplierPlanNodeId);
  };
  addSplits(leafTask.get());
  leafTask->start(1);

  auto op = PlanBuilder().exchange(supplier->outputType()).planNode();

  {
    CursorParameters params;
    params.maxDrivers = 1;
    params.planNode = op;
    params.queryConfigs[core::QueryConfig::kMaxSplitPreloadPerDriver] = "1";
    params.destination = 0;

    bool noMoreSplits = false;
    auto op_addSplits = [&](exec::Task* task) {
      if (!noMoreSplits) {
        task->addSplit(
            "0",
            exec::Split(
                std::make_shared<RemoteConnectorSplit>("local://scan-0")));
        task->noMoreSplits("0");
      }
      noMoreSplits = true;
    };

    auto [cursor, actualResults] = readCursor(params, op_addSplits);
    ensureTaskCompletion(cursor->task().get());
    printResults(actualResults, std::cout);
    std::cout << std::endl;
  }

  {
    CursorParameters params;
    params.maxDrivers = 1;
    params.planNode = op;
    params.queryConfigs[core::QueryConfig::kMaxSplitPreloadPerDriver] = "1";
    params.destination = 1;

    bool noMoreSplits = false;
    auto op_addSplits = [&](exec::Task* task) {
      if (!noMoreSplits) {
        task->addSplit(
            "0",
            exec::Split(
                std::make_shared<RemoteConnectorSplit>("local://scan-0")));
        task->noMoreSplits("0");
      }
      noMoreSplits = true;
    };

    auto [cursor, actualResults] = readCursor(params, op_addSplits);
    ensureTaskCompletion(cursor->task().get());
    printResults(actualResults, std::cout);
    std::cout << std::endl;
  }

  ensureTaskCompletion(leafTask.get());

  filesystems::finalizeS3FileSystem();
}

void add_scan_splits(
    uint32_t worker_id,
    std::shared_ptr<Task> task,
    const core::PlanNodeId& scan_node_id,
    const std::vector<std::string>& files) {
  size_t num_files = files.size();

  auto ioExecutor = std::make_unique<folly::IOThreadPoolExecutor>(16);

  // std::fprintf(stderr, "\n\nAdding Splits\n\n");
  // auto start_time = std::chrono::system_clock::now();

  for (int j = 0; j < num_files; j++) {
    if (j % FLAGS_num_workers != worker_id) {
      continue;
    }
    auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
        files[j], 1, FileFormat::PARQUET);
    for (const auto& split : splits) {
      ioExecutor->add(
          [=] { task->addSplit(scan_node_id, exec::Split(split)); });
    }
  }

  ioExecutor->join();

  task->noMoreSplits(scan_node_id);

  // auto end_time = std::chrono::system_clock::now();
  // std::fprintf(
  //     stderr,
  //     "\n\nNo More Splits\n\ntime: %ld ms\n\n",
  //     std::chrono::duration_cast<std::chrono::milliseconds>(
  //         end_time - start_time)
  //         .count());
}

void q3_demo() {
  // Add new values into the hive configuration...
  auto configurationValues = std::unordered_map<std::string, std::string>();
  auto properties =
      std::make_shared<const core::MemConfig>(configurationValues);

  // Create hive connector with config...
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, properties, ioExecutor.get());
  connector::registerConnector(hiveConnector);

  auto pool = memory::memoryManager()->addLeafPool();
  ScanQueryBuilder builder(FileFormat::PARQUET);
  builder.initialize("/scorpio/home/wanweitao/TPCH/dbgen/SF1000/parquet");

  std::vector<std::string> lineitemColumns = {
      "l_shipdate", "l_orderkey", "l_extendedprice", "l_discount"};
  std::vector<std::string> ordersColumns = {
      "o_orderdate", "o_shippriority", "o_custkey", "o_orderkey"};
  std::vector<std::string> customerColumns = {"c_custkey", "c_mktsegment"};

  const auto lineitemSelectedRowType =
      builder.getRowType(ScanQueryBuilder::kLineitem, lineitemColumns);
  const auto& lineitemFileColumns =
      builder.getFileColumnNames(ScanQueryBuilder::kLineitem);
  const auto ordersSelectedRowType =
      builder.getRowType(ScanQueryBuilder::kOrders, ordersColumns);
  const auto& ordersFileColumns =
      builder.getFileColumnNames(ScanQueryBuilder::kOrders);
  const auto customerSelectedRowType =
      builder.getRowType(ScanQueryBuilder::kCustomer, customerColumns);
  const auto& customerFileColumns =
      builder.getFileColumnNames(ScanQueryBuilder::kCustomer);

  const auto orderDate = "o_orderdate";
  const auto shipDate = "l_shipdate";
  auto orderDateFilter =
      formatDateFilter(orderDate, ordersSelectedRowType, "", "'1995-03-15'");
  auto shipDateFilter =
      formatDateFilter(shipDate, lineitemSelectedRowType, "'1995-03-15'", "");
  auto customerFilter = "c_mktsegment = 'BUILDING'";

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemPlanNodeId, lineitemExchangePlanNodeId;
  core::PlanNodeId ordersPlanNodeId, ordersExchangePlanNodeId;
  core::PlanNodeId customerPlanNodeId, customerExchangePlanNodeId;
  core::PlanNodeId custkeyJoinExchangePlanNodeId,
      orderkeyJoinExchangePlanNodeId;

  std::vector<std::shared_ptr<Task>> tasks;

  // scan 3 tables
  auto customers = PlanBuilder(planNodeIdGenerator, pool.get())
                       .tableScan(
                           ScanQueryBuilder::kCustomer,
                           customerSelectedRowType,
                           customerFileColumns,
                           {customerFilter})
                       .capturePlanNodeId(customerPlanNodeId)
                       .partitionedOutput({"c_custkey"}, FLAGS_num_workers)
                       .planNode();

  for (int i = 0; i < FLAGS_num_workers; i++) {
    auto task = makeTask(fmt::format("local://customers-{}", i), customers, i);
    add_scan_splits(
        i,
        task,
        customerPlanNodeId,
        builder.getTableFilePaths(ScanQueryBuilder::kCustomer));
    tasks.emplace_back(std::move(task));
  }

  auto orders =
      PlanBuilder(planNodeIdGenerator, pool.get())
          .tableScan(
              ScanQueryBuilder::kOrders,
              ordersSelectedRowType,
              ordersFileColumns,
              {orderDateFilter})
          .capturePlanNodeId(ordersPlanNodeId)
          .partitionedOutput(
              {"o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"},
              FLAGS_num_workers)
          .planNode();

  for (int i = 0; i < FLAGS_num_workers; i++) {
    auto task = makeTask(fmt::format("local://orders-{}", i), orders, i);
    add_scan_splits(
        i,
        task,
        ordersPlanNodeId,
        builder.getTableFilePaths(ScanQueryBuilder::kOrders));
    tasks.emplace_back(std::move(task));
  }

  auto lineitem = PlanBuilder(planNodeIdGenerator, pool.get())
                      .tableScan(
                          ScanQueryBuilder::kLineitem,
                          lineitemSelectedRowType,
                          lineitemFileColumns,
                          {shipDateFilter})
                      .capturePlanNodeId(lineitemPlanNodeId)
                      .partitionedOutput(
                          {"l_orderkey", "l_extendedprice", "l_discount"},
                          FLAGS_num_workers)
                      .planNode();

  for (int i = 0; i < FLAGS_num_workers; i++) {
    auto task = makeTask(fmt::format("local://lineitem-{}", i), lineitem, i);
    add_scan_splits(
        i,
        task,
        lineitemPlanNodeId,
        builder.getTableFilePaths(ScanQueryBuilder::kLineitem));
    tasks.emplace_back(std::move(task));
  }

  auto customers_exchange = PlanBuilder(planNodeIdGenerator, pool.get())
                                .exchange(customers->outputType())
                                .capturePlanNodeId(customerExchangePlanNodeId)
                                .localPartition({"c_custkey"})
                                .planNode();

  auto custkeyJoinNode =
      PlanBuilder(planNodeIdGenerator, pool.get())
          .exchange(orders->outputType())
          .capturePlanNodeId(ordersExchangePlanNodeId)
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              customers_exchange,
              "",
              {"o_orderdate", "o_shippriority", "o_orderkey"})
          .partitionedOutput(
              {"o_orderkey", "o_orderdate", "o_shippriority"},
              FLAGS_num_workers)
          .planNode();

  for (int i = 0; i < FLAGS_num_workers; i++) {
    auto task =
        makeTask(fmt::format("local://custkeyjoin-{}", i), custkeyJoinNode, i);
    for (int j = 0; j < FLAGS_num_workers; j++) {
      task->addSplit(
          ordersExchangePlanNodeId,
          exec::Split(std::make_shared<RemoteConnectorSplit>(
              fmt::format("local://orders-{}", j))));

      task->addSplit(
          customerExchangePlanNodeId,
          exec::Split(std::make_shared<RemoteConnectorSplit>(
              fmt::format("local://customers-{}", j))));
    }
    task->noMoreSplits(ordersExchangePlanNodeId);
    task->noMoreSplits(customerExchangePlanNodeId);
    tasks.emplace_back(std::move(task));
  }

  auto custkeyJoinExchangeNode =
      PlanBuilder(planNodeIdGenerator, pool.get())
          .exchange(custkeyJoinNode->outputType())
          .capturePlanNodeId(custkeyJoinExchangePlanNodeId)
          .localPartition({"o_orderkey", "o_orderdate", "o_shippriority"})
          .planNode();

  auto orderkeyJoinNode =
      PlanBuilder(planNodeIdGenerator, pool.get())
          .exchange(lineitem->outputType())
          .capturePlanNodeId(lineitemExchangePlanNodeId)
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              custkeyJoinExchangeNode,
              "",
              {"l_orderkey",
               "l_extendedprice",
               "l_discount",
               "o_orderdate",
               "o_shippriority"})
          .project(
              {"l_orderkey",
               "o_orderdate",
               "o_shippriority",
               "l_extendedprice * (1.0 - l_discount) AS revenue"})
          .partialAggregation(
              {"l_orderkey", "o_orderdate", "o_shippriority"},
              {"sum(revenue) as revenue"})
          .localPartition({"l_orderkey", "o_orderdate", "o_shippriority"})
          .finalAggregation(
              {"l_orderkey", "o_orderdate", "o_shippriority"},
              {"sum(revenue) as revenue"},
              {{DOUBLE()}})
          .topN({"revenue DESC", "o_orderdate"}, 10, true)
          .partitionedOutput({}, 1)
          .planNode();

  for (int i = 0; i < FLAGS_num_workers; i++) {
    auto task = makeTask(
        fmt::format("local://orderkeyjoin-{}", i), orderkeyJoinNode, i);
    for (int j = 0; j < FLAGS_num_workers; j++) {
      task->addSplit(
          custkeyJoinExchangePlanNodeId,
          exec::Split(std::make_shared<RemoteConnectorSplit>(
              fmt::format("local://custkeyjoin-{}", j))));

      task->addSplit(
          lineitemExchangePlanNodeId,
          exec::Split(std::make_shared<RemoteConnectorSplit>(
              fmt::format("local://lineitem-{}", j))));
    }
    task->noMoreSplits(custkeyJoinExchangePlanNodeId);
    task->noMoreSplits(lineitemExchangePlanNodeId);
    tasks.emplace_back(std::move(task));
  }

  // std::cout << orderkeyJoinNode->outputType()->toString() << std::endl;

  auto orderkeyJoinExchangeNode =
      PlanBuilder(planNodeIdGenerator, pool.get())
          .exchange(orderkeyJoinNode->outputType())
          .capturePlanNodeId(orderkeyJoinExchangePlanNodeId)
          .localPartition({})
          .topN({"revenue DESC", "o_orderdate"}, 10, false)
          .planNode();

  for (auto& task : tasks) {
    task->start(FLAGS_drivers_per_task);
  }

  // auto dirver = std::make_shared<folly::CPUThreadPoolExecutor>(32);

  auto query_ctx = std::make_shared<core::QueryCtx>(
      drivers[0].get(), facebook::velox::core::QueryConfig(query_configs));
  query_ctx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
      query_ctx->queryId(), memory::kMaxMemory, MemoryReclaimer::create()));

  CursorParameters params;
  params.maxDrivers = FLAGS_drivers_per_task;
  params.planNode = orderkeyJoinExchangeNode;
  params.queryCtx = query_ctx;
  // params.queryConfigs[core::QueryConfig::kMaxSplitPreloadPerDriver] = "1";

  bool noMoreSplits = false;
  auto op_addSplits = [&](exec::Task* task) {
    if (!noMoreSplits) {
      for (int i = 0; i < FLAGS_num_workers; i++) {
        task->addSplit(
            orderkeyJoinExchangePlanNodeId,
            exec::Split(std::make_shared<RemoteConnectorSplit>(
                fmt::format("local://orderkeyjoin-{}", i))));
      }
      task->noMoreSplits(orderkeyJoinExchangePlanNodeId);
    }
    noMoreSplits = true;
  };

  auto [cursor, actualResults] = readCursor(params, op_addSplits);
  ensureTaskCompletion(cursor->task().get());
  printResults(actualResults, std::cout);

  for (auto& task : tasks) {
    ensureTaskCompletion(task.get());
  }

  filesystems::finalizeS3FileSystem();
}

void initialize() {
  facebook::velox::memory::MemoryManagerOptions options;
  // options.allocatorCapacity = 32 * (1UL << 30); // 32 GB
  // options.useMmapAllocator = true;
  memory::MemoryManager::testingSetInstance(options);
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  filesystems::registerLocalFileSystem();
  exec::ExchangeSource::registerFactory(exec::test::createLocalExchangeSource);
  facebook::velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
}

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv, false};
  initialize();

  // setup dirvers
  for (int i = 0; i < FLAGS_num_workers; i++) {
    drivers.emplace_back(
        std::make_shared<folly::CPUThreadPoolExecutor>(FLAGS_drivers_per_task));
  }

  auto start_time = std::chrono::system_clock::now();

  q3_demo();

  auto end_time = std::chrono::system_clock::now();
  std::fprintf(
      stderr,
      "\n\ntime: %ld ms\n\n",
      std::chrono::duration_cast<std::chrono::milliseconds>(
          end_time - start_time)
          .count());
}