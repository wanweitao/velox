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

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <chrono>

#include "velox/benchmarks/scan/ScanQueryBuilder.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/dwio/common/Options.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;

std::atomic<uint64_t> stop = false;
extern std::atomic<uint64_t> read_bytes, total_read_bytes;
extern std::atomic<uint64_t> read_requests, total_read_requests;
extern std::atomic<uint64_t> table_scan_rows, total_table_scan_rows;
extern std::atomic<uint64_t> s3_read_bytes, s3_total_read_bytes;
extern std::atomic<uint64_t> s3_read_requests, s3_total_read_requests;

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

static bool notEmpty(const char* /*flagName*/, const std::string& value) {
  return !value.empty();
}

static bool validateDataFormat(const char* flagname, const std::string& value) {
  if ((value.compare("parquet") == 0) || (value.compare("dwrf") == 0)) {
    return true;
  }
  std::cout
      << fmt::format(
             "Invalid value for --{}: {}. Allowed values are [\"parquet\", \"dwrf\"]",
             flagname,
             value)
      << std::endl;
  return false;
}

static bool validateSelectivity(const char* /*flagName*/, double value) {
  return value == 0.2 || value == 0.4 || value == 0.6 || value == 0.8 ||
      value == 1.0;
}

static bool validateColumns(const char* /*flagName*/, int value) {
  return value == 1 || value == 2 || value == 4 || value == 6 || value == 8;
}

void ensureTaskCompletion(exec::Task* task) {
  // ASSERT_TRUE requires a function with return type void.
  ASSERT_TRUE(waitForTaskCompletion(task));
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
} // namespace

DEFINE_string(
    data_path,
    "",
    "Root path of TPC-H data. Data layout must follow Hive-style partitioning. "
    "Example layout for '-data_path=/data/tpch10'\n"
    "       /data/tpch10/customer\n"
    "       /data/tpch10/lineitem\n"
    "       /data/tpch10/nation\n"
    "       /data/tpch10/orders\n"
    "       /data/tpch10/part\n"
    "       /data/tpch10/partsupp\n"
    "       /data/tpch10/region\n"
    "       /data/tpch10/supplier\n"
    "If the above are directories, they contain the data files for "
    "each table. If they are files, they contain a file system path for each "
    "data file, one per line. This allows running against cloud storage or "
    "HDFS");

DEFINE_bool(
    include_custom_stats,
    false,
    "Include custom statistics along with execution statistics");
DEFINE_bool(include_results, false, "Include results in the output");
DEFINE_int32(num_drivers, 4, "Number of drivers");
DEFINE_string(data_format, "parquet", "Data format");
DEFINE_int32(num_splits_per_file, 1, "Number of splits per file");
DEFINE_int32(
    cache_gb,
    0,
    "GB of process memory for cache and query.. if "
    "non-0, uses mmap to allocator and in-process data cache.");
DEFINE_int32(num_threads, 8, "Threads in CPU threads pool");
DEFINE_int32(num_io_threads, 8, "Threads for speculative IO");

DEFINE_string(ssd_path, "", "Directory for local SSD cache");
DEFINE_int32(ssd_cache_gb, 0, "Size of local SSD cache in GB");
DEFINE_int32(
    ssd_checkpoint_interval_gb,
    8,
    "Checkpoint every n "
    "GB new data in cache");

DEFINE_int32(
    num_adding_split_threads,
    1,
    "Number of threads when adding splits");

DEFINE_bool(
    pretty_print,
    false,
    "Display the metrics in a more human friendly way.");

DEFINE_validator(data_path, &notEmpty);
DEFINE_validator(data_format, &validateDataFormat);

DEFINE_int64(
    max_coalesced_bytes,
    128 << 20,
    "Maximum size of single coalesced IO");

DEFINE_int32(
    max_coalesced_distance_bytes,
    512 << 10,
    "Maximum distance in bytes in which coalesce will combine requests");

DEFINE_int32(
    parquet_prefetch_rowgroups,
    1,
    "Number of next row groups to "
    "prefetch. 1 means prefetch the next row group before decoding "
    "the current one");

DEFINE_int32(split_preload_per_driver, 2, "Prefetch split metadata");
DEFINE_double(selectivity, 1.0, "Selectivity of the scan query");
DEFINE_int32(columns, 1, "Columns of the scan query");

DEFINE_validator(selectivity, &validateSelectivity);
DEFINE_validator(columns, &validateColumns);

void report_request_rate() {
  while (!stop.load(std::memory_order_relaxed)) {
    if (FLAGS_pretty_print) {
      std::fprintf(
          stderr,
          "Read: %.2lf MB/sec, %lu requests/sec\n",
          read_bytes.exchange(0) / 1.0 / (1UL << 20),
          read_requests.exchange(0));
      std::fprintf(
          stderr,
          "S3 Read: %.2lf MB/sec, %lu requests/sec\n",
          s3_read_bytes.exchange(0) / 1.0 / (1UL << 20),
          s3_read_requests.exchange(0));
      std::fprintf(
          stderr,
          "Scan: %.2lf M rows/sec\n",
          table_scan_rows.exchange(0) / 1.0 / (1e6));
    } else {
      std::fprintf(
          stderr,
          "%.2lf %lu %.2lf %lu %.2lf\n",
          read_bytes.exchange(0) / 1.0 / (1UL << 20),
          read_requests.exchange(0),
          s3_read_bytes.exchange(0) / 1.0 / (1UL << 20),
          s3_read_requests.exchange(0),
          table_scan_rows.exchange(0) / 1.0 / (1e6));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}

struct RunStats {
  std::map<std::string, std::string> flags;
  int64_t micros{0};
  int64_t rawInputBytes{0};
  int64_t userNanos{0};
  int64_t systemNanos{0};
  std::string output;

  std::string toString(bool detail) {
    std::stringstream out;
    out << succinctNanos(micros * 1000) << " "
        << succinctBytes(rawInputBytes / (micros / 1000000.0)) << "/s raw, "
        << succinctNanos(userNanos) << " user " << succinctNanos(systemNanos)
        << " system (" << (100 * (userNanos + systemNanos) / (micros * 1000))
        << "%), flags: ";
    for (auto& pair : flags) {
      out << pair.first << "=" << pair.second << " ";
    }
    out << std::endl << "======" << std::endl;
    if (detail) {
      out << std::endl << output << std::endl;
    }
    return out.str();
  }
};

class ScanBenchmark {
 public:
  void initialize() {
    if (FLAGS_cache_gb) {
      memory::MemoryManagerOptions options;
      int64_t memoryBytes = FLAGS_cache_gb * (1LL << 30);
      options.useMmapAllocator = true;
      options.allocatorCapacity = memoryBytes;
      options.useMmapArena = true;
      options.mmapArenaCapacityRatio = 1;
      memory::MemoryManager::testingSetInstance(options);
      std::unique_ptr<cache::SsdCache> ssdCache;
      if (FLAGS_ssd_cache_gb) {
        constexpr int32_t kNumSsdShards = 16;
        cacheExecutor_ =
            std::make_unique<folly::IOThreadPoolExecutor>(kNumSsdShards);
        ssdCache = std::make_unique<cache::SsdCache>(
            FLAGS_ssd_path,
            static_cast<uint64_t>(FLAGS_ssd_cache_gb) << 30,
            kNumSsdShards,
            cacheExecutor_.get(),
            static_cast<uint64_t>(FLAGS_ssd_checkpoint_interval_gb) << 30);
      }

      cache_ = cache::AsyncDataCache::create(
          memory::memoryManager()->allocator(), std::move(ssdCache));
      cache::AsyncDataCache::setInstance(cache_.get());
    } else {
      memory::MemoryManager::testingSetInstance({});
    }
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
    filesystems::registerLocalFileSystem();
    exec::ExchangeSource::registerFactory(
        exec::test::createLocalExchangeSource);
    facebook::velox::serializer::presto::PrestoVectorSerde::
        registerVectorSerde();

    ioExecutor_ =
        std::make_unique<folly::IOThreadPoolExecutor>(FLAGS_num_io_threads);

    drivers_ =
        std::make_unique<folly::CPUThreadPoolExecutor>(FLAGS_num_threads);

    pool_ = memory::memoryManager()->addLeafPool();

    // Add new values into the hive configuration...
    auto configurationValues = std::unordered_map<std::string, std::string>();
    configurationValues[connector::hive::HiveConfig::kMaxCoalescedBytes] =
        std::to_string(FLAGS_max_coalesced_bytes);
    configurationValues
        [connector::hive::HiveConfig::kMaxCoalescedDistanceBytes] =
            std::to_string(FLAGS_max_coalesced_distance_bytes);
    auto properties =
        std::make_shared<const core::MemConfig>(configurationValues);

    // Create hive connector with config...
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(kHiveConnectorId, properties, ioExecutor_.get());
    connector::registerConnector(hiveConnector);

    // Query Configs
    query_configs_["exchange.max_buffer_size"] = "536870912";
    query_configs_[core::QueryConfig::kMaxSplitPreloadPerDriver] =
        std::to_string(FLAGS_split_preload_per_driver);
  }

  void shutdown() {
    if (cache_) {
      cache_->shutdown();
    }
    filesystems::finalizeS3FileSystem();
  }

  auto get_task_memeory_pool() {
    return memory::memoryManager()->addRootPool();
  }

  void add_scan_splits(
      std::shared_ptr<Task> task,
      const core::PlanNodeId& scan_node_id,
      const std::vector<std::string>& files) {
    size_t num_files = files.size();

    // std::fprintf(stderr, "\n\nAdding Splits\n\n");
    // auto start_time = std::chrono::system_clock::now();

    auto ioExecutor = std::make_unique<folly::IOThreadPoolExecutor>(
        FLAGS_num_adding_split_threads);

    for (int j = 0; j < num_files; j++) {
      auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
          files[j], FLAGS_num_splits_per_file, FileFormat::PARQUET);
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

  std::shared_ptr<Task> make_task(
      const std::string& taskId,
      const core::PlanNodePtr& planNode,
      int destination = 0,
      Consumer consumer = nullptr) {
    // auto configCopy = configSettings_;
    auto queryCtx = std::make_shared<core::QueryCtx>(
        drivers_.get(), facebook::velox::core::QueryConfig(query_configs_));
    queryCtx->testingOverrideMemoryPool(get_task_memeory_pool());
    core::PlanFragment planFragment{planNode};
    return Task::create(
        taskId,
        std::move(planFragment),
        destination,
        std::move(queryCtx),
        std::move(consumer));
  }

  std::unique_ptr<TaskCursor> get_cursor(const core::PlanNodePtr& plan) {
    CursorParameters params;
    params.maxDrivers = FLAGS_num_drivers;
    params.planNode = plan;
    params.queryCtx = std::make_shared<core::QueryCtx>(
        drivers_.get(), facebook::velox::core::QueryConfig(query_configs_));
    params.queryCtx->testingOverrideMemoryPool(get_task_memeory_pool());

    return TaskCursor::create(params);
  }

  std::pair<std::shared_ptr<Task>, std::unique_ptr<TaskCursor>>
  scan_get_cursor() {
    ScanQueryBuilder builder(FileFormat::PARQUET);
    builder.initialize(FLAGS_data_path);

    std::vector<std::string> selectedColumns, aggregates;
    switch (FLAGS_columns) {
      case 8:
        // l_linenumber
        selectedColumns.push_back("l_linenumber");
        aggregates.push_back("sum(l_linenumber) as l_linenumber");
        // l_quantity
        selectedColumns.push_back("l_quantity");
        aggregates.push_back("sum(l_quantity) as l_quantity");
      case 6:
        // l_extendedprice
        selectedColumns.push_back("l_extendedprice");
        aggregates.push_back("sum(l_extendedprice) as l_extendedprice");
        // l_discount
        selectedColumns.push_back("l_discount");
        aggregates.push_back("sum(l_discount) as l_discount");
      case 4:
        // l_partkey
        selectedColumns.push_back("l_partkey");
        aggregates.push_back("max(l_partkey) as l_partkey");
        // l_suppkey
        selectedColumns.push_back("l_suppkey");
        aggregates.push_back("max(l_suppkey) as l_suppkey");
      case 2:
        // l_tax
        selectedColumns.push_back("l_tax");
        aggregates.push_back("sum(l_tax) as l_tax");
      case 1:
        // l_shipdate
        selectedColumns.push_back("l_shipdate");
        aggregates.push_back("max(l_shipdate) as l_shipdate");
    }

    const auto selectedRowType =
        builder.getRowType(ScanQueryBuilder::kLineitem, selectedColumns);
    const auto& fileColumnNames =
        builder.getFileColumnNames(ScanQueryBuilder::kLineitem);

    std::vector<std::vector<TypePtr>> raw_input_types;
    for (int i = 0; i < FLAGS_columns; i++) {
      raw_input_types.push_back({selectedRowType->childAt(i)});
    }

    const auto shipDate = "l_shipdate";
    std::string lowerBound;
    if (FLAGS_selectivity <= 0.2) {
      lowerBound = "'1997-06-09'";
    } else if (FLAGS_selectivity <= 0.4) {
      lowerBound = "'1996-02-09'";
    } else if (FLAGS_selectivity <= 0.6) {
      lowerBound = "'1994-10-19'";
    } else if (FLAGS_selectivity <= 0.8) {
      lowerBound = "'1993-07-05'";
    } else {
      lowerBound = "'1992-01-01'";
    }
    auto filter = formatDateFilter(shipDate, selectedRowType, lowerBound, "");

    core::PlanNodeId lineitemPlanNodeId, exchangePlanNodeId;

    auto scan_plan = PlanBuilder(pool_.get())
                         .tableScan(
                             ScanQueryBuilder::kLineitem,
                             selectedRowType,
                             fileColumnNames,
                             {filter})
                         .capturePlanNodeId(lineitemPlanNodeId)
                         .partialAggregation({}, aggregates)
                         .partitionedOutput({}, 1)
                         .planNode();

    auto scan_task = make_task("local://scan-0", scan_plan);

    add_scan_splits(
        scan_task,
        lineitemPlanNodeId,
        builder.getTableFilePaths(ScanQueryBuilder::kLineitem));

    auto agg_plan = PlanBuilder(pool_.get())
                        .exchange(scan_plan->outputType())
                        .capturePlanNodeId(exchangePlanNodeId)
                        .localPartition(std::vector<std::string>{})
                        .finalAggregation({}, aggregates, raw_input_types)
                        .planNode();

    auto cursor = get_cursor(agg_plan);

    cursor->task()->addSplit(
        exchangePlanNodeId,
        exec::Split(std::make_shared<RemoteConnectorSplit>(
            fmt::format("local://scan-0"))));
    cursor->task()->noMoreSplits(exchangePlanNodeId);

    return {std::move(scan_task), std::move(cursor)};
  }

  std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>> run() {
    try {
      auto [scan_task, cursor] = scan_get_cursor();
      auto task = cursor->task();
      // start cursor first to make sure the result
      cursor->start();
      scan_task->start(FLAGS_num_drivers);

      std::vector<RowVectorPtr> result;
      while (cursor->moveNext()) {
        result.push_back(cursor->current());
      }
      if (!waitForTaskCompletion(task.get(), 5'000'000)) {
        // NOTE: there is async memory arbitration might fail the task after all
        // the results have been consumed and before the task finishes. So we
        // might run into the failed task state in some rare case such as
        // exposed by concurrent memory arbitration test.
        if (task->state() != TaskState::kFinished &&
            task->state() != TaskState::kRunning) {
          waitForTaskDriversToFinish(task.get(), 5'000'000);
          std::rethrow_exception(task->error());
        } else {
          VELOX_FAIL(
              "Failed to wait for task to complete after {}, task: {}",
              succinctMicros(5'000'000),
              task->toString());
        }
      }

      ensureTaskCompletion(task.get());
      ensureTaskCompletion(scan_task.get());
      return {std::move(cursor), std::move(result)};
    } catch (const std::exception& e) {
      LOG(ERROR) << "Query terminated with: " << e.what();
      return {nullptr, std::vector<RowVectorPtr>()};
    }
  }

  void runMain(std::ostream& out, RunStats& runStats) {
    auto [cursor, actualResults] = run();
    if (!cursor) {
      LOG(ERROR) << "Query terminated with error. Exiting";
      exit(1);
    }
    auto task = cursor->task();
    ensureTaskCompletion(task.get());
    if (FLAGS_include_results) {
      printResults(actualResults, out);
      out << std::endl;
    }
    const auto stats = task->taskStats();
    int64_t rawInputBytes = 0;
    for (auto& pipeline : stats.pipelineStats) {
      auto& first = pipeline.operatorStats[0];
      if (first.operatorType == "TableScan") {
        rawInputBytes += first.rawInputBytes;
      }
    }
    runStats.rawInputBytes = rawInputBytes;
    out << fmt::format(
               "Execution time: {}",
               succinctMillis(
                   stats.executionEndTimeMs - stats.executionStartTimeMs))
        << std::endl;
    out << fmt::format(
               "Splits total: {}, finished: {}",
               stats.numTotalSplits,
               stats.numFinishedSplits)
        << std::endl;
    // out << printPlanWithStats(
    //            *queryPlan.plan, stats, FLAGS_include_custom_stats)
    // << std::endl;
    double time_seconds =
        (stats.executionEndTimeMs - stats.executionStartTimeMs) / 1000.0;
    if (FLAGS_pretty_print) {
      std::fprintf(
          stderr,
          "Avg read: %.2lf MB/sec, %.2lf requests/sec\n",
          total_read_bytes.load() / time_seconds / (1UL << 20),
          total_read_requests.load() / time_seconds);
      std::fprintf(
          stderr,
          "Avg S3 read: %.2lf MB/sec, %.2lf requests/sec\n",
          s3_total_read_bytes.load() / time_seconds / (1UL << 20),
          s3_total_read_requests.load() / time_seconds);
      std::fprintf(
          stderr,
          "Avg scan: %.2lf M rows/sec\n",
          total_table_scan_rows.load() / time_seconds / (1e6));
    } else {
      std::fprintf(
          stderr,
          "%.2lf %.2lf %.2lf %.2lf %.2lf\n",
          total_read_bytes.load() / time_seconds / (1UL << 20),
          total_read_requests.load() / time_seconds,
          s3_total_read_bytes.load() / time_seconds / (1UL << 20),
          s3_total_read_requests.load() / time_seconds,
          total_table_scan_rows.load() / time_seconds / (1e6));
    }
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> drivers_;
  std::unordered_map<std::string, std::string> query_configs_;

  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> cacheExecutor_;
  std::shared_ptr<memory::MemoryAllocator> allocator_;
  std::shared_ptr<cache::AsyncDataCache> cache_;

  std::shared_ptr<memory::MemoryPool> pool_;

  std::vector<RunStats> runStats_;
};

ScanBenchmark benchmark;

int scanBenchmarkMain() {
  benchmark.initialize();

  // std::thread t(report_request_rate);
  RunStats ignore;
  benchmark.runMain(std::cout, ignore);
  // stop.store(true, std::memory_order_relaxed);
  // t.join();

  benchmark.shutdown();
  return 0;
}
