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
#include <sys/time.h>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fstream>

#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/dwio/common/Options.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
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
      out << vector->toString(i) << std::endl;
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

// DEFINE_int32(
//     run_query_verbose,
//     -1,
//     "Run a given query and print execution statistics");
DEFINE_int32(
    io_meter_column_pct,
    0,
    "Percentage of lineitem columns to "
    "include in IO meter query. The columns are sorted by name and the n% first "
    "are scanned");

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
DEFINE_int32(num_repeats, 1, "Number of times to run each query");
DEFINE_int32(num_io_threads, 8, "Threads for speculative IO");
DEFINE_string(
    test_flags_file,
    "",
    "Path to a file containing gflafs and "
    "values to try. Produces results for each flag combination "
    "sorted on performance");
DEFINE_bool(
    full_sorted_stats,
    true,
    "Add full stats to the report on  --test_flags_file");

DEFINE_string(ssd_path, "", "Directory for local SSD cache");
DEFINE_int32(ssd_cache_gb, 0, "Size of local SSD cache in GB");
DEFINE_int32(
    ssd_checkpoint_interval_gb,
    8,
    "Checkpoint every n "
    "GB new data in cache");
DEFINE_bool(
    clear_ram_cache,
    false,
    "Clear RAM cache before each query."
    "Flushes in process and OS file system cache (if root on Linux)");
DEFINE_bool(
    clear_ssd_cache,
    false,
    "Clears SSD cache before "
    "each query");

DEFINE_bool(
    warmup_after_clear,
    false,
    "Runs one warmup of the query before "
    "measured run. Use to run warm after clearing caches.");

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
    // report
    // std::fprintf(
    //     stderr,
    //     "Internal shuffle request: %lu requests/sec, %.2lf MB/sec\n",
    //     internal_request_count.load(std::memory_order_relaxed),
    //     internal_request_bytes.load(std::memory_order_relaxed) / 1.0 /
    //         (1UL << 20));
    // std::fprintf(
    //     stderr,
    //     "External shuffle request: %lu requests/sec, %.2lf MB/sec\n",
    //     external_request_count.load(std::memory_order_relaxed),
    //     external_request_bytes.load(std::memory_order_relaxed) / 1.0 /
    //         (1UL << 20));

    // std::fprintf(
    //     stderr,
    //     "Scan: %.2lf M Rows/sec, filter rate: %.2lf\n",
    //     table_scan_rows.load(std::memory_order_relaxed) / 1.0 / 1e6,
    //     table_scan_request_rows ? static_cast<double>(table_scan_rows.load(
    //                                   std::memory_order_relaxed)) /
    //             table_scan_request_rows.load(std::memory_order_relaxed)
    //                             : 0);
    // wait for next report
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

struct ParameterDim {
  std::string flag;
  std::vector<std::string> values;
};

std::shared_ptr<TpchQueryBuilder> queryBuilder;

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

    ioExecutor_ =
        std::make_unique<folly::IOThreadPoolExecutor>(FLAGS_num_io_threads);

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
  }

  void shutdown() {
    if (cache_) {
      cache_->shutdown();
    }
    filesystems::finalizeS3FileSystem();
  }

  std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>> run(
      const TpchPlan& tpchPlan) {
    int32_t repeat = 0;
    try {
      for (;;) {
        CursorParameters params;
        params.maxDrivers = FLAGS_num_drivers;
        params.planNode = tpchPlan.plan;
        params.queryConfigs[core::QueryConfig::kMaxSplitPreloadPerDriver] =
            std::to_string(FLAGS_split_preload_per_driver);
        const int numSplitsPerFile = FLAGS_num_splits_per_file;

        bool noMoreSplits = false;
        auto addSplits = [&](exec::Task* task) {
          if (!noMoreSplits) {
            for (const auto& entry : tpchPlan.dataFiles) {
              for (const auto& path : entry.second) {
                auto const splits =
                    HiveConnectorTestBase::makeHiveConnectorSplits(
                        path, numSplitsPerFile, tpchPlan.dataFileFormat);
                for (const auto& split : splits) {
                  task->addSplit(entry.first, exec::Split(split));
                }
              }
              task->noMoreSplits(entry.first);
            }
          }
          noMoreSplits = true;
        };
        auto result = readCursor(params, addSplits);
        ensureTaskCompletion(result.first->task().get());
        if (++repeat >= FLAGS_num_repeats) {
          return result;
        }
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "Query terminated with: " << e.what();
      return {nullptr, std::vector<RowVectorPtr>()};
    }
  }

  void runMain(std::ostream& out, RunStats& runStats) {
    const auto queryPlan = FLAGS_io_meter_column_pct > 0
        ? queryBuilder->getIoMeterPlan(FLAGS_io_meter_column_pct)
        : queryBuilder->getScanPlan(FLAGS_columns, FLAGS_selectivity);
    auto [cursor, actualResults] = run(queryPlan);
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
    out << printPlanWithStats(
               *queryPlan.plan, stats, FLAGS_include_custom_stats)
        << std::endl;
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

  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> cacheExecutor_;
  std::shared_ptr<memory::MemoryAllocator> allocator_;
  std::shared_ptr<cache::AsyncDataCache> cache_;

  std::vector<RunStats> runStats_;
};

ScanBenchmark benchmark;

BENCHMARK(q1) {
  const auto planContext = queryBuilder->getQueryPlan(1);
  benchmark.run(planContext);
}

int scanBenchmarkMain() {
  benchmark.initialize();
  queryBuilder =
      std::make_shared<TpchQueryBuilder>(toFileFormat(FLAGS_data_format));
  queryBuilder->initialize(FLAGS_data_path);

  std::thread t(report_request_rate);
  RunStats ignore;
  benchmark.runMain(std::cout, ignore);
  stop.store(true, std::memory_order_relaxed);
  t.join();

  benchmark.shutdown();
  queryBuilder.reset();
  return 0;
}
