#pragma once

#include "velox/dwio/common/Options.h"

namespace facebook::velox::exec::test {

struct ScanTableMetadata {
  RowTypePtr type;
  std::vector<std::string> dataFiles;
  std::unordered_map<std::string, std::string> fileColumnNames;
};

class ScanQueryBuilder {
 public:
  explicit ScanQueryBuilder(dwio::common::FileFormat format)
      : format_(format) {}

  /// Read each data file, initialize row types, and determine data paths for
  /// each table.
  /// @param dataPath path to the data files
  void initialize(const std::string& dataPath);

  /// Get the query plan for a given TPC-H query number.
  /// @param queryId TPC-H query number
  // DemoPlan getQueryPlan(int queryId) const;

  /// Get the TPC-H table names present.
  static const std::vector<std::string>& getTableNames();

  const std::vector<std::string>& getTableFilePaths(
      const std::string& tableName) const {
    return tableMetadata_.at(tableName).dataFiles;
  }

  std::shared_ptr<const RowType> getRowType(
      const std::string& tableName,
      const std::vector<std::string>& columnNames) const {
    auto columnSelector = std::make_shared<dwio::common::ColumnSelector>(
        tableMetadata_.at(tableName).type, columnNames);
    return columnSelector->buildSelectedReordered();
  }

  const std::unordered_map<std::string, std::string>& getFileColumnNames(
      const std::string& tableName) const {
    return tableMetadata_.at(tableName).fileColumnNames;
  }

  static constexpr const char* kLineitem = "lineitem";
  static constexpr const char* kCustomer = "customer";
  static constexpr const char* kOrders = "orders";
  static constexpr const char* kNation = "nation";
  static constexpr const char* kRegion = "region";
  static constexpr const char* kPart = "part";
  static constexpr const char* kSupplier = "supplier";
  static constexpr const char* kPartsupp = "partsupp";

 private:
  // Initializes the schema information for 'tableName' from sample file at
  // 'filePath'.
  void readFileSchema(
      const std::string& tableName,
      const std::string& filePath,
      const std::vector<std::string>& columns);

  std::unordered_map<std::string, ScanTableMetadata> tableMetadata_;
  const dwio::common::FileFormat format_;
  static const std::unordered_map<std::string, std::vector<std::string>>
      kTables_;
  static const std::vector<std::string> kTableNames_;

  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();
};

} // namespace facebook::velox::exec::test
