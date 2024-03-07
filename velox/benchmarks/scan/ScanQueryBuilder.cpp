#include "velox/benchmarks/scan/ScanQueryBuilder.h"

#include <filesystem>

#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/tpch/gen/TpchGen.h"

namespace fs = std::filesystem;

namespace facebook::velox::exec::test {

void ScanQueryBuilder::readFileSchema(
    const std::string& tableName,
    const std::string& filePath,
    const std::vector<std::string>& columns) {
  dwio::common::ReaderOptions readerOptions{pool_.get()};
  readerOptions.setFileFormat(format_);
  auto uniqueReadFile =
      filesystems::getFileSystem(filePath, nullptr)->openFileForRead(filePath);
  std::shared_ptr<ReadFile> readFile;
  readFile.reset(uniqueReadFile.release());
  auto input = std::make_unique<dwio::common::BufferedInput>(
      readFile, readerOptions.getMemoryPool());
  std::unique_ptr<dwio::common::Reader> reader =
      dwio::common::getReaderFactory(readerOptions.getFileFormat())
          ->createReader(std::move(input), readerOptions);
  const auto fileType = reader->rowType();
  const auto fileColumnNames = fileType->names();
  // There can be extra columns in the file towards the end.
  VELOX_CHECK_GE(fileColumnNames.size(), columns.size());
  std::unordered_map<std::string, std::string> fileColumnNamesMap(
      columns.size());
  std::transform(
      columns.begin(),
      columns.end(),
      fileColumnNames.begin(),
      std::inserter(fileColumnNamesMap, fileColumnNamesMap.begin()),
      [](std::string a, std::string b) { return std::make_pair(a, b); });
  auto columnNames = columns;
  auto types = fileType->children();
  types.resize(columnNames.size());
  tableMetadata_[tableName].type =
      std::make_shared<RowType>(std::move(columnNames), std::move(types));
  tableMetadata_[tableName].fileColumnNames = std::move(fileColumnNamesMap);
}

void ScanQueryBuilder::initialize(const std::string& dataPath) {
  for (const auto& [tableName, columns] : kTables_) {
    const fs::path tablePath{dataPath + "/" + tableName};
    std::error_code error;
    bool anyFound = false;
    for (auto const& dirEntry : fs::directory_iterator{
             tablePath, std::filesystem::directory_options(), error}) {
      if (!dirEntry.is_regular_file()) {
        continue;
      }
      // Ignore hidden files.
      if (dirEntry.path().filename().c_str()[0] == '.') {
        continue;
      }
      if (tableMetadata_[tableName].dataFiles.empty()) {
        anyFound = true;
        readFileSchema(tableName, dirEntry.path().string(), columns);
      }
      tableMetadata_[tableName].dataFiles.push_back(dirEntry.path());
    }
    if (!anyFound && error) {
      std::ifstream file(tablePath);
      std::string line;
      while (std::getline(file, line)) {
        if (tableMetadata_[tableName].dataFiles.empty()) {
          readFileSchema(tableName, line, columns);
        }
        tableMetadata_[tableName].dataFiles.push_back(line);
      }
    }
  }
}

const std::vector<std::string>& ScanQueryBuilder::getTableNames() {
  return kTableNames_;
}

const std::vector<std::string> ScanQueryBuilder::kTableNames_ = {
    kLineitem,
    kOrders,
    kCustomer,
    kNation,
    kRegion,
    kPart,
    kSupplier,
    kPartsupp};

const std::unordered_map<std::string, std::vector<std::string>>
    ScanQueryBuilder::kTables_ = {
        std::make_pair(
            "lineitem",
            tpch::getTableSchema(tpch::Table::TBL_LINEITEM)->names()),
        std::make_pair(
            "orders",
            tpch::getTableSchema(tpch::Table::TBL_ORDERS)->names()),
        std::make_pair(
            "customer",
            tpch::getTableSchema(tpch::Table::TBL_CUSTOMER)->names()),
        std::make_pair(
            "nation",
            tpch::getTableSchema(tpch::Table::TBL_NATION)->names()),
        std::make_pair(
            "region",
            tpch::getTableSchema(tpch::Table::TBL_REGION)->names()),
        std::make_pair(
            "part",
            tpch::getTableSchema(tpch::Table::TBL_PART)->names()),
        std::make_pair(
            "supplier",
            tpch::getTableSchema(tpch::Table::TBL_SUPPLIER)->names()),
        std::make_pair(
            "partsupp",
            tpch::getTableSchema(tpch::Table::TBL_PARTSUPP)->names())};

} // namespace facebook::velox::exec::test