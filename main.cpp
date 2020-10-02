
#include <arrow/csv/api.h>
#include <arrow/json/api.h>
#include <arrow/io/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>
#include <istream>
#include <fstream>
#include <string>
#include <string_view>

using arrow::Status;


DEFINE_string(infile, "", "Input file either JSON or CSV");
DEFINE_string(outfile, "foo.parquet", "Output file in Parquet format");


/**
 * 
 *  g++ -Wall -std=c++17 -I/usr/local/include -o main main.cpp -lparquet -larrow -lglog -lgflags
 *  ./main --infile <csv or json file> --outfile <parquet file path> --std 
 * 
 */

static Status write_parquet_file(
    arrow::MemoryPool *pool,
    const std::string &filename,
    const arrow::Table& table) {

    auto properties = parquet::WriterProperties::Builder()
        //.encoding(parquet::Encoding::PLAIN_DICTIONARY)
        .compression(arrow::Compression::ZSTD)
        ->enable_statistics()
        ->enable_dictionary()
        ->build();

    auto ap = parquet::ArrowWriterProperties::Builder()
        .enable_deprecated_int96_timestamps()
        ->disallow_truncated_timestamps()
        ->build();

    std::shared_ptr<parquet::ArrowWriterProperties> arrow_properties =
               parquet::default_arrow_writer_properties();

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open(filename));
    // The last argument to the function call is the size of the RowGroup in
    // the parquet file. Normally you would choose this to be rather large but
    // for the example, we use a small value to have multiple RowGroups.
    PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(table, pool, outfile, 1073741824, properties, ap));
    LOG(INFO) << "* Wrote parquet file: " << filename;
    return Status::OK();
}

static Status parse_csv_arrow(std::string &filename) {
    arrow::MemoryPool *pool = arrow::default_memory_pool();
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    LOG(INFO) << "* Reading CSV file '" << filename << "' into table";
    ARROW_ASSIGN_OR_RAISE(auto input_file, arrow::io::ReadableFile::Open(filename));
    ARROW_ASSIGN_OR_RAISE(auto csv_reader,
      arrow::csv::TableReader::Make(pool,input_file, read_options,
                                    parse_options, convert_options));
    ARROW_ASSIGN_OR_RAISE(auto table, csv_reader->Read());

    LOG(INFO) <<  "* Read table:";
    return write_parquet_file(pool, FLAGS_outfile, *table.get());
}

static Status parse_json_arrow(std::string &filename) {
    arrow::MemoryPool *pool = arrow::default_memory_pool();

   auto read_options = arrow::json::ReadOptions::Defaults();
   auto parse_options = arrow::json::ParseOptions::Defaults();

   LOG(INFO) << "* Reading JSON file '" << filename << "' into table";
   ARROW_ASSIGN_OR_RAISE(auto input_file, arrow::io::ReadableFile::Open(filename));
   ARROW_ASSIGN_OR_RAISE(auto json_reader, 
     arrow::json::TableReader::Make(pool, input_file, read_options,
                                    parse_options));
    ARROW_ASSIGN_OR_RAISE(auto table, json_reader->Read());
    LOG(INFO) <<  "* Read table:";
    return write_parquet_file(pool, FLAGS_outfile, *table.get());
}

// static bool ends_with(const std::string &filename, const std::string &extension) {
//     if (auto ex = filename.find_last_of("."); ex != std::string::npos) {
//             if (filename.substr(ex + 1) == extension)
//                 return true;
//     }
//     return false;
// }

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    std::string filename = FLAGS_infile;
    Status status;

    std::fstream fs;
    fs.open(filename, std::fstream::in);
    char first_char = fs.peek(); 
    if (first_char == '{' || first_char == '[') {
        status = parse_json_arrow(filename);
    }
    else {
        status = parse_csv_arrow(filename);
    }

    // if (ends_with(filename, "json")) {
    //     status = parse_json_arrow(filename);
    // } else if (ends_with(filename, "csv")) {
    //     status = parse_csv_arrow(filename);
    // } else {
    //     LOG(ERROR) << "Unknown file format";
    //     return 1;
    // }
    if (!status.ok()) {
        LOG(ERROR) << status;
        return 1;
    }
    return 0;
}
