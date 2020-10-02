
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include "csv2.hpp"
#include "simdjson.h"
#include <iostream>
#include <string>
#include <string_view>

using arrow::Status;

/**
 * 
 *  g++ -Wall -std=c++17 -o main main.cpp simdjson.cpp
 *  ./main <csv or json file> 
 * 
 */

// #1 Write out the data as a Parquet file
static Status write_parquet_file(
    arrow::MemoryPool *pool,
    const std::string &filename,
    const arrow::Table& table) {

    //std::shared_ptr<parquet::WriterProperties> properties = parquet::default_writer_properties();
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
    std::cout << "* Wrote parquet file: " << filename << std::endl;
    return Status::OK();
}

static Status parse_csv_arrow(std::string &filename) {
    arrow::MemoryPool *pool = arrow::default_memory_pool();
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    std::cout << "* Reading CSV file '" << filename << "' into table" << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto input_file, arrow::io::ReadableFile::Open(filename));
    ARROW_ASSIGN_OR_RAISE(auto csv_reader,
      arrow::csv::TableReader::Make(pool,input_file, read_options,
                                    parse_options, convert_options));
    ARROW_ASSIGN_OR_RAISE(auto table, csv_reader->Read());

    std::cout << "* Read table:" << std::endl;
    //ARROW_RETURN_NOT_OK(arrow::PrettyPrint(*table, {}, &std::cerr));
    return write_parquet_file(pool, "foo.parquet", *table.get());
}

static void parse_csv_file(std::string &filename) {
    csv2::Reader<csv2::delimiter<','>,
         csv2::quote_character<'"'>,
         csv2::first_row_is_header<true>,
         csv2::trim_policy::trim_whitespace> csv;
               
    if (csv.mmap(filename)) {
        const auto header = csv.header();
        std::cout << "Header: ";
        for (const auto cell : header) {
            std::string value;
            cell.read_raw_value(value);
            std::cout << value << ", ";
        }
        std::cout << std::endl;
        std::cout << "Number of rows: " << csv.rows() << std::endl;
        // for (const auto row: csv) {
        //   for (const auto cell: row) {
        //     // Do something with cell value
        //     // std::string value;
        //     // cell.read_value(value);
        //   }
        // }
    }
}

static void parse_json_file(std::string &filename) {
    simdjson::dom::parser parser;
    simdjson::dom::object object;
    uint64_t count = 0;
    auto parsed = parser.load_many(filename);
    for (auto business : parsed) {
        if (count == 0) {
            std::cout << "Keys: ";
            auto error = business.get(object);
            if (error) { 
                std::cerr << error << std::endl; 
                return; 
            }
            for (auto [key, value] : object) {
                std::cout << key << ", ";
            }
            std::cout << std::endl;
        }
        // auto postal_code = std::string_view(business["postal_code"]);
        // auto state = std::string_view(business["state"]);
        // auto name = std::string_view(business["name"]);
        // std::cout << state << " - " << postal_code << " - " << name << std::endl;
        count++;
    }
    //std::cout << "Found " << count << " businesses" << std::endl;
    std::cout << "Number of rows: " << count << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 2) {
      std::cerr << "Please provide a filename" << std::endl;
      return 1;
    }
    std::string filename(argv[1]);

    if (auto ex = filename.find_last_of("."); ex != std::string::npos) {
        auto file_ext = filename.substr(ex + 1);
        if (file_ext == "json") {
            parse_json_file(filename);
        } else if (file_ext == "csv") {
            //parse_csv_file(filename);
            auto status = parse_csv_arrow(filename);
            if (!status.ok()) {
                std::cerr << status << std::endl;
                return 1;
            }
        } else {
            std::cerr << "Unknown file format" << std::endl;
            return 1;
        }
    }
    return 0;
}
