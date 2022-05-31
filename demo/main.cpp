#include <iostream>

#include <boost/program_options.hpp>
#include <boost/unordered_map.hpp>
#include <rocksdb/slice.h>

#include <database.hpp>
#include <logsettings.hpp>

namespace po = boost::program_options;

int main(int argc, char* argv[]){
  po::options_description desc("Options");
  desc.add_options()
      ("help,h", "Help information")
          ("log_level,ll", po::value<std::string>(), "Logging level")
              ("thread_count,tc", po::value<size_t>(), "Count of threads")
                  ("output,o", po::value<std::string>(), "Path to file for result");

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
  }
  catch (po::error &e) {
    std::cout << e.what() << std::endl;
    std::cout << desc << std::endl;
    return 1;
  }

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  if (!vm.count("log_level")
      || !vm.count("thread_count")
      || !vm.count("output")) {
    std::cout << "error: bad format" << std::endl << desc << std::endl;
    return 1;
  }

  std::string log_lvl = vm["log_level"].as<std::string>();
  std::size_t threads = vm["thread_count"].as<size_t>();
  std::string output = vm["output"].as<std::string>();

    Logs(log_lvl);

  std::string input_db = "my_db";
  Create_DB(input_db);

  Database manager(input_db, output, threads);
  manager.start_process();
  return 0;
}