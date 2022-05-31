// Copyright 2022 Evgenzayts evgenzaytsev2002@yandex.ru

#include <logsettings.hpp>

boost::log::trivial::severity_level Choose_level(const std::string& lvl) {
  if (lvl == "info")
    return boost::log::trivial::severity_level::info;
  else if (lvl == "warning")
    return boost::log::trivial::severity_level::warning;
  else
    return boost::log::trivial::severity_level::error;
}

void Logs(const std::string& lvl) {
  boost::log::add_common_attributes();

  boost::log::core::get()->set_filter(boost::log::trivial::severity == Choose_level(lvl));

  boost::log::add_console_log(std::clog,
                              boost::log::keywords::format = "[%TimeStamp%][%ThreadID%][%Severity%]: %Message%");

  boost::log::add_file_log(boost::log::keywords::file_name = "Logs/Log_%N.log",
                           boost::log::keywords::rotation_size = 10 * 1024 * 1024,
                           boost::log::keywords::time_based_rotation =
                                   boost::log::sinks::file::rotation_at_time_point(0, 0, 0),
                           boost::log::keywords::format = "[%TimeStamp%][%Severity%]: %Message%");
}
