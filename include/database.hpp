// Copyright 2022 Evgenzayts evgenzaytsev2002@yandex.ru

#ifndef INCLUDE_DATABASE_HPP_
#define INCLUDE_DATABASE_HPP_

#include <string>
#include <rocksdb/db.h>
#include <boost/log/trivial.hpp>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/advanced_options.h>
#include "../third-party/PicoSHA2/picosha2.h"
#include "../third-party/ThreadPool.h"
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include <queue.hpp>
#include <database.hpp>

//создаём первоначальную БД
void make_inp_BD(const std::string& directory);

std::string calc_hash(const std::string& key, const std::string& value);

//запись таблицы (БД)
struct Entry{
  size_t Handle; //дескриптор
  std::string Key; //ключ
  std::string Value; //значение
};


class My_BD {
 public:
  My_BD(std::string& input_filename, std::string& output_filename,
        size_t number_of_threads);
  ~My_BD();
  void write_val_to_BD(Entry&& KeyHash);
  void parse_inp_BD();
  void make_cons_queue(Entry& en);
  void write_new_BD();
  void start_process();
  void make_cons_pool();

 private:
  //отвечаем за парсинг начальной БД
  bool ParseFlag_ = false;
  //отвечаетуспех вычисления хешей
  bool HashFlag_ = false;
  //отвечает за запись новых пар ключ-значение в новую БД
  bool WriteFlag_ = false;

  Queue<Entry> ProdQueue_;
  Queue<Entry> ConsQueue_;

  std::string input_; //файл с начальной БД
  std::string output_; //файл с конечной БД

  //Семейства столбцов обрабатываются и ссылаются
  // с помощью  ColumnFamilyHandle
  std::vector<rocksdb::ColumnFamilyHandle*> fromHandles_;//начальная БД
  std::vector<rocksdb::ColumnFamilyHandle*> outHandles_;// конечная

  //начальная и конечная БД
  rocksdb::DB* inpBD_ = nullptr;
  rocksdb::DB* outputBD_ = nullptr;
  //пул потоков
  ThreadPool pool_;
};

#endif //INCLUDE_DATABASE_HPP_

