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
void Create_DB(const std::string& directory);

std::string Get_hash(const std::string& key, const std::string& value);

//запись таблицы (БД)
struct Input {
  size_t handle; //дескриптор
  std::string key; //ключ
  std::string value; //значение
};


class Database {
 public:
  Database(std::string& input_filename, std::string& output_filename, size_t number_of_threads);

  ~Database();

  void write_val_to_db(Input&& KeyHash);
  void parse_input_db();
  void make_cons_queue(Input& input);
  void write_new_db();
  void start_process();
  void make_cons_pool();

 private:
  bool _parse_flag = false;   //отвечаем за парсинг начальной БД
  bool _hash_flag = false;     //отвечает за успех вычисления хешей
  bool _write_flag = false;    //отвечает за запись новых пар ключ-значение в новую БД

  Queue<Input> _prod_queue;
  Queue<Input> _cons_queue;

  std::string _input;   //файл с начальной БД
  std::string _output;  //файл с конечной БД

  // Семейства столбцов обрабатываются и ссылаются
  // с помощью  ColumnFamilyHandle
  std::vector<rocksdb::ColumnFamilyHandle*> _inp_handles;   //начальная БД
  std::vector<rocksdb::ColumnFamilyHandle*> _out_handles;    // конечная

  //начальная и конечная БД
  rocksdb::DB* _input_DB = nullptr;
  rocksdb::DB* _output_DB = nullptr;

  //пул потоков
  ThreadPool _pool;
};

#endif //INCLUDE_DATABASE_HPP_
