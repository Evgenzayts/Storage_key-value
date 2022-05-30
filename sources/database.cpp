// Copyright 2022 Evgenzayts evgenzaytsev2002@yandex.ru

#include <database.hpp>
#include <iostream>

// СОЗДАНИЕ БД (принимает путь до файла)
void make_inp_BD(const std::string& directory) {
  //5 значений в каждом из 3-х семейств
  const unsigned int NUMBER_OF_COLUMNS = 3;
  const unsigned int NUMBER_OF_VALUES = 5;

  try {
    //  СОЗДАЁМ И ОТКРЫВАЕМ БАЗУ ДАННЫХ
    // переменная для опций
    rocksdb::Options options;
    // "создать БД, если она не существует"
    options.create_if_missing = true;
    // db - private член класса -
    // переменная для БД
    rocksdb::DB* db = nullptr;
    // открываем/создаем БД
    rocksdb::Status status = rocksdb::DB::Open(options, directory, &db);

    if (!status.ok()) throw std::runtime_error{"DB::Open failed"};

    //    ЗАДАЁМ СЕМЕЙСТВА СТОЛБЦОВ (ColumnFamilies) в БД
    std::vector<std::string> column_family;

    // задаём размер вектора
    column_family.reserve(NUMBER_OF_COLUMNS);
    //заполняем вектор семействами столбцов
    for (unsigned int i = 0; i < NUMBER_OF_COLUMNS; ++i) {
      column_family.emplace_back("ColumnFamily_" + std::to_string(i + 1));
    }

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    status = db->CreateColumnFamilies(rocksdb::ColumnFamilyOptions(),
                                      column_family, &handles);
    if (!status.ok()) throw std::runtime_error{"CreateColumnFamilies failed"};

    //   ЗАПОЛЯЕМ БД СЛУЧАЙНЫМИ ЗНАЧЕНИЯМИ
    std::string key;
    std::string value;
    for (unsigned int i = 0; i < NUMBER_OF_COLUMNS; ++i) {
      for (unsigned int j = 0; j < NUMBER_OF_VALUES; ++j) {
        key = "key-" + std::to_string((i * NUMBER_OF_VALUES) + j);
        value = "value-" + std::to_string(std::rand() % 100);
        status = db->Put(rocksdb::WriteOptions(), handles[i],
                         rocksdb::Slice(key), rocksdb::Slice(value));
        if (!status.ok())
          throw std::runtime_error{"Putting [" + std::to_string(i + 1) + "][" +
                                   std::to_string(j) + "] failed"};

        BOOST_LOG_TRIVIAL(info) << "Added [" << key << "]:[" << value
                                << "] -- [" << i + 1 << " family column ]"
                                <<" -- [ FIRST DATA BASE ]";
      }
    }
    //Перед удалением базы данных нужно закрыть
    //все семейства столбцов,
    //вызвав DestroyColumnFamilyHandle() со всеми дескрипторами.
    // закрываем БД
    for (auto& x : handles) {
      status = db->DestroyColumnFamilyHandle(x);
      if (!status.ok()) throw std::runtime_error{"DestroyColumnFamily failed"};
    }

    delete db;
  } catch (std::exception& e) {
    BOOST_LOG_TRIVIAL(error) << e.what();
  }
}

My_BD::My_BD(std::string& input_dir,
             std::string& output_dir,
             size_t number_of_threads)
    : ProdQueue_(),
      ConsQueue_(),
      input_(input_dir),
      output_(output_dir),
      pool_(number_of_threads) {
  //STATUS-
  // Значения этого типа возвращаются большинством функций
  // в RocksDB, которые могут столкнуться с ошибкой
  rocksdb::Status s{};
  std::vector<std::string> names;
  //ColumnFamilyDescriptor - структура с именем семейства столбцов
  // и ColumnFamilyOptions
  std::vector<rocksdb::ColumnFamilyDescriptor> desc;
  try {
    //List Column Families  - это статическая функция,
    //которая возвращает список всех семейств столбцов,
    //присутствующих в данный момент в базе данных.
    s = rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), input_, &names);
    if (!s.ok()) throw std::runtime_error("ListColumnFamilies is failed");

    //выделяем место в векторе под names
    desc.reserve(names.size());
    // заполняем вектор дескрипторов именами семейств столбцов и опциями
    for (auto& x : names) {
      desc.emplace_back(x, rocksdb::ColumnFamilyOptions());
    }
    //OpenForReadOnly -
    //Поведение аналогично DB::Open, за исключением того,
    // что он открывает БД в режиме только для чтения.
    // Одна большая разница заключается в том, что при открытии БД
    // только для чтения не нужно указывать все семейства столбцов -
    // можно открыть только подмножество семейств столбцов.

    s = rocksdb::DB::OpenForReadOnly(rocksdb::DBOptions(), input_, desc,
                                     &fromHandles_, &inpBD_);
    if (!s.ok())
      throw std::runtime_error("OpenForReadOnly of input DB is failed");
    //очищаем вектор с именами (строки)
    names.erase(names.begin());
    //------создаём новую БД для хешей-------------------------
    //Options структура определяет, как RocksDB ведет себя
    rocksdb::Options options;
    //проверяем создана ли БД
    options.create_if_missing = true;
    //открываем БД на запись
    s = rocksdb::DB::Open(options, output_, &outputBD_);
    if (!s.ok()) throw std::runtime_error("Open of output DB is failed");
    //--создаём семецства столбцов--
    //CreateColumnFamilies - создает семейство столбцов,
    //указанное с names, и возвращает
    // ColumnFamilyHandle через аргумент outHandles_.
    outputBD_->CreateColumnFamilies(rocksdb::ColumnFamilyOptions(), names,
                                    &outHandles_);

    outHandles_.insert(outHandles_.begin(), outputBD_->DefaultColumnFamily());
  } catch (std::exception& e) {
    BOOST_LOG_TRIVIAL(error) << e.what();
  }
  //------------------------------------------------------------
}

//парсим исходные данные в первоначальной БД
void My_BD::parse_inp_BD() {
  std::vector<rocksdb::Iterator*> iterators;
  rocksdb::Iterator* it;
  //устанавливаем итератор на исходную БД
  for (size_t i = 0; i < fromHandles_.size(); ++i) {
    it = inpBD_->NewIterator(rocksdb::ReadOptions(), fromHandles_[i]);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      //ставим в очередь пары ключ-значение - очередь продюсера
      ProdQueue_.push({i, it->key().ToString(), it->value().ToString()});
    }
    iterators.emplace_back(it);
    it = nullptr;
  }

  for (auto& x : iterators) {
    delete x;
  }

  ParseFlag_ = true;
}

//деструктор - закрываем начальную БД
My_BD::~My_BD() {
  try {
    //Перед удалением базы данных нужно закрыть
    //все семейства столбцов,
    //вызвав DestroyColumnFamilyHandle() со всеми дескрипторами.
    rocksdb::Status s;
    if (!fromHandles_.empty() && inpBD_ != nullptr) {
      for (auto& x : fromHandles_) {
        s = inpBD_->DestroyColumnFamilyHandle(x);
        if (!s.ok()) {
          throw std::runtime_error("Destroy From Handle failed in destructor");
        }
      }
      fromHandles_.clear();
      s = inpBD_->Close();
      if (!s.ok()) {
        throw std::runtime_error("Closing of fromDB in destructor");
      }
      delete inpBD_;
    }

    if (!outHandles_.empty() && outputBD_ != nullptr) {
      for (auto& x : outHandles_) {
        s = outputBD_->DestroyColumnFamilyHandle(x);
        if (!s.ok()) {
          throw std::runtime_error(
              "Destroy Output Handle failed in destructor");
        }
      }
      outHandles_.clear();
    }
  } catch (std::exception& e) {
    BOOST_LOG_TRIVIAL(error) << e.what();
  }
}
//запись хешированных данных в новую БД
void My_BD::write_val_to_BD(Entry &&Key_Hash) {
  try {
    rocksdb::Status s = outputBD_->Put(rocksdb::WriteOptions(),
                                       outHandles_[Key_Hash.Handle],
                                       Key_Hash.Key, Key_Hash.Value);
    BOOST_LOG_TRIVIAL(info)
        <<"[" << Key_Hash.Key << "] " << " [" << Key_Hash.Value << "] "
        << " [-NEW DATA BASE-]";
    if (!s.ok()) {
      throw std::runtime_error("Writing in output DB is failed");
    }
  } catch (std::exception& e) {
    BOOST_LOG_TRIVIAL(error) << e.what();
  }
}
//-----------вычисляем хеш--------------------------------------------
std::string calc_hash(const std::string& key, const std::string& value) {
  return picosha2::hash256_hex_string(std::string(key + value));
}
//----------ставим в очередь консьюмера новые пары  для новой БД------
void My_BD::make_cons_queue(Entry& en) {
  ConsQueue_.push({en.Handle, en.Key, calc_hash(en.Key, en.Value)});
}
//--------------------------------------------------------------------
//задаём пул потоков консьюмера
void My_BD::make_cons_pool() {
  Entry item;
  //пока есть задачи в очереди продюсера
  while (!ParseFlag_ || !ProdQueue_.empty()) {
    if (ProdQueue_.pop(item)) {
      //в пуле потоков вычисляем хеши и формируем очереь на запись
      pool_.enqueue([this](Entry x) { make_cons_queue(x); }, item);
    }
  }
  HashFlag_ = true;
}
//------------------------------------------------------------------
//------записываем всю очередь в новую БД---------------------------
void My_BD::write_new_BD() {
  Entry item;
  //пока есть задачи в очереди консьюмера
  while (!ConsQueue_.empty() || !HashFlag_) {
    if (ConsQueue_.pop(item)) {
      //записываем их в новую БД
      write_val_to_BD(std::move(item));
    }
  }
  WriteFlag_ = true;
}
//-------запускаем потоки и выполняем парсинг и запись---------------
void My_BD::start_process() {
  std::thread producer([this]() { parse_inp_BD(); });

  std::thread consumer([this]() { write_new_BD(); });

  producer.join();
  make_cons_pool();
  consumer.join();
  //если какой-то из флагов не выставлен - ждём
  while (!HashFlag_ || !ParseFlag_ || !WriteFlag_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}
