// Copyright 2022 Evgenzayts evgenzaytsev2002@yandex.ru

#include <database.hpp>
#include <iostream>

// СОЗДАНИЕ БД (принимает путь до файла)
void Create_DB(const std::string &directory) {
    //5 значений в каждом из 3-х семейств
    const unsigned int NUMBER_OF_COLUMNS = 3;
    const unsigned int NUMBER_OF_VALUES = 5;

    try {
        //  СОЗДАЁМ И ОТКРЫВАЕМ БАЗУ ДАННЫХ
        // переменная для опций
        rocksdb::Options options;
        options.create_if_missing = true;   // "создать БД, если она не существует"
        rocksdb::DB *db = nullptr;    // db - private член класса - переменная для БД
        rocksdb::Status status = rocksdb::DB::Open(options, directory, &db);    // открываем/создаем БД

        if (!status.ok())
            throw std::runtime_error{"DB::Open failed"};

        std::vector<std::string> column_family; // ЗАДАЁМ СЕМЕЙСТВА СТОЛБЦОВ (ColumnFamilies) в БД
        column_family.reserve(NUMBER_OF_COLUMNS);    // задаём размер вектора
        //заполняем вектор семействами столбцов
        for (size_t i = 0; i < NUMBER_OF_COLUMNS; ++i) {
            column_family.emplace_back("ColumnFamily_" + std::to_string(i + 1));
        }

        std::vector<rocksdb::ColumnFamilyHandle *> handles;
        status = db->CreateColumnFamilies(rocksdb::ColumnFamilyOptions(),
                                          column_family,
                                          &handles);

        if (!status.ok())
            throw std::runtime_error{"CreateColumnFamilies failed"};

        //   ЗАПОЛЯЕМ БД СЛУЧАЙНЫМИ ЗНАЧЕНИЯМИ
        std::string key;
        std::string value;
        for (size_t i = 0; i < NUMBER_OF_COLUMNS; ++i) {
            for (size_t j = 0; j < NUMBER_OF_VALUES; ++j) {
                key = "key-" + std::to_string((i * NUMBER_OF_VALUES) + j);
                value = "value-" + std::to_string(std::rand() % 100);
                status = db->Put(rocksdb::WriteOptions(),
                                 handles[i],
                                 rocksdb::Slice(key),
                                 rocksdb::Slice(value));

                if (!status.ok())
                    throw std::runtime_error{"Putting [" + std::to_string(i + 1) + "][" +
                                             std::to_string(j) + "] failed"};

                BOOST_LOG_TRIVIAL(info) << "Added [" << key << "]:[" << value
                                        << "] -- [" << i + 1 << " family column ]"
                                        << " -- [ FIRST DATA BASE ]";
            }
        }

        //Перед удалением базы данных нужно закрыть
        //все семейства столбцов,
        //вызвав DestroyColumnFamilyHandle() со всеми дескрипторами.
        // закрываем БД
        for (auto &handle: handles) {
            status = db->DestroyColumnFamilyHandle(handle);
            if (!status.ok()) throw std::runtime_error{"DestroyColumnFamily failed"};
        }

        delete db;
    } catch (std::exception &e) {
        BOOST_LOG_TRIVIAL(error) << e.what();
    }
}

std::string Get_hash(const std::string &key, const std::string &value) {
    return picosha2::hash256_hex_string(std::string(key + value));
}

Database::Database(std::string &input_dir,
                   std::string &output_dir,
                   size_t number_of_threads)
        : _prod_queue(),
          _cons_queue(),
          _input(input_dir),
          _output(output_dir),
          _pool(number_of_threads) {
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
        s = rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), _input, &names);
        if (!s.ok()) throw std::runtime_error("ListColumnFamilies is failed");

        //выделяем место в векторе под names
        desc.reserve(names.size());
        // заполняем вектор дескрипторов именами семейств столбцов и опциями
        for (auto &x: names) {
            desc.emplace_back(x, rocksdb::ColumnFamilyOptions());
        }
        //OpenForReadOnly -
        //Поведение аналогично DB::Open, за исключением того,
        // что он открывает БД в режиме только для чтения.
        // Одна большая разница заключается в том, что при открытии БД
        // только для чтения не нужно указывать все семейства столбцов -
        // можно открыть только подмножество семейств столбцов.

        s = rocksdb::DB::OpenForReadOnly(rocksdb::DBOptions(), _input, desc,
                                         &_inp_handles, &_input_DB);
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
        s = rocksdb::DB::Open(options, _output, &_output_DB);
        if (!s.ok()) throw std::runtime_error("Open of output DB is failed");
        //--создаём семецства столбцов--
        //CreateColumnFamilies - создает семейство столбцов,
        //указанное с names, и возвращает
        // ColumnFamilyHandle через аргумент _out_handles.
        _output_DB->CreateColumnFamilies(rocksdb::ColumnFamilyOptions(), names,
                                         &_out_handles);

        _out_handles.insert(_out_handles.begin(), _output_DB->DefaultColumnFamily());
    } catch (std::exception &e) {
        BOOST_LOG_TRIVIAL(error) << e.what();
    }
    //------------------------------------------------------------
}

//парсим исходные данные в первоначальной БД
void Database::parse_input_db() {
    std::vector<rocksdb::Iterator *> iterators;
    rocksdb::Iterator *it;
    //устанавливаем итератор на исходную БД
    for (size_t i = 0; i < _inp_handles.size(); ++i) {
        it = _input_DB->NewIterator(rocksdb::ReadOptions(), _inp_handles[i]);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            //ставим в очередь пары ключ-значение - очередь продюсера
            _prod_queue.push({i, it->key().ToString(), it->value().ToString()});
        }
        iterators.emplace_back(it);
        it = nullptr;
    }

    for (auto &iterator: iterators) {
        delete iterator;
    }

    _parse_flag = true;
}

//деструктор - закрываем начальную БД
Database::~Database() {
    try {
        //Перед удалением базы данных нужно закрыть
        //все семейства столбцов,
        //вызвав DestroyColumnFamilyHandle() со всеми дескрипторами.
        rocksdb::Status s;
        if (!_inp_handles.empty() && _input_DB != nullptr) {
            for (auto &x: _inp_handles) {
                s = _input_DB->DestroyColumnFamilyHandle(x);
                if (!s.ok()) {
                    throw std::runtime_error("Destroy From handle failed in destructor");
                }
            }
            _inp_handles.clear();
            s = _input_DB->Close();
            if (!s.ok()) {
                throw std::runtime_error("Closing of fromDB in destructor");
            }
            delete _input_DB;
        }

        if (!_out_handles.empty() && _output_DB != nullptr) {
            for (auto &x: _out_handles) {
                s = _output_DB->DestroyColumnFamilyHandle(x);
                if (!s.ok()) {
                    throw std::runtime_error(
                            "Destroy Output handle failed in destructor");
                }
            }
            _out_handles.clear();
        }
    } catch (std::exception &e) {
        BOOST_LOG_TRIVIAL(error) << e.what();
    }
}

//запись хешированных данных в новую БД
void Database::write_val_to_db(Input &&KeyHash) {
    try {
        rocksdb::Status s = _output_DB->Put(rocksdb::WriteOptions(),
                                            _out_handles[KeyHash.handle],
                                            KeyHash.key, KeyHash.value);
        BOOST_LOG_TRIVIAL(info)
            << "[" << KeyHash.key << "] " << " [" << KeyHash.value << "] "
            << " [-NEW DATA BASE-]";
        if (!s.ok()) {
            throw std::runtime_error("Writing in output DB is failed");
        }
    } catch (std::exception &e) {
        BOOST_LOG_TRIVIAL(error) << e.what();
    }
}

//----------ставим в очередь консьюмера новые пары  для новой БД------
void Database::make_cons_queue(Input &input) {
    _cons_queue.push({input.handle, input.key, Get_hash(input.key, input.value)});
}

//--------------------------------------------------------------------
//задаём пул потоков консьюмера
void Database::make_cons_pool() {
    Input item;
    //пока есть задачи в очереди продюсера
    while (!_parse_flag || !_prod_queue.empty()) {
        if (_prod_queue.pop(item)) {
            //в пуле потоков вычисляем хеши и формируем очереь на запись
            _pool.enqueue([this](Input x) { make_cons_queue(x); }, item);
        }
    }
    _hash_flag = true;
}

//------------------------------------------------------------------
//------записываем всю очередь в новую БД---------------------------
void Database::write_new_db() {
    Input item;
    //пока есть задачи в очереди консьюмера
    while (!_cons_queue.empty() || !_hash_flag) {
        if (_cons_queue.pop(item)) {
            //записываем их в новую БД
            write_val_to_db(std::move(item));
        }
    }
    _write_flag = true;
}

//-------запускаем потоки и выполняем парсинг и запись------------
void Database::start_process() {
    std::thread producer([this]() { parse_input_db(); });

    std::thread consumer([this]() { write_new_db(); });

    producer.join();
    make_cons_pool();
    consumer.join();
    //если какой-то из флагов не выставлен - ждём
    while (!_hash_flag || !_parse_flag || !_write_flag) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}
