#include <yokan/cxx/client.hpp>
#include <tclap/CmdLine.h>
#include <functional>
#include <unordered_map>
#include <fstream>
#include <iostream>
#include <regex>
#include <chrono>

using namespace std::string_literals;

static std::string gen_random_string(size_t len)
{
    static const char alphanum[]
        = "0123456789"
          "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
          "abcdefghijklmnopqrstuvwxyz";
    std::string s(len, ' ');
    for (unsigned i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return s;
}

struct range : public TCLAP::StringLikeTrait {

    size_t min = 0;
    size_t max = 0;

    range() = default;
    range(const std::string& str) {
        std::regex rgx("^(0|([1-9][0-9]*))(,(0|([1-9][0-9]*)))?$");
        std::smatch matches;
        if(std::regex_search(str, matches, rgx)) {
            min = atol(matches[1].str().c_str());
            if(!matches[4].str().empty())
                max = atol(matches[1].str().c_str());
        } else {
            throw TCLAP::ArgParseException("invalid range format \""s + str + "\"");
        }
    }
};

struct options {
    std::string operation;
    range       key_sizes;
    range       val_sizes;
    size_t      num_items;
    unsigned    seed;
    unsigned    repetitions;
    std::string database_id;
    std::string margo_config;
    std::string server_address;
    uint16_t    provider_id;
    std::string prefix;
    uint8_t     prefix_freq;
    bool        no_remove;
    unsigned    batch_size;
};

void fill_reference_map(const options& opt, std::unordered_map<std::string, std::string>& map) {
    for(size_t i=0; i < opt.num_items; i++) {
        auto ksize = opt.key_sizes.min + (rand() % (opt.key_sizes.max - opt.key_sizes.min + 1));
        auto vsize = opt.val_sizes.min + (rand() % (opt.val_sizes.max - opt.val_sizes.min + 1));
        std::string key, val;
        auto use_prefix = (rand() % 100) < opt.prefix_freq;
        if(use_prefix) {
            key = opt.prefix + gen_random_string(ksize);
        } else {
            key = gen_random_string(ksize + opt.prefix.size());
        }
        val = gen_random_string(vsize);
        map.emplace(std::move(key), std::move(val));
    }
}

void remove_keys_from_database(const std::shared_ptr<yokan::Database>& db,
                               const std::unordered_map<std::string, std::string>& ref) {
    std::string packed_keys;
    std::vector<size_t> packed_ksizes;
    packed_ksizes.reserve(ref.size());
    size_t total_size = 0;
    for(auto& pair : ref) {
        total_size += pair.first.size();
        packed_ksizes.push_back(pair.first.size());
    }
    packed_keys.reserve(total_size);
    for(auto& pair : ref) {
        packed_keys += pair.first;
    }
    db->erasePacked(ref.size(), packed_keys.data(), packed_ksizes.data());
}

void put_keys_into_database(const std::shared_ptr<yokan::Database>& db,
                            const std::unordered_map<std::string, std::string>& ref) {
    std::string packed_keys;
    std::string packed_vals;
    std::vector<size_t> packed_ksizes;
    std::vector<size_t> packed_vsizes;
    packed_ksizes.reserve(ref.size());
    packed_vsizes.reserve(ref.size());
    size_t total_ksize = 0;
    size_t total_vsize = 0;
    for(auto& pair : ref) {
        total_ksize += pair.first.size();
        total_vsize += pair.second.size();
        packed_ksizes.push_back(pair.first.size());
        packed_vsizes.push_back(pair.second.size());
    }
    packed_keys.reserve(total_ksize);
    packed_vals.reserve(total_vsize);
    for(auto& pair : ref) {
        packed_keys += pair.first;
        packed_vals += pair.second;
    }
    db->putPacked(ref.size(), packed_keys.data(), packed_ksizes.data(),
                              packed_vals.data(), packed_vsizes.data());
}

class Benchmark {

    options m_opt;

    public:

    Benchmark(const options& opt)
    : m_opt(opt) {}

    virtual ~Benchmark() = default;
    virtual void setUp() = 0;
    virtual void run() = 0;
    virtual void tearDown() = 0;

    const options& getOptions() const {
        return m_opt;
    }

    using BenchmarkFactory = std::function<std::unique_ptr<Benchmark>(std::shared_ptr<yokan::Database>, const options&)>;
    static std::unordered_map<std::string, BenchmarkFactory> factories;
};

std::unordered_map<std::string, Benchmark::BenchmarkFactory> Benchmark::factories;

template<class B>
struct BenchmarkFactoryRegistration {
    BenchmarkFactoryRegistration(const char* name) {
        Benchmark::factories[name] = [](std::shared_ptr<yokan::Database> db, const options& opt) {
            return std::unique_ptr<Benchmark>(new B(db, opt));
        };
    }
};
#define REGISTER_BENCHMARK(__class__, __name__) \
    static BenchmarkFactoryRegistration<__class__> \
        _benchmark_registration_for_ ##__name__(#__name__)


/**
 * @brief PUT benchmark
 */
class PutBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;

    public:

    PutBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {}

    void run() override {
        for(auto& pair : m_ref) {
            m_db->put(pair.first.data(), pair.first.size(),
                      pair.second.data(), pair.second.size());
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(PutBenchmark, put);

/**
 * @brief PUT-MULTI benchmark
 */
class PutMultiBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<std::tuple<std::vector<const void*>, // key ptrs
                           std::vector<size_t>,      // key sizes
                           std::vector<const void*>, // val ptrs
                           std::vector<size_t>>>     // val sizes
                               m_batches;

    public:

    PutMultiBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        unsigned batch_size = getOptions().batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        m_batches.resize((m_ref.size() + batch_size - 1) / batch_size);
        unsigned i = 0;
        for(auto& pair : m_ref) {
            unsigned k = i % batch_size;
            auto& batch = m_batches[k];
            std::get<0>(batch).push_back(pair.first.data());
            std::get<1>(batch).push_back(pair.first.size());
            std::get<2>(batch).push_back(pair.second.data());
            std::get<3>(batch).push_back(pair.second.size());
            i += 1;
        }
    }

    void run() override {
        for(auto& batch : m_batches) {
            const auto& kptrs = std::get<0>(batch);
            const auto& ksize = std::get<1>(batch);
            const auto& vptrs = std::get<2>(batch);
            const auto& vsize = std::get<3>(batch);
            m_db->putMulti(kptrs.size(), kptrs.data(), ksize.data(),
                           vptrs.data(), vsize.data());
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(PutMultiBenchmark, put_multi);

/**
 * @brief PUT-PACKED benchmark
 */
class PutPackedBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<std::tuple<std::string,          // keys
                           std::vector<size_t>,  // key sizes
                           std::string,          // val ptrs
                           std::vector<size_t>>> // val sizes
                               m_batches;

    public:

    PutPackedBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        unsigned batch_size = getOptions().batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        m_batches.resize((m_ref.size() + batch_size - 1) / batch_size);
        unsigned i = 0;
        for(auto& pair : m_ref) {
            unsigned k = i/batch_size;
            auto& batch = m_batches[k];
            std::get<0>(batch) += pair.first;
            std::get<1>(batch).push_back(pair.first.size());
            std::get<2>(batch) += pair.second;
            std::get<3>(batch).push_back(pair.second.size());
            i += 1;
        }
    }

    void run() override {
        for(auto& batch : m_batches) {
            const auto& keys  = std::get<0>(batch);
            const auto& ksize = std::get<1>(batch);
            const auto& vals  = std::get<2>(batch);
            const auto& vsize = std::get<3>(batch);
            m_db->putPacked(ksize.size(), keys.data(), ksize.data(),
                            vals.data(), vsize.data());
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(PutPackedBenchmark, put_packed);

/**
 * @brief GET benchmark
 */
class GetBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<char> m_buffer;

    public:

    GetBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        auto& opt = getOptions();
        m_buffer.resize(opt.val_sizes.max);
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        size_t vsize;
        for(auto& pair : m_ref) {
            vsize = m_buffer.size();
            m_db->get(pair.first.data(), pair.first.size(),
                      m_buffer.data(), &vsize);
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(GetBenchmark, get);

/**
 * @brief GET-MULTI benchmark
 */
class GetMultiBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<std::vector<char>> m_buffers; // buffers to store values
    std::vector<std::tuple<std::vector<const void*>, // key ptrs
                           std::vector<size_t>,      // key sizes
                           std::vector<void*>,       // val ptrs
                           std::vector<size_t>>>     // val sizes
                               m_batches;

    public:

    GetMultiBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        unsigned batch_size = getOptions().batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        auto num_batches = (m_ref.size() + batch_size - 1) / batch_size;
        m_batches.resize(num_batches);
        m_buffers.resize(num_batches);
        unsigned i = 0;
        auto& opt = getOptions();
        for(auto& pair : m_ref) {
            unsigned k = i % batch_size;
            auto& batch = m_batches[k];
            auto& buffer = m_buffers[k];
            if(buffer.size() == 0)
                buffer.resize(opt.val_sizes.max);
            std::get<0>(batch).push_back(pair.first.data());
            std::get<1>(batch).push_back(pair.first.size());
            std::get<2>(batch).push_back(buffer.data());
            std::get<3>(batch).push_back(buffer.size());
            i += 1;
        }
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        for(auto& batch : m_batches) {
            const auto& kptrs = std::get<0>(batch);
            const auto& ksize = std::get<1>(batch);
            auto& vptrs = std::get<2>(batch);
            auto& vsize = std::get<3>(batch);
            m_db->getMulti(kptrs.size(), kptrs.data(), ksize.data(),
                           vptrs.data(), vsize.data());
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(GetMultiBenchmark, get_multi);

/**
 * @brief GET-PACKED benchmark
 */
class GetPackedBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<char> m_buffer; // buffers to store values
    std::vector<size_t> m_vsizes; // buffer for value sizes
    std::vector<std::tuple<std::string,          // keys packed
                           std::vector<size_t>>> // key sizes
                               m_batches;

    public:

    GetPackedBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        unsigned batch_size = getOptions().batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        auto num_batches = (m_ref.size() + batch_size - 1) / batch_size;
        auto& opt = getOptions();
        m_batches.resize(num_batches);
        m_buffer.resize(batch_size*opt.val_sizes.max);
        m_vsizes.resize(batch_size);
        unsigned i = 0;
        for(auto& pair : m_ref) {
            unsigned k = i/batch_size;
            auto& batch = m_batches[k];
            std::get<0>(batch) += pair.first;
            std::get<1>(batch).push_back(pair.first.size());
            i += 1;
        }
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        for(auto& batch : m_batches) {
            const auto& keys  = std::get<0>(batch);
            const auto& ksize = std::get<1>(batch);
            m_db->getPacked(ksize.size(), keys.data(), ksize.data(),
                            m_buffer.size(), m_buffer.data(), m_vsizes.data());
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(GetPackedBenchmark, get_packed);

/**
 * @brief LENGTH benchmark
 */
class LengthBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;

    public:

    LengthBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        for(auto& pair : m_ref) {
            m_db->length(pair.first.data(), pair.first.size());
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(LengthBenchmark, length);

/**
 * @brief LENGTH-MULTI benchmark
 */
class LengthMultiBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<size_t> m_vsizes;
    std::vector<std::tuple<std::vector<const void*>, // key ptrs
                           std::vector<size_t>>>      // key sizes
                               m_batches;

    public:

    LengthMultiBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        unsigned batch_size = getOptions().batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        auto num_batches = (m_ref.size() + batch_size - 1) / batch_size;
        m_batches.resize(num_batches);
        m_vsizes.resize(batch_size);
        unsigned i = 0;
        for(auto& pair : m_ref) {
            unsigned k = i/batch_size;
            auto& batch = m_batches[k];
            std::get<0>(batch).push_back(pair.first.data());
            std::get<1>(batch).push_back(pair.first.size());
            i += 1;
        }
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        for(auto& batch : m_batches) {
            const auto& kptrs = std::get<0>(batch);
            const auto& ksize = std::get<1>(batch);
            m_db->lengthMulti(kptrs.size(), kptrs.data(), ksize.data(),
                              m_vsizes.data());
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(LengthMultiBenchmark, length_multi);

/**
 * @brief LENGTH-PACKED benchmark
 */
class LengthPackedBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<size_t> m_vsizes; // buffer for value sizes
    std::vector<std::tuple<std::string,          // keys packed
                           std::vector<size_t>>> // key sizes
                               m_batches;

    public:

    LengthPackedBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        unsigned batch_size = getOptions().batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        auto num_batches = (m_ref.size() + batch_size - 1) / batch_size;
        m_batches.resize(num_batches);
        m_vsizes.resize(batch_size);
        unsigned i = 0;
        for(auto& pair : m_ref) {
            unsigned k = i/batch_size;
            auto& batch = m_batches[k];
            std::get<0>(batch) += pair.first;
            std::get<1>(batch).push_back(pair.first.size());
            i += 1;
        }
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        for(auto& batch : m_batches) {
            const auto& keys  = std::get<0>(batch);
            const auto& ksize = std::get<1>(batch);
            m_db->lengthPacked(ksize.size(), keys.data(), ksize.data(),
                               m_vsizes.data());
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(LengthPackedBenchmark, length_packed);

/**
 * @brief EXSISTS benchmark
 */
class ExistsBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::unordered_map<std::string, std::string> m_ref_stored;
    std::shared_ptr<yokan::Database> m_db;

    public:

    ExistsBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        int i = 0;
        for(auto& pair : m_ref) {
            if(i % 2 == 0)
                m_ref_stored[pair.first] = pair.second;
            i += 1;
        }
        put_keys_into_database(m_db, m_ref_stored);
    }

    void run() override {
        for(auto& pair : m_ref) {
            m_db->exists(pair.first.data(), pair.first.size());
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(ExistsBenchmark, exists);

/**
 * @brief EXISTS-MULTI benchmark
 */
class ExistsMultiBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::unordered_map<std::string, std::string> m_ref_stored;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<std::tuple<std::vector<const void*>, // key ptrs
                           std::vector<size_t>>>      // key sizes
                               m_batches;

    public:

    ExistsMultiBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        unsigned batch_size = getOptions().batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        auto num_batches = (m_ref.size() + batch_size - 1) / batch_size;
        m_batches.resize(num_batches);
        unsigned i = 0;
        for(auto& pair : m_ref) {
            if(i % 2 == 0)
                m_ref_stored[pair.first] = pair.second;
            unsigned k = i/batch_size;
            auto& batch = m_batches[k];
            std::get<0>(batch).push_back(pair.first.data());
            std::get<1>(batch).push_back(pair.first.size());
            i += 1;
        }
        put_keys_into_database(m_db, m_ref_stored);
    }

    void run() override {
        for(auto& batch : m_batches) {
            const auto& kptrs = std::get<0>(batch);
            const auto& ksize = std::get<1>(batch);
            m_db->existsMulti(kptrs.size(), kptrs.data(), ksize.data());
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref_stored);
    }
};
REGISTER_BENCHMARK(ExistsMultiBenchmark, exists_multi);

/**
 * @brief EXISTS-PACKED benchmark
 */
class ExistsPackedBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<std::tuple<std::string,          // keys packed
                           std::vector<size_t>>> // key sizes
                               m_batches;

    public:

    ExistsPackedBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        unsigned batch_size = getOptions().batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        auto num_batches = (m_ref.size() + batch_size - 1) / batch_size;
        m_batches.resize(num_batches);
        unsigned i = 0;
        for(auto& pair : m_ref) {
            unsigned k = i/batch_size;
            auto& batch = m_batches[k];
            std::get<0>(batch) += pair.first;
            std::get<1>(batch).push_back(pair.first.size());
            i += 1;
        }
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        for(auto& batch : m_batches) {
            const auto& keys  = std::get<0>(batch);
            const auto& ksize = std::get<1>(batch);
            m_db->existsPacked(ksize.size(), keys.data(), ksize.data());
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(ExistsPackedBenchmark, exists_packed);

/**
 * @brief ERASE benchmark
 */
class EraseBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;

    public:

    EraseBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        for(auto& pair : m_ref) {
            m_db->erase(pair.first.data(), pair.first.size());
        }
    }

    void tearDown() override {}
};
REGISTER_BENCHMARK(EraseBenchmark, erase);

/**
 * @brief ERASE-MULTI benchmark
 */
class EraseMultiBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<std::tuple<std::vector<const void*>, // key ptrs
                           std::vector<size_t>>>     // key sizes
                               m_batches;

    public:

    EraseMultiBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        unsigned batch_size = getOptions().batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        m_batches.resize((m_ref.size() + batch_size - 1) / batch_size);
        unsigned i = 0;
        for(auto& pair : m_ref) {
            unsigned k = i/batch_size;
            auto& batch = m_batches[k];
            std::get<0>(batch).push_back(pair.first.data());
            std::get<1>(batch).push_back(pair.first.size());
            i += 1;
        }
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        for(auto& batch : m_batches) {
            const auto& kptrs = std::get<0>(batch);
            const auto& ksize = std::get<1>(batch);
            m_db->eraseMulti(kptrs.size(), kptrs.data(), ksize.data());
        }
    }

    void tearDown() override {}
};
REGISTER_BENCHMARK(EraseMultiBenchmark, erase_multi);

/**
 * @brief ERASE-PACKED benchmark
 */
class ErasePackedBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<std::tuple<std::string,          // keys
                           std::vector<size_t>>> // key sizes
                               m_batches;

    public:

    ErasePackedBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        unsigned batch_size = getOptions().batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        m_batches.resize((m_ref.size() + batch_size - 1) / batch_size);
        unsigned i = 0;
        for(auto& pair : m_ref) {
            unsigned k = i/batch_size;
            auto& batch = m_batches[k];
            std::get<0>(batch) += pair.first;
            std::get<1>(batch).push_back(pair.first.size());
            i += 1;
        }
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        for(auto& batch : m_batches) {
            const auto& keys  = std::get<0>(batch);
            const auto& ksize = std::get<1>(batch);
            m_db->erasePacked(ksize.size(), keys.data(), ksize.data());
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(ErasePackedBenchmark, erase_packed);

/**
 * @brief LIST-KEYS benchmark
 */
class ListKeysBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<std::vector<char>> m_key_buffers; // buffers to store keys
    std::vector<std::tuple<std::vector<void*>,    // key ptrs
                           std::vector<size_t>>>  // key sizes
                               m_batches;

    public:

    ListKeysBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        auto& opt = getOptions();
        unsigned batch_size = opt.batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        auto num_batches = (m_ref.size() + batch_size - 1) / batch_size;
        m_batches.resize(num_batches);
        m_key_buffers.resize(batch_size, std::vector<char>(opt.key_sizes.max + opt.prefix.size()));
        for(unsigned i = 0; i < num_batches; i++) {
            auto& batch = m_batches[i];
            for(unsigned j = 0; j < batch_size; j++) {
                auto& kbuffer = m_key_buffers[j];
                std::get<0>(batch).push_back(kbuffer.data());
                std::get<1>(batch).push_back(kbuffer.size());
            }
        }
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        std::string start_key;
        auto& prefix = getOptions().prefix;
        auto batch_size = getOptions().batch_size;
        for(auto& batch : m_batches) {
            auto& kptrs = std::get<0>(batch);
            auto& ksize = std::get<1>(batch);
            m_db->listKeys(start_key.data(),
                           start_key.size(),
                           prefix.data(),
                           prefix.size(),
                           batch_size,
                           kptrs.data(),
                           ksize.data());
            for(unsigned i = 0; i < batch_size; i++) {
                if(ksize[i] == YOKAN_NO_MORE_KEYS)
                    return;
                else if(i == batch_size-1)
                    start_key = std::string((char*)kptrs[i], ksize[i]);
            }
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(ListKeysBenchmark, list_keys);

/**
 * @brief LIST-KEYS-PACKED benchmark
 */
class ListKeysPackedBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<std::tuple<std::vector<char>,      // keys
                           std::vector<size_t>>>   // key sizes
                               m_batches;

    public:

    ListKeysPackedBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        auto& opt = getOptions();
        unsigned batch_size = opt.batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        auto num_batches = (m_ref.size() + batch_size - 1) / batch_size;
        m_batches.resize(num_batches);
        for(unsigned i = 0; i < num_batches; i++) {
            auto& batch = m_batches[i];
            std::get<0>(batch).resize(batch_size*(opt.key_sizes.max + opt.prefix.size()));
            std::get<1>(batch).resize(batch_size, opt.key_sizes.max + opt.prefix.size());
        }
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        std::string start_key;
        auto& prefix = getOptions().prefix;
        auto batch_size = getOptions().batch_size;
        for(auto& batch : m_batches) {
            auto& keys  = std::get<0>(batch);
            auto& ksize = std::get<1>(batch);
            m_db->listKeysPacked(start_key.data(),
                                 start_key.size(),
                                 prefix.data(),
                                 prefix.size(),
                                 batch_size,
                                 keys.data(),
                                 keys.size(),
                                 ksize.data());
            size_t koffset = 0;
            for(unsigned i = 0; i < batch_size; i++) {
                if(ksize[i] == YOKAN_NO_MORE_KEYS)
                    return;
                else if(i == batch_size-1)
                    start_key = std::string((char*)keys.data()+koffset, ksize[i]);
                else
                    koffset += ksize[i];
            }
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(ListKeysPackedBenchmark, list_keys_packed);

/**
 * @brief LIST-KEYVALS benchmark
 */
class ListKeyValsBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<std::vector<char>> m_key_buffers; // buffers to store keys
    std::vector<std::vector<char>> m_val_buffers; // buffers to store values
    std::vector<std::tuple<std::vector<void*>,    // key ptrs
                           std::vector<size_t>,   // key sizes
                           std::vector<void*>,    // val ptrs
                           std::vector<size_t>>>  // val sizes
                               m_batches;

    public:

    ListKeyValsBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        auto& opt = getOptions();
        unsigned batch_size = opt.batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        auto num_batches = (m_ref.size() + batch_size - 1) / batch_size;
        m_batches.resize(num_batches);
        m_key_buffers.resize(batch_size, std::vector<char>(opt.key_sizes.max + opt.prefix.size()));
        m_val_buffers.resize(batch_size, std::vector<char>(opt.val_sizes.max));
        for(unsigned i = 0; i < num_batches; i++) {
            auto& batch = m_batches[i];
            for(unsigned j = 0; j < batch_size; j++) {
                auto& kbuffer = m_key_buffers[j];
                auto& vbuffer = m_val_buffers[j];
                std::get<0>(batch).push_back(kbuffer.data());
                std::get<1>(batch).push_back(kbuffer.size());
                std::get<2>(batch).push_back(vbuffer.data());
                std::get<3>(batch).push_back(vbuffer.size());
            }
        }
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        std::string start_key;
        auto& prefix = getOptions().prefix;
        auto batch_size = getOptions().batch_size;
        for(auto& batch : m_batches) {
            auto& kptrs = std::get<0>(batch);
            auto& ksize = std::get<1>(batch);
            auto& vptrs = std::get<2>(batch);
            auto& vsize = std::get<3>(batch);
            m_db->listKeyVals(start_key.data(),
                              start_key.size(),
                              prefix.data(),
                              prefix.size(),
                              batch_size,
                              kptrs.data(),
                              ksize.data(),
                              vptrs.data(),
                              vsize.data());
            for(unsigned i = 0; i < batch_size; i++) {
                if(ksize[i] == YOKAN_NO_MORE_KEYS)
                    return;
                else if(i == batch_size-1)
                    start_key = std::string((char*)kptrs[i], ksize[i]);
            }
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(ListKeyValsBenchmark, list_keyvals);

/**
 * @brief LIST-KEYVALS-PACKED benchmark
 */
class ListKeyValsPackedBenchmark : public Benchmark {

    std::unordered_map<std::string, std::string> m_ref;
    std::shared_ptr<yokan::Database> m_db;
    std::vector<std::tuple<std::vector<char>,    // keys
                           std::vector<size_t>,   // key sizes
                           std::vector<char>,    // vals
                           std::vector<size_t>>>  // val sizes
                               m_batches;

    public:

    ListKeyValsPackedBenchmark(std::shared_ptr<yokan::Database> db, const options& opt)
    : Benchmark(opt), m_db(std::move(db)) {
        fill_reference_map(opt, m_ref);
    }

    void setUp() override {
        auto& opt = getOptions();
        unsigned batch_size = opt.batch_size;
        if(batch_size == 0) batch_size = m_ref.size();
        auto num_batches = (m_ref.size() + batch_size - 1) / batch_size;
        m_batches.resize(num_batches);
        for(unsigned i = 0; i < num_batches; i++) {
            auto& batch = m_batches[i];
            std::get<0>(batch).resize(batch_size*(opt.key_sizes.max + opt.prefix.size()));
            std::get<1>(batch).resize(batch_size, opt.key_sizes.max + opt.prefix.size());
            std::get<2>(batch).resize(batch_size*opt.val_sizes.max);
            std::get<3>(batch).resize(batch_size, opt.val_sizes.max);
        }
        put_keys_into_database(m_db, m_ref);
    }

    void run() override {
        std::string start_key;
        auto& prefix = getOptions().prefix;
        auto batch_size = getOptions().batch_size;
        for(auto& batch : m_batches) {
            auto& keys  = std::get<0>(batch);
            auto& ksize = std::get<1>(batch);
            auto& vals  = std::get<2>(batch);
            auto& vsize = std::get<3>(batch);
            m_db->listKeyValsPacked(start_key.data(),
                                    start_key.size(),
                                    prefix.data(),
                                    prefix.size(),
                                    batch_size,
                                    keys.data(),
                                    keys.size(),
                                    ksize.data(),
                                    vals.data(),
                                    vals.size(),
                                    vsize.data());
            size_t koffset = 0;
            for(unsigned i = 0; i < batch_size; i++) {
                if(ksize[i] == YOKAN_NO_MORE_KEYS)
                    return;
                else if(i == batch_size-1)
                    start_key = std::string((char*)keys.data()+koffset, ksize[i]);
                else
                    koffset += ksize[i];
            }
        }
    }

    void tearDown() override {
        remove_keys_from_database(m_db, m_ref);
    }
};
REGISTER_BENCHMARK(ListKeyValsPackedBenchmark, list_keyvals_packed);

static options parse_arguments(int argc, char** argv);

int main(int argc, char** argv) {
    auto opt = parse_arguments(argc, argv);
    srand(opt.seed);

    std::string protocol;
    {
        auto p = opt.server_address.find(":");
        protocol = opt.server_address.substr(0, p);
    }

    std::string margo_config_str;
    if(!opt.margo_config.empty()) {
        std::ifstream t(opt.margo_config.c_str());
        if(!t.good()) {
            std::cerr << "ERROR: file " << opt.margo_config << " does not exist" << std::endl;
            exit(-1);
        }
        std::stringstream buffer;
        buffer << t.rdbuf();
        margo_config_str = buffer.str();
    }

    margo_init_info margo_args;
    memset(&margo_args, 0, sizeof(margo_args));
    margo_args.json_config = margo_config_str.c_str();
    margo_instance_id mid = margo_init_ext(protocol.c_str(),
                                           MARGO_CLIENT_MODE,
                                           &margo_args);
    if(!mid) {
        std::cerr << "ERROR: could not initialize margo instance" << std::endl;
        exit(-1);
    }

    hg_addr_t svr_addr = HG_ADDR_NULL;
    hg_return_t hret = margo_addr_lookup(mid, opt.server_address.c_str(), &svr_addr);
    if(hret != HG_SUCCESS) {
        std::cerr << "ERROR: could not lookup address " << opt.server_address << std::endl;
        margo_finalize(mid);
        exit(-1);
    }

    yk_database_id_t database_id;
    yk_database_id_from_string(opt.database_id.c_str(), &database_id);

    std::shared_ptr<yokan::Client> client;
    std::shared_ptr<yokan::Database> database;

    try {
        client = std::make_shared<yokan::Client>(mid);
        database = std::make_shared<yokan::Database>(
            client->makeDatabaseHandle(
                svr_addr, opt.provider_id, database_id));
    } catch(const yokan::Exception& ex) {
        std::cerr << "ERROR: " << ex.what() << std::endl;
        margo_finalize(mid);
        exit(-1);
    }

    if(Benchmark::factories.count(opt.operation) == 0) {
        std::cerr << "ERROR: invalid operation " << opt.operation << std::endl;
        margo_finalize(mid);
        exit(-1);
    }

    {
        std::vector<double> timings;
        auto& factory = Benchmark::factories[opt.operation];
        for(unsigned i = 0; i < opt.repetitions; i++) {
            auto benchmark = factory(database, opt);
            benchmark->setUp();
            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
            benchmark->run();
            std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            benchmark->tearDown();
            timings.push_back(
                std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count());
        }
        double avg = 0.0;
        double var = 0.0;
        double min = -1;
        double max = -1;
        for(auto& t : timings) {
            avg += t;
            var += t*t;
            if(min < 0 || min > t) min = t;
            if(max < 0 || max < t) max = t;
        }
        avg /= timings.size();
        var /= timings.size();
        var -= avg*avg;
        std::cout << "----- TIMING (milliseconds) ------" << std::endl;
        std::cout << "AVERAGE  : " << avg << std::endl;
        std::cout << "VARIANCE : " << var << std::endl;
        std::cout << "MAXIMUM  : " << max << std::endl;
        std::cout << "MINIMUM  : " << min << std::endl;
    }

    margo_finalize(mid);
    return 0;
}

static options parse_arguments(int argc, char** argv) {
    options opt;
    try {
        TCLAP::CmdLine cmd("Yokan Benchmark", ' ', "0.1");
        TCLAP::ValueArg<std::string> operationArg(
            "o", "operation", "Operation to benchmark (e.g. \"store\")", true, "", "string");
        TCLAP::ValueArg<range> keySizesArg(
            "k", "key-sizes", "Range of key sizes (e.g. \"32,64\")", true, range(), "range");
        TCLAP::ValueArg<range> valSizesArg(
            "v", "value-sizes", "Range of value sizes (e.g. \"32,64\")", true, range(), "range");
        TCLAP::ValueArg<unsigned> numItemsArg(
            "n", "num-items", "Number of items", true, 0, "integer");
        TCLAP::ValueArg<unsigned> seedArg(
            "s", "seed", "RNG seed", false, 1234, "integer");
        TCLAP::ValueArg<unsigned> repetitionsArg(
            "r", "repetitions", "Number of repetitions of the benchmark", false, 1, "integer");
        TCLAP::ValueArg<std::string> databaseIdArg(
            "d", "database-id", "Database id", true, "", "uuid");
        TCLAP::ValueArg<std::string> margoConfigArg(
            "m", "margo-config", "Margo JSON configuration file", false, "", "filename");
        TCLAP::ValueArg<std::string> serverAddressArg(
            "a", "server-address", "Address of the server", true, "", "string");
        TCLAP::ValueArg<uint16_t> providerIdArg(
            "p", "provider-id", "Id of the Yokan provider", false, 0, "integer");
        TCLAP::ValueArg<std::string> prefixArg(
            "", "prefix", "Prefix to use for some of the keys", false, "", "string");
        TCLAP::ValueArg<uint16_t> prefixFreqArg(
            "", "prefix-freq", "Persentage of appearance of the prefix", false, 50, "integer");
        TCLAP::ValueArg<unsigned> batchSizeArg(
            "b", "batch-size", "Batch size for operations that acces multiple items", false, 0, "integer");
        TCLAP::SwitchArg noRemoveArg(
            "", "no-remove", "Do not remove stored key/value on teardown");

        cmd.add(operationArg);
        cmd.add(keySizesArg);
        cmd.add(valSizesArg);
        cmd.add(numItemsArg);
        cmd.add(seedArg);
        cmd.add(repetitionsArg);
        cmd.add(databaseIdArg);
        cmd.add(margoConfigArg);
        cmd.add(serverAddressArg);
        cmd.add(providerIdArg);
        cmd.add(prefixArg);
        cmd.add(prefixFreqArg);
        cmd.add(batchSizeArg);
        cmd.add(noRemoveArg);

        cmd.parse(argc, argv);

        opt.operation      = operationArg.getValue();
        opt.key_sizes      = keySizesArg.getValue();
        opt.val_sizes      = valSizesArg.getValue();
        opt.num_items      = numItemsArg.getValue();
        opt.seed           = seedArg.getValue();
        opt.repetitions    = repetitionsArg.getValue();
        opt.database_id    = databaseIdArg.getValue();
        opt.margo_config   = margoConfigArg.getValue();
        opt.server_address = serverAddressArg.getValue();
        opt.provider_id    = providerIdArg.getValue();
        opt.prefix         = prefixArg.getValue();
        opt.prefix_freq    = prefixFreqArg.getValue();
        opt.batch_size     = batchSizeArg.getValue();
        opt.no_remove      = noRemoveArg.getValue();

        if(opt.prefix_freq > 100) {
            throw TCLAP::ArgException(
                "Value should be between 0 and 100",
                "prefix-freq", "Invalid value");
        }
        if(opt.database_id.size() != 36) {
            throw TCLAP::ArgException(
                "Invalid UUID",
                "database-id",
                "Invalid value");
        }

    } catch(TCLAP::ArgException &e) {
        std::cerr << e.what() << std::endl;
        exit(-1);
    }
    return opt;
}
