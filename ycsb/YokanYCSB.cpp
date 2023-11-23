/**
 * (C) The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <YCSBCppInterface.hpp>

#include <yokan/cxx/client.hpp>
#include <yokan/cxx/database.hpp>

#include <algorithm>
#include <map>
#include <unordered_map>
#include <string>
#include <iostream>
#include <atomic>

namespace yokan {

class YokanDB : public ycsb::DB {

    struct Settings {
        std::string protocol;
        std::string provider_address;
        uint16_t    provider_id = 0;
        bool        use_progress_thread = false;
    };

    margo_instance_id m_mid = MARGO_INSTANCE_NULL;
    yokan::Client     m_client;
    yokan::Database   m_db;

    mutable struct {
        size_t num_samples  = 0;
        double avg_key_size = 0.0;
        size_t max_key_size = 0;
        double avg_val_size = 0.0;
        size_t max_val_size = 0;
    } m_stats;

    public:

#define CHECK_MISSING_PROPERTY(__name__)           \
    do {                                           \
        if(it == properties.end()) {               \
            std::cerr << "[ERROR] Missing "        \
               __name__ " property" << std::endl; \
            return nullptr;                        \
        }                                          \
    } while(0)


    static ycsb::DB* New(const ycsb::Properties& properties) {
        Settings                   settings;
        ycsb::Properties::const_iterator it;

        it = properties.find("yokan.protocol");
        CHECK_MISSING_PROPERTY("yokan.protocol");
        settings.protocol = it->second;

        it = properties.find("yokan.provider.address");
        CHECK_MISSING_PROPERTY("yokan.provider.address");
        settings.provider_address = it->second;

        it = properties.find("yokan.provider.id");
        try {
            if(it != properties.end())
                settings.provider_id = std::stoi(it->second);
        } catch(const std::invalid_argument&) {
            std::cerr << "[ERROR] Failed to parse yokan.provider.id "
                "property as an integer" << std::endl;;
            return nullptr;
        }

        it = properties.find("yokan.use_progress_thread");
        if(it != properties.end()) {
            if(it->second == "true")
                settings.use_progress_thread = true;
            else if(it->second == "false")
                settings.use_progress_thread = false;
            else
                std::cerr << "[WARNING] yokan.use_progress_thread property "
                    "should be true or false, defaulting to false" << std::endl;
        }

        margo_instance_id mid = margo_init(
            settings.protocol.c_str(), MARGO_SERVER_MODE,
            settings.use_progress_thread, 0);
        if(mid == MARGO_INSTANCE_NULL) {
            std::cerr << "[ERROR] Could not initialize margo with protocol "
                << settings.protocol << std::endl;
            return nullptr;
        }

        hg_addr_t   addr = HG_ADDR_NULL;
        hg_return_t hret = margo_addr_lookup(
            mid, settings.provider_address.c_str(), &addr);
        if(hret  != HG_SUCCESS) {
            std::cerr << "[ERROR] Could not lookup address "
                << settings.provider_address << std::endl;
            margo_finalize(mid);
            return nullptr;
        }

        yokan::Client client = yokan::Client(mid);
        yokan::Database db;
        try {
            db = client.makeDatabaseHandle(addr, settings.provider_id);
        } catch(const yokan::Exception& ex) {
            std::cerr << "[ERROR] " << ex.what() << std::endl;
            margo_addr_free(mid, addr);
            margo_finalize(mid);
        }

        margo_addr_free(mid, addr);
        return new YokanDB(mid, std::move(client), std::move(db));
    }

    YokanDB(margo_instance_id mid, yokan::Client&& client,
            yokan::Database&& db)
    : m_mid(mid)
    , m_client(std::move(client))
    , m_db(std::move(db))
    {}

    ~YokanDB() {
        margo_finalize(m_mid);
    }

    ycsb::Status read(ycsb::StringView table,
                      ycsb::StringView key,
                      const std::unordered_set<ycsb::StringView>& fields,
                      ycsb::DB::Record& result) const override {
        (void)table;
        try {
            size_t record_length = m_db.length(key.data(), key.size());

            std::vector<char> serialized(record_length);
            m_db.get(key.data(), key.size(), serialized.data(), &record_length);

            deserializeRecord<ycsb::StringBuffer>(
                ycsb::StringView(serialized.data(), serialized.size()),
                result, &fields);

            m_stats.max_key_size = std::max(m_stats.max_key_size, key.size());
            m_stats.max_val_size = std::max(m_stats.max_val_size, record_length);
            double r1 = (double)m_stats.num_samples / ((double)m_stats.num_samples+1.0);
            double r2 = 1.0 / ((double)m_stats.num_samples+1.0);
            m_stats.avg_key_size = r1*m_stats.avg_key_size + r2*key.size();
            m_stats.avg_val_size = r1*m_stats.avg_val_size + r2*record_length;

        } catch(const yokan::Exception& ex) {
            return ycsb::Status("yokan::Exception", ex.what());
        }
        return ycsb::Status::OK();
    }

    ycsb::Status read(ycsb::StringView table,
                      ycsb::StringView key,
                      ycsb::DB::Record& result) const override {
        (void)table;
        try {
            size_t record_length = m_db.length(key.data(), key.size());

            std::vector<char> serialized(record_length);
            m_db.get(key.data(), key.size(), serialized.data(), &record_length);

            deserializeRecord<ycsb::StringBuffer>(
                ycsb::StringView(serialized.data(), serialized.size()),
                result);

            m_stats.max_key_size = std::max(m_stats.max_key_size, key.size());
            m_stats.max_val_size = std::max(m_stats.max_val_size, record_length);
            double r1 = (double)m_stats.num_samples / ((double)m_stats.num_samples+1.0);
            double r2 = 1.0 / ((double)m_stats.num_samples+1.0);
            m_stats.avg_key_size = r1*m_stats.avg_key_size + r2*key.size();
            m_stats.avg_val_size = r1*m_stats.avg_val_size + r2*record_length;

        } catch(const yokan::Exception& ex) {
            return ycsb::Status("yokan::Exception", ex.what());
        }
        return ycsb::Status::OK();
    }

    ycsb::Status _scan(ycsb::StringView table,
                       ycsb::StringView startKey,
                       int recordCount,
                       std::vector<ycsb::DB::Record>& result,
                       const std::unordered_set<ycsb::StringView>* fields = nullptr) const {
        (void)table;

        size_t estimated_val_size = 2048;
        size_t estimated_key_size = 2048;

        std::string startKeyStr = static_cast<std::string>(startKey);
        int32_t mode            = YOKAN_MODE_INCLUSIVE;

        try {
            while(recordCount != 0) {

                size_t vals_buffer_size = estimated_val_size*recordCount;
                size_t keys_buffer_size = estimated_key_size*recordCount;

                std::vector<char>   keys_buffer(keys_buffer_size);
                std::vector<size_t> keys_sizes(recordCount, 0);
                std::vector<char>   vals_buffer(vals_buffer_size);
                std::vector<size_t> vals_sizes(recordCount, 0);

                m_db.listKeyValsPacked(
                    startKeyStr.data(), startKeyStr.size(),
                    nullptr, 0,
                    recordCount,
                    keys_buffer.data(),
                    keys_buffer_size,
                    keys_sizes.data(),
                    vals_buffer.data(),
                    vals_buffer_size,
                    vals_sizes.data(),
                    mode);

                if(vals_sizes[0] == YOKAN_SIZE_TOO_SMALL) {
                    estimated_val_size *= 2;
                    continue;
                }

                if(keys_sizes[0] == YOKAN_SIZE_TOO_SMALL) {
                    estimated_key_size *= 2;
                    continue;
                }

                size_t recordsRead = 0;
                size_t val_offset  = 0;
                size_t key_offset  = 0;

                auto last_key = ycsb::StringView(keys_buffer.data(), keys_sizes[0]);
                auto last_val = ycsb::StringView(vals_buffer.data(), vals_sizes[0]);

                for(size_t i = 0; i < (size_t)recordCount; i++) {
                    if(keys_sizes[i] == YOKAN_NO_MORE_KEYS)
                        return ycsb::Status::OK();

                    if(vals_sizes[i] == YOKAN_SIZE_TOO_SMALL
                    || keys_sizes[i] == YOKAN_SIZE_TOO_SMALL) {
                        break;
                    }

                    last_key = ycsb::StringView(keys_buffer.data()+key_offset, keys_sizes[i]);
                    key_offset += keys_sizes[i];
                    last_val = ycsb::StringView(vals_buffer.data()+val_offset, vals_sizes[i]);
                    val_offset += vals_sizes[i];

                    DB::Record record;
                    deserializeRecord<ycsb::StringBuffer>(last_val, record, fields);

                    result.push_back(std::move(record));
                    recordsRead += 1;
                }

                recordCount -= recordsRead;
                if(recordCount != 0) startKeyStr.assign(last_key.data(), last_key.size());
                mode = YOKAN_MODE_DEFAULT;
            }

        } catch(const yokan::Exception& ex) {
            return ycsb::Status("yokan::Exception", ex.what());
        }
        return ycsb::Status::OK();
    }

    ycsb::Status scan(ycsb::StringView table,
                      ycsb::StringView startKey,
                      int recordCount,
                      const std::unordered_set<ycsb::StringView>& fields,
                      std::vector<ycsb::DB::Record>& result) const override {
        return _scan(table, startKey, recordCount, result, &fields);
    }

    ycsb::Status scan(ycsb::StringView table,
                      ycsb::StringView startKey,
                      int recordCount,
                      std::vector<ycsb::DB::Record>& result) const override {
        return _scan(table, startKey, recordCount, result);
    }

    ycsb::Status update(ycsb::StringView table,
                        ycsb::StringView key,
                        const RecordView& recordUpdate) override {
        (void)table;
        ycsb::DB::Record existingRecord;
        ycsb::DB::RecordView newRecord = recordUpdate;
        auto status = read(table, key, existingRecord);
        if(status.name != "OK") return status;
        for(auto& p : existingRecord) {
            auto sw = ycsb::StringView(p.second->data(), p.second->size());
            // emplace does not do anything if the field already exists
            newRecord.emplace(ycsb::StringView(p.first.data(), p.first.size()),
                              ycsb::StringView(p.second->data(), p.second->size()));
        }
        return insert(table, key, newRecord);
    }

    ycsb::Status insert(ycsb::StringView table,
                        ycsb::StringView key,
                        const RecordView& record) override {
        (void)table;
        auto serialized_record = serializeRecord(record);
        try {
            m_db.put(key.data(), key.size(),
                 serialized_record.data(), serialized_record.size());
        } catch(const yokan::Exception& ex) {
            return ycsb::Status("yokan::Exception", ex.what());
        }
        return ycsb::Status::OK();
    }

    ycsb::Status erase(ycsb::StringView table,
                       ycsb::StringView key) override {
        (void)table;
        try {
            m_db.erase(key.data(), key.size());
        } catch(const yokan::Exception& ex) {
            return ycsb::Status("yokan::Exception", ex.what());
        }
        return ycsb::Status::OK();
    }

    private:

    static std::vector<char> serializeRecord(const RecordView& record) {
        std::vector<char> result;
        size_t required_size = 0;
        for(auto& p : record) {
            required_size += 2*sizeof(size_t) + p.first.size() + p.second.size();
        }
        result.resize(required_size);
        char* ptr = result.data();
        size_t s;
        for(auto& p : record) {
            s = p.first.size();
            std::memcpy(ptr, &s, sizeof(s));
            ptr += sizeof(s);

            std::memcpy(ptr, p.first.data(), s);
            ptr += s;

            s = p.second.size();
            std::memcpy(ptr, &s, sizeof(s));
            ptr += sizeof(s);

            std::memcpy(ptr, p.second.data(), s);
            ptr += s;
        }
        return result;
    }

    template<typename BufferType>
    static void deserializeRecord(
            const ycsb::StringView& serialized,
            ycsb::DB::Record& record,
            const std::unordered_set<ycsb::StringView>* fields = nullptr) {
        size_t remaining_size = serialized.size();
        const char* ptr = serialized.data();
        size_t field_size, value_size;
        const char* field;
        const char* value;
        while(remaining_size > 0) {

            if(remaining_size < sizeof(field_size)) break;
            std::memcpy(&field_size, ptr, sizeof(field_size));
            ptr += sizeof(field_size);
            remaining_size -= sizeof(field_size);

            if(remaining_size < field_size) break;
            field = ptr;
            ptr += field_size;
            remaining_size -= field_size;

            if(remaining_size < sizeof(value_size)) break;
            std::memcpy(&value_size, ptr, sizeof(value_size));
            ptr += sizeof(value_size);
            remaining_size -= sizeof(value_size);

            if(remaining_size < value_size) break;
            value = ptr;
            ptr += value_size;
            remaining_size -= value_size;

            if(fields && !fields->count(ycsb::StringView(field, field_size)))
                continue;

            record.emplace(
                std::string(field, field_size),
                std::make_unique<BufferType>(value, value_size));
        }
    }

};

YCSB_CPP_REGISTER_DB_TYPE(yokan, YokanDB);

}
