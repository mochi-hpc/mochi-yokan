/**
 * (C) The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <YCSBCppInterface.hpp>

#include <yokan/cxx/client.hpp>
#include <yokan/cxx/database.hpp>

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
        std::string database_name;
        bool        use_progress_thread = false;
    };

    margo_instance_id m_mid = MARGO_INSTANCE_NULL;
    yokan::Client     m_client;
    yokan::Database   m_db;

    public:

#define CHECK_MISSING_PROPERTY(__name__)           \
    do {                                           \
        if(it == properties.end()) {               \
            std::cerr << "[ERROR] Missing "        \
               __name__ "  property" << std::endl; \
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

        it = properties.find("yokan.database.name");
        CHECK_MISSING_PROPERTY("yokan.database.name");
        settings.database_name = it->second;

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
            db = client.findDatabaseByName(
                addr, settings.provider_id, settings.database_name.c_str());
        } catch(const yokan::Exception& ex) {
            std::cerr << "[ERROR] " << ex.what() << std::endl;
            margo_addr_free(mid, addr);
            margo_finalize(mid);
        }

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
                      const std::vector<ycsb::StringView>& fields,
                      ycsb::DB::Record& result) const override {
        // TODO
        (void)table;
        (void)key;
        (void)fields;
        (void)result;
        return ycsb::Status::OK();
    }

    ycsb::Status read(ycsb::StringView table,
                      ycsb::StringView key,
                      ycsb::DB::Record& result) const override {
        // TODO
        (void)table;
        (void)key;
        (void)result;
        return ycsb::Status::OK();
    }

    ycsb::Status scan(ycsb::StringView table,
                      ycsb::StringView startKey,
                      int recordCount,
                      const std::vector<ycsb::StringView>& fields,
                      std::vector<ycsb::DB::Record>& result) const override {
        // TODO
        (void)table;
        (void)startKey;
        (void)recordCount;
        (void)fields;
        (void)result;
        return ycsb::Status::OK();
    }

    ycsb::Status scan(ycsb::StringView table,
                      ycsb::StringView startKey,
                      int recordCount,
                      std::vector<ycsb::DB::Record>& result) const override {
        // TODO
        (void)table;
        (void)startKey;
        (void)recordCount;
        (void)result;
        return ycsb::Status::OK();
    }

    ycsb::Status update(ycsb::StringView table,
                        ycsb::StringView key,
                        const std::vector<ycsb::StringView>& fields,
                        const std::vector<ycsb::StringView>& values) override {
        // TODO
        (void)table;
        (void)key;
        (void)fields;
        (void)values;
        return ycsb::Status::OK();
    }

    ycsb::Status insert(ycsb::StringView table,
                        ycsb::StringView key,
                        const std::vector<ycsb::StringView>& fields,
                        const std::vector<ycsb::StringView>& values) override {
        // TODO
        (void)table;
        (void)key;
        (void)fields;
        (void)values;
        return ycsb::Status::OK();
    }

    ycsb::Status erase(ycsb::StringView table,
                       ycsb::StringView key) override {
        // TODO
        (void)table;
        (void)key;
        return ycsb::Status::OK();
    }

};

YCSB_CPP_REGISTER_DB_TYPE(yokan, YokanDB);

}
