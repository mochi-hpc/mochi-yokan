#include "yokan/cxx/admin.hpp"
#include "yokan/cxx/common.hpp"
#include <tclap/CmdLine.h>
#include <iostream>
#include <memory>
#include <fstream>
#include <string>

struct options {
    std::string command;
    std::string address;
    std::string token;
    uint16_t    provider_id;
    std::string database_id;
    std::string backend_type;
    std::string config;
};

static void parse_arguments(options& opt, int argc, char** argv);

int main(int argc, char** argv) {
    if(argc < 2) {
        std::cerr << "Usage: " << argv[0] << " {open|close|destroy|list} ..." << std::endl;
        exit(-1);
    }
    std::string cmd = argv[1];
    for(int i=1; i < argc-1; i++)
        argv[i] = argv[i+1];
    argc -= 1;

    options opt;
    opt.command = cmd;

    if(cmd != "open"
    && cmd != "close"
    && cmd != "destroy"
    && cmd != "list") {
        std::cerr << "Unknown command \"" << cmd << "\"" << std::endl;
        std::cerr << "Usage: " << argv[0] << " {open|create|close|destroy|list} ..." << std::endl;
        exit(-1);
    }

    parse_arguments(opt, argc, argv);

    std::string protocol;
    {
        auto p = opt.address.find(":");
        protocol = opt.address.substr(0, p);
    }

    std::string db_config = "{}";
    if(!opt.config.empty()) {
        std::ifstream t(opt.config.c_str());
        if(!t.good()) {
            std::cerr << "ERROR: file " << opt.config << " does not exist" << std::endl;
            exit(-1);
        }
        std::stringstream buffer;
        buffer << t.rdbuf();
        db_config = buffer.str();
    }

    margo_instance_id mid = margo_init_ext(protocol.c_str(),
                                           MARGO_CLIENT_MODE,
                                           nullptr);

    if(!mid) {
        std::cerr << "ERROR: could not initialize margo instance" << std::endl;
        exit(-1);
    }

    hg_addr_t svr_addr = HG_ADDR_NULL;
    hg_return_t hret = margo_addr_lookup(mid, opt.address.c_str(), &svr_addr);
    if(hret != HG_SUCCESS) {
        std::cerr << "ERROR: could not lookup address " << opt.address << std::endl;
        margo_finalize(mid);
        exit(-1);
    }

    {
        auto admin = std::make_shared<yokan::Admin>(mid);
        if(cmd == "open") {
            auto db_id = admin->openDatabase(svr_addr,
                opt.provider_id, opt.token.c_str(),
                opt.backend_type.c_str(),
                db_config.c_str());
            std::cout << "Created database " << db_id << std::endl;
        } else if(cmd == "close") {
            yk_database_id_t db_id;
            if(opt.database_id.size() != 36) {
                std::cerr << "ERROR: invalid database id " << opt.database_id << std::endl;
                exit(-1);
            }
            yk_database_id_from_string(opt.database_id.c_str(), &db_id);
            admin->closeDatabase(svr_addr,
                opt.provider_id, opt.token.c_str(),
                db_id);
            std::cout << "Closed database " << db_id << std::endl;
        } else if(cmd == "destroy") {
            yk_database_id_t db_id;
            if(opt.database_id.size() != 36) {
                std::cerr << "ERROR: invalid database id " << opt.database_id << std::endl;
                exit(-1);
            }
            yk_database_id_from_string(opt.database_id.c_str(), &db_id);
            admin->destroyDatabase(svr_addr,
                opt.provider_id, opt.token.c_str(),
                db_id);
            std::cout << "Destroyed database " << db_id << std::endl;
        } else if(cmd == "list") {
            auto db_ids = admin->listDatabases(svr_addr, opt.provider_id, opt.token.c_str());
            for(auto& db_id : db_ids) {
                std::cout << db_id << std::endl;
            }
        }
    }

    margo_finalize(mid);

    return 0;
}

static void parse_arguments(options& opt, int argc, char** argv) {
    try {
        TCLAP::CmdLine cmd("Yokan Admin", ' ', "0.1");

        TCLAP::ValueArg<std::string> address(
            "a", "address", "Server address", true, "", "string");
        TCLAP::ValueArg<std::string> token(
            "t", "token", "Security token", false, "", "string");
        TCLAP::ValueArg<std::string> config(
            "c", "config", "JSON configuration file for the database", false, "", "string");
        TCLAP::ValueArg<uint16_t> providerId(
            "p", "provider-id", "Provider id", false, 0, "string");
        TCLAP::ValueArg<std::string> backendType(
            "b", "backend-type", "Database backend type",
            (opt.command == "open"),
            "", "string");
        TCLAP::ValueArg<std::string> databaseId(
            "d", "database-id", "Database id",
            (opt.command == "close" || opt.command == "destroy"),
            "", "string");

        cmd.add(address);
        cmd.add(token);
        cmd.add(config);
        cmd.add(providerId);
        cmd.add(backendType);
        cmd.add(databaseId);

        cmd.parse(argc, argv);

        opt.address      = address.getValue();
        opt.token        = token.getValue();
        opt.provider_id  = providerId.getValue();
        opt.backend_type = backendType.getValue();
        opt.database_id  = databaseId.getValue();
        opt.config       = config.getValue();

    } catch(TCLAP::ArgException &e) {
        std::cerr << e.what() << std::endl;
        exit(-1);
    }
}
