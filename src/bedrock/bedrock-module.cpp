/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include "yokan/cxx/server.hpp"
#include <bedrock/AbstractComponent.hpp>
#include <nlohmann/json.hpp>

namespace tl = thallium;

class YokanComponent : public bedrock::AbstractComponent {

    std::unique_ptr<yokan::Provider> m_provider;

    public:

    YokanComponent(const tl::engine& engine,
                   uint16_t  provider_id,
                   const std::string& config,
                   const yk_provider_args* args)
    : m_provider{
        std::make_unique<yokan::Provider>(
            engine.get_margo_instance(), provider_id, config.c_str(), args)}
    {}

    void* getHandle() override {
        return static_cast<void*>(m_provider.get());
    }

    std::string getConfig() override {
        return m_provider->getConfig();
    }

    void snapshot(const std::string& dest_path,
                  const std::string& options_json,
                  bool remove_source) override {
        (void)options_json; // reserved for future per-backend knobs
        yk_snapshot_options opts = { /* extra_config */ nullptr,
                                     /* xfer_size    */ 0 };
        if(!options_json.empty()) opts.extra_config = options_json.c_str();
        auto ret = yk_provider_snapshot_database(
            m_provider->handle(), dest_path.c_str(), remove_source, &opts);
        if(ret != YOKAN_SUCCESS) {
            throw bedrock::Exception{
                std::string{"yk_provider_snapshot_database failed: "}
                + std::to_string(static_cast<int>(ret))};
        }
    }

    void restore(const std::string& src_path,
                 const char* options_json) override {
        // options_json may carry: { "new_root": "...", "extra_config": {...} }
        std::string new_root;
        std::string extra_config;
        if(options_json && *options_json) {
            try {
                auto j = nlohmann::json::parse(options_json);
                if(j.contains("new_root") && j["new_root"].is_string())
                    new_root = j["new_root"].get<std::string>();
                if(j.contains("extra_config")) {
                    extra_config = j["extra_config"].is_string()
                        ? j["extra_config"].get<std::string>()
                        : j["extra_config"].dump();
                }
            } catch(const std::exception& e) {
                throw bedrock::Exception{
                    std::string{"restore: invalid options_json: "} + e.what()};
            }
        }
        yk_restore_options opts = {
            /* new_root     */ new_root.empty() ? nullptr : new_root.c_str(),
            /* extra_config */ extra_config.empty() ? nullptr : extra_config.c_str(),
            /* xfer_size    */ 0
        };
        auto ret = yk_provider_restore_database(
            m_provider->handle(), src_path.c_str(), &opts);
        if(ret != YOKAN_SUCCESS) {
            throw bedrock::Exception{
                std::string{"yk_provider_restore_database failed: "}
                + std::to_string(static_cast<int>(ret))};
        }
    }

    static std::shared_ptr<bedrock::AbstractComponent>
        Register(const bedrock::ComponentArgs& args) {
            tl::pool pool;
            auto it = args.dependencies.find("pool");
            if(it != args.dependencies.end() && !it->second.empty()) {
                pool = it->second[0]->getHandle<tl::pool>();
            }
            remi_client_t remi_sender = nullptr;
            it = args.dependencies.find("remi_sender");
            if(it != args.dependencies.end() && !it->second.empty()) {
                auto component = it->second[0]->getHandle<bedrock::ComponentPtr>();
                remi_sender = static_cast<remi_client_t>(component->getHandle());
            }
            remi_provider_t remi_receiver = nullptr;
            it = args.dependencies.find("remi_receiver");
            if(it != args.dependencies.end() && !it->second.empty()) {
                auto component = it->second[0]->getHandle<bedrock::ComponentPtr>();
                remi_receiver = static_cast<remi_provider_t>(component->getHandle());
            }
            yk_provider_args yk_args = {
                /* .pool = */ pool.native_handle(),
                /* .cache = */ nullptr,
                /* .remi = */ {
                    /* .client = */ remi_sender,
                    /* .provider = */ remi_receiver
                }
            };
            return std::make_shared<YokanComponent>(
                args.engine, args.provider_id, args.config, &yk_args);
        }

    static std::vector<bedrock::Dependency>
        GetDependencies(const bedrock::ComponentArgs& args) {
            (void)args;
            std::vector<bedrock::Dependency> dependencies{
                bedrock::Dependency{
                    /* name */ "pool",
                    /* type */ "pool",
                    /* is_required */ false,
                    /* is_array */ false,
                    /* is_updatable */ false
                },
                bedrock::Dependency{
                    /* name */ "remi_sender",
                    /* type */ "remi_sender",
                    /* is_required */ false,
                    /* is_array */ false,
                    /* is_updatable */ false
                },
                bedrock::Dependency{
                    /* name */ "remi_receiver",
                    /* type */ "remi_receiver",
                    /* is_required */ false,
                    /* is_array */ false,
                    /* is_updatable */ false
                }
            };
            return dependencies;
        }
};

BEDROCK_REGISTER_COMPONENT_TYPE(yokan, YokanComponent)
