/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include "yokan/cxx/server.hpp"
#include <bedrock/AbstractComponent.hpp>

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
