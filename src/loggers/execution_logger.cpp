#include "loggers/execution_logger.h"
#include <memory>
#include "loggers/main_logger.h"

namespace terrier::execution {

std::shared_ptr<spdlog::logger> execution_logger;

void init_execution_logger() {
  execution_logger = std::make_shared<spdlog::logger>("execution_logger", ::default_sink);
  spdlog::register_logger(execution_logger);
}

}  // namespace terrier::execution