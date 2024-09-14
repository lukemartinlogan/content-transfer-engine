//
// Created by lukemartinlogan on 9/30/23.
//

#ifndef HERMES_TASKS_DATA_STAGER_SRC_ABSTRACT_STAGER_H_
#define HERMES_TASKS_DATA_STAGER_SRC_ABSTRACT_STAGER_H_

#include "hermes/hermes.h"

namespace hermes {

class AbstractStager {
 public:
  std::string path_;
  std::string params_;

  AbstractStager() = default;
  ~AbstractStager() = default;

  virtual void RegisterStager(const std::string &tag_name,
                              const std::string &params) = 0;
  virtual void StageIn(hermes::Client &client,
                       const TagId &tag_id,
                       const std::string &blob_name,
                       float score) = 0;
  virtual void StageOut(hermes::Client &client,
                        const TagId &tag_id,
                        const std::string &blob_name,
                        hipc::Pointer &data_p,
                        size_t data_size) = 0;
  virtual void UpdateSize(hermes::Client &client,
                          const TagId &tag_id,
                          const std::string &blob_name,
                          size_t blob_off,
                          size_t data_size) = 0;
};

}  // namespace hermes

#endif  // HERMES_TASKS_DATA_STAGER_SRC_ABSTRACT_STAGER_H_
