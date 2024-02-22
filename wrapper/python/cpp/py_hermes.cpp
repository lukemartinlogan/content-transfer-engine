/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <hrun/hrun_types.h>
#include "hermes/hermes.h"
#include "hermes/bucket.h"

namespace py = pybind11;

using hermes::BlobId;
using hermes::BucketId;
using hermes::TagId;
using hermes::TargetId;
using hermes::BufferInfo;
using hermes::BlobInfo;
using hermes::TargetStats;
using hermes::TagInfo;
using hermes::MetadataTable;
using hermes::Hermes;
using hermes::Context;
using hermes::AccessInfo;
using hrun::UniqueId;

bool TRANSPARENT_HERMES_FUN() {
  if (TRANSPARENT_HRUN()) {
    HERMES_CONF->ClientInit();
    return true;
  }
  return false;
}

template<typename UniqueT>
void BindUniqueId(py::module &m, const std::string &name) {
  py::class_<UniqueT>(m, name.c_str())
      .def(py::init<>())
      .def(py::init<u32, u64>(), py::arg("node_id"), py::arg("unique"))
      .def(py::init<u32, u32, u64>(), py::arg("node_id"), py::arg("hash"), py::arg("unique"))
      .def("IsNull", &UniqueT::IsNull)
      .def("GetNull", &UniqueT::GetNull)
      .def("SetNull", &UniqueT::SetNull)
      .def("GetNodeId", &UniqueT::GetNodeId)
      .def("__eq__", &UniqueT::operator==)
      .def("__ne__", &UniqueT::operator!=)
      .def_readonly("node_id", &UniqueT::node_id_)
      .def_readonly("hash", &UniqueT::hash_)
      .def_readonly("unique", &UniqueT::unique_);
}

void BindBufferInfo(py::module &m) {
  py::class_<BufferInfo>(m, "BufferInfo")
      .def(py::init<>())
      .def_readwrite("tid", &BufferInfo::tid_)
      .def_readwrite("t_slab", &BufferInfo::t_slab_)
      .def_readwrite("t_off", &BufferInfo::t_off_)
      .def_readwrite("t_size", &BufferInfo::t_size_);
}

void BindBlobInfo(py::module &m) {
  py::class_<BlobInfo>(m, "BlobInfo")
      .def(py::init<>())
      .def("UpdateWriteStats", &BlobInfo::UpdateWriteStats)
      .def("UpdateReadStats", &BlobInfo::UpdateReadStats)
      .def_readonly("tag_id", &BlobInfo::tag_id_)
      .def_readonly("blob_id", &BlobInfo::blob_id_)
      .def("get_name", &BlobInfo::GetName)
      .def_readonly("buffers", &BlobInfo::buffers_)
      .def_readonly("tags", &BlobInfo::tags_)
      .def_readonly("blob_size", &BlobInfo::blob_size_)
      .def_readonly("max_blob_size", &BlobInfo::max_blob_size_)
      .def_readonly("score", &BlobInfo::score_)
      .def_readonly("access_freq", &BlobInfo::access_freq_)
      .def_readonly("last_access", &BlobInfo::last_access_)
      .def_readonly("mod_count", &BlobInfo::mod_count_);
}

void BindTargetStats(py::module &m) {
  py::class_<TargetStats>(m, "TargetStats")
      .def(py::init<>())
      .def_readonly("tgt_id", &TargetStats::tgt_id_)
      .def_readonly("node_id", &TargetStats::node_id_)
      .def_readonly("rem_cap", &TargetStats::rem_cap_)
      .def_readonly("max_cap", &TargetStats::max_cap_)
      .def_readonly("bandwidth", &TargetStats::bandwidth_)
      .def_readonly("latency", &TargetStats::latency_)
      .def_readonly("score", &TargetStats::score_);
}

void BindTagInfo(py::module &m) {
  py::class_<TagInfo>(m, "TagInfo")
      .def(py::init<>())
      .def_readonly("tag_id", &TagInfo::tag_id_)
      .def("get_name", &TagInfo::GetName)
      .def_readonly("blobs", &TagInfo::blobs_)
      .def_readonly("traits", &TagInfo::traits_)
      .def_readonly("internal_size", &TagInfo::internal_size_)
      .def_readonly("page_size", &TagInfo::page_size_)
      .def_readonly("owner", &TagInfo::owner_);
}

void BindMetadataTable(py::module &m) {
  py::class_<MetadataTable>(m, "MetadataTable")
      .def(py::init<>())
      .def_readonly("blob_info", &MetadataTable::blob_info_)
      .def_readonly("target_info", &MetadataTable::target_info_)
      .def_readonly("bkt_info", &MetadataTable::bkt_info_);

  py::class_<AccessInfo>(m, "AccessInfo")
      .def(py::init<>())  // Default constructor
      .def_readwrite("tag_id_", &AccessInfo::tag_id_)
      .def_readwrite("blob_id_", &AccessInfo::blob_id_)
      .def_readwrite("score_", &AccessInfo::score_)
      .def_readwrite("blob_name_", &AccessInfo::blob_name_)
      .def_readwrite("acc_off_", &AccessInfo::acc_off_)
      .def_readwrite("acc_size_", &AccessInfo::acc_size_)
      .def_readwrite("blob_size_", &AccessInfo::blob_size_)
      .def_readwrite("access_type_", &AccessInfo::access_type_);
}

void BindHermes(py::module &m) {
  py::class_<Hermes>(m, "Hermes")
      .def(py::init<>())
      .def("ClientInit", &Hermes::ClientInit)
      .def("IsInitialized", &Hermes::IsInitialized)
      .def("GetTagId", &Hermes::GetTagId)
      .def("ParseAccessPattern", &Hermes::ParseAccessPattern)
      .def("CollectMetadataSnapshot", &Hermes::CollectMetadataSnapshot);
  m.def("TRANSPARENT_HERMES", &TRANSPARENT_HERMES_FUN);

  py::class_<hermes::Context>(m, "Context")
      .def(py::init<>())
      .def_readwrite("dpe", &hermes::Context::dpe_)
      .def_readwrite("blob_score", &hermes::Context::blob_score_)
      .def_readwrite("flags", &hermes::Context::flags_)
      .def_readwrite("bkt_params", &hermes::Context::bkt_params_)
      .def_readwrite("node_id", &hermes::Context::node_id_);

  py::class_<hermes::Bucket>(m, "Bucket")
      .def(py::init<const std::string&, size_t, u32>(), py::arg("bkt_name"), py::arg("backend_size") = 0, py::arg("flags") = 0)
      .def(py::init<const std::string&, hermes::Context&, size_t, u32>(), py::arg("bkt_name"), py::arg("ctx"), py::arg("backend_size") = 0, py::arg("flags") = 0)
      .def(py::init<TagId>())
      .def("reorganize_blob", py::overload_cast<const std::string&, float, const hermes::Context&>(&hermes::Bucket::ReorganizeBlob))
      .def("reorganize_blob", py::overload_cast<const BlobId&, float, const hermes::Context&>(&hermes::Bucket::ReorganizeBlob))
      .def("contains_blob", &hermes::Bucket::ContainsBlob)
      .def("rename_blob", &hermes::Bucket::RenameBlob)
      .def("destroy_blob", &hermes::Bucket::DestroyBlob)
      .def("get_contained_blob_ids", &hermes::Bucket::GetContainedBlobIds);
}

PYBIND11_MODULE(py_hermes, m) {
  BindUniqueId<BlobId>(m, "BlobId");
  BindUniqueId<BucketId>(m, "BucketId");
  BindUniqueId<TagId>(m, "TagId");
  BindUniqueId<TargetId>(m, "TargetId");
  BindBufferInfo(m);
  BindBlobInfo(m);
  BindTargetStats(m);
  BindTagInfo(m);
  BindMetadataTable(m);
  BindHermes(m);
}
