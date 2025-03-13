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

#include <chimaera/chimaera_types.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "hermes/bucket.h"
#include "hermes/hermes.h"

namespace py = pybind11;

using hermes::BlobId;
using hermes::BlobInfo;
using hermes::BucketId;
using hermes::BufferInfo;
using hermes::Hermes;
using hermes::IoStat;
using hermes::IoType;
using hermes::TagId;
using hermes::TagInfo;
using hermes::TargetId;
using hermes::TargetStats;

template <typename UniqueT>
void BindUniqueId(py::module &m, const std::string &name) {
  py::class_<UniqueT>(m, name.c_str())
      .def(py::init<>())
      .def(py::init<u32, u64>(), py::arg("node_id"), py::arg("unique"))
      .def(py::init<u32, u32, u64>(), py::arg("node_id"), py::arg("hash"),
           py::arg("unique"))
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
      .def_readwrite("off", &BufferInfo::off_)
      .def_readwrite("size", &BufferInfo::size_);
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

void BinIoType(py::module &m) {
  py::enum_<IoType>(m, "IoType")
      .value("kRead", IoType::kRead)
      .value("kWrite", IoType::kWrite)
      .value("kNone", IoType::kNone)
      .export_values();
}

void BindIoStat(py::module &m) {
  py::class_<IoStat>(m, "IoStat")
      .def(py::init<>())
      .def(py::init<IoType, const BlobId &, const TagId &, size_t, int>(),
           py::arg("type"), py::arg("blob_id"), py::arg("tag_id"),
           py::arg("blob_size"), py::arg("rank"))
      .def_readonly("id", &IoStat::id_)
      .def_readonly("type", &IoStat::type_)
      .def_readonly("blob_id", &IoStat::blob_id_)
      .def_readonly("tag_id", &IoStat::tag_id_)
      .def_readonly("blob_size", &IoStat::blob_size_)
      .def_readonly("rank", &IoStat::rank_);
}

void BindHermes(py::module &m) {
  py::class_<Hermes>(m, "Hermes")
      .def(py::init<>())
      .def("ClientInit", &Hermes::ClientInit)
      .def("IsInitialized", &Hermes::IsInitialized)
      .def("GetTagId", &Hermes::GetTagId)
      .def("PollTargetMetadata", &Hermes::PollTargetMetadata)
      .def("PollTagMetadata", &Hermes::PollTagMetadata)
      .def("PollBlobMetadata", &Hermes::PollBlobMetadata)
      .def("PollAccessPattern", &Hermes::PollAccessPattern);
  m.def("TRANSPARENT_HERMES", &HERMES_INIT);
  m.def("HERMES_INIT", &HERMES_INIT);
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
  BindHermes(m);
}
