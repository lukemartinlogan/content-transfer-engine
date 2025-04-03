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

#ifndef HERMES_ADAPTER_POSIX_NATIVE_H_
#define HERMES_ADAPTER_POSIX_NATIVE_H_

#include <memory>

#include "hermes_adapters/filesystem/filesystem.h"
#include "hermes_adapters/filesystem/filesystem_mdm.h"
#include "posix_api.h"

namespace hermes::adapter {

/** A class to represent POSIX IO file system */
class PosixFs : public hermes::adapter::Filesystem {
 public:
  HERMES_POSIX_API_T real_api_; /**< pointer to real APIs */

 public:
  PosixFs() : Filesystem(AdapterType::kPosix) { real_api_ = HERMES_POSIX_API; }

  template <typename StatT>
  int Stat(File &f, StatT *buf) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto existing = mdm->Find(f);
    if (existing) {
      AdapterStat &astat = *existing;
      /*memset(buf, 0, sizeof(StatT));
      buf->st_dev = 0;
      buf->st_ino = 0;*/
      buf->st_mode = 0100644;
      /*buf->st_nlink = 1;*/
      buf->st_uid = HSHM_SYSTEM_INFO->uid_;
      buf->st_gid = HSHM_SYSTEM_INFO->gid_;
      // buf->st_rdev = 0;
      buf->st_size = GetSize(f, astat);
      /*buf->st_blksize = 0;
      buf->st_blocks = 0;*/
      buf->st_atime = astat.st_atim_.tv_sec;
      buf->st_mtime = astat.st_mtim_.tv_sec;
      buf->st_ctime = astat.st_ctim_.tv_sec;
      errno = 0;
      return 0;
    } else {
      errno = EBADF;
      HELOG(kError, "File with descriptor {} does not exist in Hermes",
            f.hermes_fd_);
      return -1;
    }
  }

  template <typename StatT>
  int Stat(const char *__filename, StatT *buf) {
    bool stat_exists;
    AdapterStat stat;
    stat.flags_ = O_RDONLY;
    stat.st_mode_ = 0;
    File f = Open(stat, __filename);
    if (!f.status_) {
      // HILOG(kInfo, "Failed to stat the file {}", __filename);
      memset(buf, 0, sizeof(StatT));
      return -1;
    }
    int result = Stat(f, buf);
    Close(f, stat_exists);
    return result;
  }

  /** Whether or not \a fd FILE DESCRIPTOR was generated by Hermes */
  static bool IsFdTracked(int fd, std::shared_ptr<AdapterStat> &stat) {
    if (!HERMES->IsInitialized() || fd < 8192) {
      return false;
    }
    hermes::adapter::File f;
    f.hermes_fd_ = fd;
    stat = HERMES_FS_METADATA_MANAGER->Find(f);
    return stat != nullptr;
  }

  /** Whether or not \a fd FILE DESCRIPTOR was generated by Hermes */
  static bool IsFdTracked(int fd) {
    std::shared_ptr<AdapterStat> stat;
    return IsFdTracked(fd, stat);
  }

 public:
  /** Allocate an fd for the file f */
  void RealOpen(File &f, AdapterStat &stat, const std::string &path) override {
    if (stat.flags_ & O_APPEND) {
      stat.hflags_.SetBits(HERMES_FS_APPEND);
    }
    if (stat.flags_ & O_CREAT || stat.flags_ & O_TMPFILE) {
      stat.hflags_.SetBits(HERMES_FS_CREATE);
    }
    if (stat.flags_ & O_TRUNC) {
      stat.hflags_.SetBits(HERMES_FS_TRUNC);
    }

    if (stat.hflags_.Any(HERMES_FS_CREATE)) {
      if (stat.adapter_mode_ != AdapterMode::kScratch) {
        stat.fd_ = real_api_->open(path.c_str(), stat.flags_, stat.st_mode_);
      }
    } else {
      stat.fd_ = real_api_->open(path.c_str(), stat.flags_);
    }

    if (stat.fd_ >= 0) {
      stat.hflags_.SetBits(HERMES_FS_EXISTS);
    }
    if (stat.fd_ < 0 && stat.adapter_mode_ != AdapterMode::kScratch) {
      f.status_ = false;
    }
  }

  /**
   * Called after real open. Allocates the Hermes representation of
   * identifying file information, such as a hermes file descriptor
   * and hermes file handler. These are not the same as POSIX file
   * descriptor and STDIO file handler.
   * */
  void HermesOpen(File &f, const AdapterStat &stat,
                  FilesystemIoClientState &fs_mdm) override {
    f.hermes_fd_ = fs_mdm.mdm_->AllocateFd();
  }

  /** Synchronize \a file FILE f */
  int RealSync(const File &f, const AdapterStat &stat) override {
    (void)f;
    if (stat.adapter_mode_ == AdapterMode::kScratch && stat.fd_ == -1) {
      return 0;
    }
    return real_api_->fsync(stat.fd_);
  }

  /** Close \a file FILE f */
  int RealClose(const File &f, AdapterStat &stat) override {
    (void)f;
    if (stat.adapter_mode_ == AdapterMode::kScratch && stat.fd_ == -1) {
      return 0;
    }
    return real_api_->close(stat.fd_);
  }

  /**
   * Called before RealClose. Releases information provisioned during
   * the allocation phase.
   * */
  void HermesClose(File &f, const AdapterStat &stat,
                   FilesystemIoClientState &fs_mdm) override {
    fs_mdm.mdm_->ReleaseFd(f.hermes_fd_);
  }

  /** Remove \a file FILE f */
  int RealRemove(const std::string &path) override {
    return real_api_->remove(path.c_str());
  }

  /** Get initial statistics from the backend */
  size_t GetBackendSize(const chi::string &bkt_name) override {
    size_t true_size = 0;
    std::string filename = bkt_name.str();
    int fd = real_api_->open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
      return 0;
    }
    struct stat buf;
    real_api_->fstat(fd, &buf);
    true_size = buf.st_size;
    real_api_->close(fd);

    HILOG(kDebug, "The size of the file {} on disk is {}", filename, true_size);
    return true_size;
  }

  /** Write blob to backend */
  void WriteBlob(const std::string &bkt_name, const Blob &full_blob,
                 const FsIoOptions &opts, IoStatus &status) override {
    (void)opts;
    status.success_ = true;
    HILOG(kDebug,
          "Writing to file: {}"
          " offset: {}"
          " size: {}",
          bkt_name, opts.backend_off_, full_blob.size());
    int fd = real_api_->open(bkt_name.c_str(), O_RDWR | O_CREAT);
    if (fd < 0) {
      status.size_ = 0;
      status.success_ = false;
      return;
    }
    status.size_ = real_api_->pwrite(fd, full_blob.data(), full_blob.size(),
                                     opts.backend_off_);
    if (status.size_ != full_blob.size()) {
      status.success_ = false;
    }
    real_api_->close(fd);
  }

  /** Read blob from the backend */
  void ReadBlob(const std::string &bkt_name, Blob &full_blob,
                const FsIoOptions &opts, IoStatus &status) override {
    (void)opts;
    status.success_ = true;
    HILOG(kDebug,
          "Reading from file: {}"
          " offset: {}"
          " size: {}",
          bkt_name, opts.backend_off_, full_blob.size());
    int fd = real_api_->open(bkt_name.c_str(), O_RDONLY);
    if (fd < 0) {
      status.size_ = 0;
      status.success_ = false;
      return;
    }
    status.size_ = real_api_->pread(fd, full_blob.data(), full_blob.size(),
                                    opts.backend_off_);
    if (status.size_ != full_blob.size()) {
      status.success_ = false;
    }
    real_api_->close(fd);
  }

  void UpdateIoStatus(const FsIoOptions &opts, IoStatus &status) override {
    (void)opts;
    (void)status;
  }
};

/** Simplify access to the stateless PosixFs Singleton */
#define HERMES_POSIX_FS \
  hshm::Singleton<::hermes::adapter::PosixFs>::GetInstance()
#define HERMES_POSIX_FS_T hermes::adapter::PosixFs *

}  // namespace hermes::adapter

#endif  // HERMES_ADAPTER_POSIX_NATIVE_H_
