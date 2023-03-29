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

#ifndef HERMES_SHM_SINGLETON_H
#define HERMES_SHM_SINGLETON_H

#include <memory>
#include "hermes_shm/thread/lock/mutex.h"

namespace hshm {

/**
 * Makes a singleton. Constructs the first time GetInstance is called.
 * Requires user to define the static storage of obj_ in separate file.
 * @tparam T
 */
template<typename T>
class Singleton {
 private:
  static std::unique_ptr<T> obj_;
  static hshm::Mutex lock_;

 public:
  Singleton() = default;

  /** Get or create an instance of type T */
  inline static T* GetInstance() {
    if (!obj_) {
      hshm::ScopedMutex lock(lock_);
      if (obj_ == nullptr) {
        obj_ = std::make_unique<T>();
      }
    }
    return obj_.get();
  }
};
#define DEFINE_SINGLETON_CC(T)\
  template<> std::unique_ptr<T>\
    hshm::Singleton<T>::obj_ = nullptr;\
  template<> hshm::Mutex\
    hshm::Singleton<T>::lock_ =\
    hshm::Mutex();

/**
 * Makes a singleton. Constructs during initialization of program.
 * Requires user to define the static storage of obj_ in separate file.
 * */
template<typename T>
class GlobalSingleton {
 private:
  static T obj_;
 public:
  GlobalSingleton() = default;
  static T* GetInstance() {
    return &obj_;
  }
};
#define DEFINE_GLOBAL_SINGLETON_CC(T)\
  template<> T\
    hshm::GlobalSingleton<T>::obj_ = T();

/**
 * A class to represent singleton pattern
 * Does not require specific initialization of the static variable
 * */
template<typename T>
class EasySingleton {
 protected:
  /** static instance. */
  static std::unique_ptr<T> obj_;
  static hshm::Mutex lock_;

 public:
  /**
   * Uses unique pointer to build a static global instance of variable.
   * @tparam T
   * @return instance of T
   */
  static T* GetInstance() {
    if (obj_ == nullptr) {
      hshm::ScopedMutex lock(lock_);
      if (obj_ == nullptr) {
        obj_ = std::make_unique<T>();
      }
    }
    return obj_.get();
  }
};
template <typename T>
std::unique_ptr<T> EasySingleton<T>::obj_ = nullptr;
template <typename T>
hshm::Mutex EasySingleton<T>::lock_ = hshm::Mutex();

/**
 * Makes a singleton. Constructs during initialization of program.
 * Does not require specific initialization of the static variable.
 * */
template<typename T>
class EasyGlobalSingleton {
 private:
  static T obj_;
 public:
  EasyGlobalSingleton() = default;
  static T* GetInstance() {
    return &obj_;
  }
};
template <typename T>
T EasyGlobalSingleton<T>::obj_;

}  // namespace hshm

#endif  // HERMES_SHM_SINGLETON_H
