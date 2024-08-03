//
// Created by llogan on 7/1/23.
//

#include <thread>
#include "basic_test.h"
#include "chimaera/api/chimaera_client.h"
#include "chimaera_admin/chimaera_admin.h"
#include "small_message/small_message.h"
#include "hermes_shm/util/timer.h"
#include "chimaera/work_orchestrator/affinity.h"
#include "hermes/hermes.h"
#include "chimaera/api/chimaera_runtime.h"

/** The performance of getting a queue */
TEST_CASE("TestGetQueue") {
  /*chi::QueueId qid(0, 3);
  CHI_ADMIN->CreateQueue(chi::DomainId::GetLocal(), qid,
                                 16, 16, 256,
                                 hshm::bitfield32_t(0));
  CHI_CLIENT->GetQueue(qid);

  hshm::Timer t;
  t.Resume();
  size_t ops = (1 << 20);
  for (size_t i = 0; i < ops; ++i) {
    chi::MultiQueue *queue = CHI_CLIENT->GetQueue(qid);
    REQUIRE(queue->id_ == qid);
  }
  t.Pause();

  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());*/
}

/** Single-thread performance of allocating + freeing tasks */
TEST_CASE("TestHshmAllocateFree") {
  hshm::Timer t;
  t.Resume();
  size_t ops = (1 << 20);
  size_t count = (1 << 8);
  size_t reps = ops / count;
  for (size_t i = 0; i < reps; ++i) {
    std::vector<chi::Task*> tasks(count);
    for (size_t j = 0; j < count; ++j) {
      tasks[j] = CHI_CLIENT->NewTask<chi::Task>().ptr_;
    }
    for (size_t j = 0; j < count; ++j) {
      CHI_CLIENT->DelTask(tasks[j]);
    }
  }
  t.Pause();
  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}

/** Single-thread performance of emplacing, and popping a mpsc_ptr_queue */
TEST_CASE("TestPointerQueueEmplacePop") {
  size_t ops = (1 << 20);
  auto queue_ptr = hipc::make_uptr<hipc::mpsc_ptr_queue<hipc::Pointer>>(ops);
  auto queue = queue_ptr.get();
  hipc::Pointer p;

  hshm::Timer t;
  t.Resume();
  for (size_t i = 0; i < ops; ++i) {
    queue->emplace(p);
    queue->pop(p);
  }
  t.Pause();

  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}

/** Single-thread performance of empacling + popping vec<mpsc_ptr_queue> */
TEST_CASE("TestPointerQueueVecEmplacePop") {
  auto queues_ptr = hipc::make_uptr<hipc::vector<hipc::mpsc_ptr_queue<hipc::Pointer>>>(16);
  auto queues = queues_ptr.get();
  hipc::Pointer p;

  hshm::Timer t;
  size_t ops = (1 << 20);
  for (size_t i = 0; i < ops; ++i) {
    t.Resume();
    auto &queue = (*queues)[0];
    queue.emplace(p);
    queue.pop(p);
    t.Pause();
  }

  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}

/** Single-thread performance of getting, emplacing, and popping a queue */
TEST_CASE("TestHshmQueueEmplacePop") {
  chi::QueueId qid(0, 3);
  u32 ops = (1 << 20);
  std::vector<PriorityInfo> queue_info = {
      {TaskPrio::kAdmin, 16, 16, ops, 0}
  };
  auto queue = hipc::make_uptr<chi::MultiQueue>(
      qid, queue_info);
  chi::LaneData entry;
  auto task = CHI_CLIENT->NewTask<chi::Task>();
  entry.p_ = task.shm_;

  hshm::Timer t;
  t.Resume();
  chi::Lane &lane = queue->GetLane(0, 0);
  for (size_t i = 0; i < ops; ++i) {
    queue->Emplace(0, 0, entry);
    lane.pop();
  }
  t.Pause();

  CHI_CLIENT->DelTask(task);
  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}

/** Single-thread performance of getting a lane from a queue */
TEST_CASE("TestHshmQueueGetLane") {
  chi::QueueId qid(0, 3);
  std::vector<PriorityInfo> queue_info = {
      {TaskPrio::kAdmin, 16, 16, 256, 0}
  };
  auto queue = hipc::make_uptr<chi::MultiQueue>(
      qid, queue_info);
  chi::LaneGroup group = queue->GetGroup(0);

  hshm::Timer t;
  size_t ops = (1 << 20);
  t.Resume();
  for (size_t i = 0; i < ops; ++i) {
    queue->GetLane(0, i % group.num_lanes_);
  }
  t.Pause();

  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}

/** Single-thread performance of getting, emplacing, and popping a queue */
TEST_CASE("TestHshmQueueAllocateEmplacePop") {
  TRANSPARENT_HERMES();
  chi::QueueId qid(0, 3);
  std::vector<PriorityInfo> queue_info = {
      {TaskPrio::kAdmin, 16, 16, 256, 0}
  };
  auto queue = hipc::make_uptr<chi::MultiQueue>(
      qid, queue_info);
  chi::Lane &lane = queue->GetLane(0, 0);

  hshm::Timer t;
  size_t ops = (1 << 20);
  t.Resume();
  for (size_t i = 0; i < ops; ++i) {
    chi::LaneData entry;
    auto task = CHI_CLIENT->NewTask<chi::Task>();
    entry.p_ = task.shm_;
    queue->Emplace(0, 0, entry);
    lane.pop();
    CHI_CLIENT->DelTask(task);
  }
  t.Pause();

  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}

/** Time to spawn and join a thread */
TEST_CASE("TestSpawnJoinThread") {
  hshm::Timer t;
  t.Resume();
  size_t count = 16 * (1 << 10);
  for (int i = 0; i < count; ++i) {
    std::thread thread([]() { });
    thread.join();
  }
  t.Pause();
  HILOG(kInfo, "Latency: {} MOps", count / t.GetUsec());
}

/** Time to spawn and join a thread */
TEST_CASE("TestSpawnJoinArgoThread") {
  hshm::Timer t;
  t.Resume();
  ABT_xstream xstream;
  ABT_thread tl_thread_;
  size_t count = 16 * (1 << 10);
  ABT_init(0, nullptr);
  int ret = ABT_xstream_create(ABT_SCHED_NULL, &xstream);
  if (ret != ABT_SUCCESS) {
    HELOG(kFatal, "Could not create argobots xstream");
  }
  for (int i = 0; i < count; ++i) {
    int ret = ABT_thread_create_on_xstream(xstream,
                                           [](void *args) { }, nullptr,
                                           ABT_THREAD_ATTR_NULL, &tl_thread_);
    if (ret != ABT_SUCCESS) {
      HELOG(kFatal, "Couldn't spawn worker");
    }
    ABT_thread_join(tl_thread_);
  }
  t.Pause();
  HILOG(kInfo, "Latency: {} MOps", count / t.GetUsec());
}

void TestWorkerIterationLatency(u32 num_queues, u32 num_lanes) {
  HRUN_RUNTIME->Create();

  chi::Worker worker(0);
  std::vector<hipc::uptr<chi::MultiQueue>> queues;
  for (u32 i = 0; i < num_queues; ++i) {
    chi::QueueId qid(0, i + 1);
    std::vector<PriorityInfo> queue_info = {
        {TaskPrio::kAdmin, num_lanes, num_lanes, 256, 0}
    };
    auto queue = hipc::make_uptr<chi::MultiQueue>(
        qid, queue_info);
    queues.emplace_back(std::move(queue));
    for (u32 j = 0; j < num_lanes; ++j) {
      worker.PollQueues({{0, j, queue.get()}});
    }
  }

  chi::small_message::Client client;
  CHI_ADMIN->RegisterModule(chi::DomainId::GetLocal(), "small_message");\
  client.Create(chi::DomainId::GetLocal(), "ipc_test");

  hshm::Timer t;
  t.Resume();
  // size_t ops = (1 << 20);
  size_t ops = 256;
  for (size_t i = 0; i < ops; ++i) {
    hipc::LPointer<chi::small_message::MdPushTask> task;
    chi::TaskNode task_node(chi::TaskId((u32)0, (u64)i));
    task = client.AsyncMdPushEmplace(queues[num_queues - 1].get(),
                                     task_node,
                                     chi::DomainId::GetLocal());
    worker.Run(false);
    CHI_CLIENT->DelTask(task);
  }
  t.Pause();

  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}

/** Time for worker to process a request */
TEST_CASE("TestWorkerLatency") {
  CHIMAERA_CLIENT_INIT();
  TestWorkerIterationLatency(1, 16);
  TestWorkerIterationLatency(5, 16);
  TestWorkerIterationLatency(10, 16);
  TestWorkerIterationLatency(15, 16);
}

/** Time to process a request */
TEST_CASE("TestRoundTripLatency") {
  TRANSPARENT_HERMES();
  chi::small_message::Client client;
  CHI_ADMIN->RegisterModule(chi::DomainId::GetLocal(), "small_message");
//  int count = 25;
//  for (int i = 0; i < count; ++i) {
//    chi::small_message::Client client2;
//    client2.Create(chi::DomainId::GetLocal(), "ipc_test" + std::to_string(i));
//  }
  client.Create(chi::DomainId::GetLocal(), "ipc_test");
  hshm::Timer t;

  // int pid = getpid();
  // ProcessAffiner::SetCpuAffinity(pid, 8);

  t.Resume();
  size_t ops = (1 << 20);
  // size_t ops = 1024;
  for (size_t i = 0; i < ops; ++i) {
    // client.Md(chi::DomainId::GetLocal());
    client.MdPush(chi::DomainId::GetLocal());
  }
  t.Pause();

  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}

TEST_CASE("TestTimespecLatency") {
  size_t ops = (1 << 20);
  hshm::Timer t;

  t.Resume();
  for (size_t i = 0; i < ops; ++i) {
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
  }
  t.Pause();

  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}

TEST_CASE("TestTimerLatency") {
  size_t ops = (1 << 20);
  hshm::Timer t, tmp;

  t.Resume();
  double usec;
  for (size_t i = 0; i < ops; ++i) {
    usec = tmp.GetUsecFromStart();
  }
  t.Pause();

  HILOG(kInfo, "Latency: {} MOps (usec={})", ops / t.GetUsec(), usec);
}

TEST_CASE("TestTimepointLatency") {
  size_t ops = (1 << 20);
  hshm::Timer t;
  hshm::Timepoint tmp;

  t.Resume();
  double usec;
  for (size_t i = 0; i < ops; ++i) {
    usec = tmp.GetUsecFromStart();
  }
  t.Pause();

  HILOG(kInfo, "Latency: {} MOps (usec={})", ops / t.GetUsec(), usec);
}

/** Time to process a request */
//TEST_CASE("TestHermesGetBlobIdLatency") {
//  HERMES->ClientInit();
//  hshm::Timer t;
//
//  int pid = getpid();
//  ProcessAffiner::SetCpuAffinity(pid, 8);
//  hermes::Bucket bkt = HERMES_FILESYSTEM_API->Open("/home/lukemartinlogan/hi.txt");
//
//  t.Resume();
//  size_t ops = (1 << 20);
//  hermes::Context ctx;
//  std::string data(ctx.page_size_, 0);
//  for (size_t i = 0; i < ops; ++i) {
//    bkt.GetBlobId(std::to_string(i));
//  }
//  t.Pause();
//
//  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
//}
//
///** Time to process a request */
//TEST_CASE("TestHermesFsWriteLatency") {
//  HERMES->ClientInit();
//  hshm::Timer t;
//
//  int pid = getpid();
//  ProcessAffiner::SetCpuAffinity(pid, 8);
//  hermes::Bucket bkt = HERMES_FILESYSTEM_API->Open("/home/lukemartinlogan/hi.txt");
//
//  t.Resume();
//  size_t ops = 150;
//  hermes::Context ctx;
//  ctx.page_size_ = 4096;
//  std::string data(ctx.page_size_, 0);
//  for (size_t i = 0; i < ops; ++i) {
//    HERMES_FILESYSTEM_API->Write(bkt, data.data(), i * ctx.page_size_, data.size(), false, rctx);
//  }
//  t.Pause();
//
//  HILOG(kInfo, "Latency: {} MBps", ops * 4096 / t.GetUsec());
//}
//
///** Time to process a request */
//TEST_CASE("TestHermesFsReadLatency") {
//  HERMES->ClientInit();
//  hshm::Timer t;
//
//  int pid = getpid();
//  ProcessAffiner::SetCpuAffinity(pid, 8);
//  hermes::Bucket bkt = HERMES_FILESYSTEM_API->Open("/home/lukemartinlogan/hi.txt");
//
//  size_t ops = 1024;
//  hermes::Context ctx;
//  ctx.page_size_ = 4096;
//  std::string data(ctx.page_size_, 0);
//  for (size_t i = 0; i < ops; ++i) {
//    HERMES_FILESYSTEM_API->Write(bkt, data.data(), i * ctx.page_size_, data.size(), false, rctx);
//  }
//
//  t.Resume();
//  for (size_t i = 0; i < ops; ++i) {
//    HERMES_FILESYSTEM_API->Read(bkt, data.data(), i * ctx.page_size_, data.size(), false, rctx);
//  }
//  t.Pause();
//
//  HILOG(kInfo, "Latency: {} MBps", ops * 4096 / t.GetUsec());
//}