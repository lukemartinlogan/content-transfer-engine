#ifndef HERMES_SRC_CONFIG_SERVER_DEFAULT_H_
#define HERMES_SRC_CONFIG_SERVER_DEFAULT_H_
const char* kServerDefaultConfigStr = 
"# Example Hermes configuration file\n"
"\n"
"### Define properties of the storage devices\n"
"devices:\n"
"  # The name of the device.\n"
"  # It can be whatever the user wants, there are no special names\n"
"  ram:\n"
"    # The mount point of each device. RAM should be the empty string. For block\n"
"    # devices, this is the directory where Hermes will create buffering files. For\n"
"    # object storage or cloud targets, this will be a url.\n"
"    mount_point: \"\"\n"
"\n"
"    # The maximum buffering capacity in MiB of each device.\n"
"    capacity: 50MB\n"
"\n"
"    # The size of the smallest available buffer in KiB. In general this should be\n"
"    # the page size of your system for byte addressable storage, and the block size\n"
"    # of the storage device for block addressable storage.\n"
"    block_size: 4KB\n"
"\n"
"    # The number of blocks (the size of which is chosen in block_sizes_kb) that each\n"
"    # device should contain for each slab (controlled by num_slabs). This allows for\n"
"    # precise control of the distibution of buffer sizes.\n"
"    slab_sizes: [ 4KB, 16KB, 64KB, 1MB ]\n"
"\n"
"    # The maximum theoretical bandwidth (as advertised by the manufacturer) in\n"
"    # Possible units: KBps, MBps, GBps\n"
"    bandwidth: 6000MBps\n"
"\n"
"    # The latency of each device (as advertised by the manufacturer).\n"
"    # Possible units: ns, us, ms, s\n"
"    latency: 15us\n"
"\n"
"    # For each device, indicate \'1\' if it is shared among nodes (e.g., burst\n"
"    # buffers), or \'0\' if it is per node (e.g., local NVMe).\n"
"    is_shared_device: false\n"
"\n"
"    # For each device, the minimum and maximum percent capacity threshold at which\n"
"    # the BufferOrganizer will trigger. Decreasing the maximum thresholds will cause\n"
"    # the BufferOrganizer to move data to lower devices, making more room in faster\n"
"    # devices (ideal for write-heavy workloads). Conversely, increasing the minimum\n"
"    # threshold will cause data to be moved from slower devices into faster devices\n"
"    # (ideal for read-heavy workloads). For example, a maximum capacity threshold of\n"
"    # 0.8 would have the effect of always keeping 20% of the device\'s space free for\n"
"    # incoming writes. Conversely, a minimum capacity threshold of 0.3 would ensure\n"
"    # that the device is always at least 30% occupied.\n"
"    borg_capacity_thresh: [0.0, 1.0]\n"
"\n"
"  nvme:\n"
"    mount_point: \"./\"\n"
"    capacity: 100MB\n"
"    block_size: 4KB\n"
"    slab_sizes: [ 4KB, 16KB, 64KB, 1MB ]\n"
"    bandwidth: 1GBps\n"
"    latency: 600us\n"
"    is_shared_device: false\n"
"    borg_capacity_thresh: [ 0.0, 1.0 ]\n"
"\n"
"  ssd:\n"
"    mount_point: \"./\"\n"
"    capacity: 100MB\n"
"    block_size: 4KB\n"
"    slab_sizes: [ 4KB, 16KB, 64KB, 1MB ]\n"
"    bandwidth: 500MBps\n"
"    latency: 1200us\n"
"    is_shared_device: false\n"
"    borg_capacity_thresh: [ 0.0, 1.0 ]\n"
"\n"
"  pfs:\n"
"    mount_point: \"./\"\n"
"    capacity: 100MB\n"
"    block_size: 64KB # The stripe size of PFS\n"
"    slab_sizes: [ 4KB, 16KB, 64KB, 1MB ]\n"
"    bandwidth: 100MBps # Per-device bandwidth\n"
"    latency: 200ms\n"
"    is_shared_device: true\n"
"    borg_capacity_thresh: [ 0.0, 1.0 ]\n"
"\n"
"# Define the maximum amount of memory Hermes can use for non-buffering tasks.\n"
"# This includes metadata management and memory allocations.\n"
"# This memory will not be preallocated, so if you don\'t know, 0 indicates\n"
"# any amount of memory\n"
"max_memory: 0g\n"
"\n"
"### Define properties of RPCs\n"
"rpc:\n"
"  # A path to a file containing a list of server names, 1 per line. If your\n"
"  # servers are named according to a pattern (e.g., server-1, server-2, etc.),\n"
"  # prefer the `rpc_server_base_name` and `rpc_host_number_range` options. If this\n"
"  # option is not empty, it will override anything in `rpc_server_base_name`.\n"
"  host_file: \"\"\n"
"\n"
"  # Host names can be defined using the following syntax:\n"
"  # ares-comp-[0-9]-40g will convert to ares-comp-0-40g, ares-comp-1-40g, ...\n"
"  # ares-comp[00-09] will convert to ares-comp-00, ares-comp-01, ...\n"
"  host_names: [\"localhost\"]\n"
"\n"
"  # The RPC protocol. This must come from the documentation of the specific RPC\n"
"  # library in use.\n"
"  protocol: \"ofi+sockets\"\n"
"\n"
"  # RPC domain name for verbs transport. Blank for tcp.\n"
"  domain: \"\"\n"
"\n"
"  # Desired RPC port number.\n"
"  port: 8080\n"
"\n"
"  # The number of handler threads for each RPC server.\n"
"  num_threads: 4\n"
"\n"
"### Define properties of the BORG\n"
"buffer_organizer:\n"
"  # The number of threads used in the background organization of internal Hermes buffers.\n"
"  num_threads: 1\n"
"\n"
"  # Interval (seconds) where blobs are checked for flushing\n"
"  flush_period: 1\n"
"\n"
"  # Interval (seconds) where blobs are checked for re-organization\n"
"  blob_reorg_period: 1\n"
"\n"
"  ## What does \"recently accessed\" mean?\n"
"  # Time when score is equal to 1 (seconds)\n"
"  recency_min: 0\n"
"  # Time when score is equal to 0 (seconds)\n"
"  recency_max: 60\n"
"\n"
"  ## What does \"frequently accessed\" mean?\n"
"  # Number of accesses for score to be equal to 1 (count)\n"
"  freq_max: 15\n"
"  # Number of accesses for score to be equal to 0 (count)\n"
"  freq_min: 0\n"
"\n"
"### Define the default data placement policy\n"
"dpe:\n"
"  # Choose Random, RoundRobin, or MinimizeIoTime\n"
"  default_placement_policy: \"MinimizeIoTime\"\n"
"\n"
"  # If true (1) the RoundRobin placement policy algorithm will split each Blob\n"
"  # into a random number of smaller Blobs.\n"
"  default_rr_split: 0\n"
"\n"
"### Define I/O tracing properties\n"
"tracing:\n"
"  enabled: false\n"
"  output: \"\"\n"
"\n"
"### Define prefetcher properties\n"
"prefetch:\n"
"  enabled: false\n"
"  io_trace_path: \"\"\n"
"  apriori_schema_path: \"\"\n"
"  epoch_ms: 50\n"
"  is_mpi: false\n"
"\n"
"# The shared memory prefix for the hermes shared memory segment. A user name\n"
"# will be automatically appended.\n"
"shmem_name: \"/hermes_shm_\"\n"
"\n"
"# The interval in milliseconds at which to update the global system view.\n"
"system_view_state_update_interval_ms: 1000\n"
"\n"
"### Define the names of the traits to search LD_LIBRARY_PATH for\n"
"traits:\n"
"  - \"hermes_posix_io_client\"\n"
"  - \"hermes_stdio_io_client\"\n"
"  - \"hermes_mpiio_io_client\"\n"
"  - \"hermes_example_trait\"\n"
"  - \"hermes_prefetcher_trait\"\n";
#endif  // HERMES_SRC_CONFIG_SERVER_DEFAULT_H_