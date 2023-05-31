/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Programmer:  Luke Logan
 *              Jan 2023
 *
 * Purpose: The hermes file driver using only the HDF5 public API
 *          and buffer datasets in Hermes buffering systems with
 *          multiple storage tiers.
 */
#ifndef _GNU_SOURCE
  #define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <errno.h>

#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

/* HDF5 header for dynamic plugin loading */
#include "H5PLextern.h"
#include "H5FDhermes.h"     /* Hermes file driver     */
#include "H5FDhermes_err.h" /* error handling         */

#include "adapter/posix/posix_io_client.h"
#include "posix/posix_fs_api.h"

/* for traces */
#include <H5FDdevelop.h> /* File driver development macros */
#include "H5FDhermes_log.h"
// extern "C" {
// #include "H5FDhermes_log.h" /* Connecting to vol         */
// }


/**
 * Make this adapter use Hermes.
 * Disabling will use POSIX.
 * */
#define USE_HERMES

#define H5FD_HERMES (H5FD_hermes_init())
/* HDF5 doesn't currently have a driver init callback. Use
 * macro to initialize driver if loaded as a plugin.
 */
#define H5FD_HERMES_INIT                        \
  do {                                          \
    if (H5FD_HERMES_g < 0)                      \
      H5FD_HERMES_g = H5FD_HERMES;              \
  } while (0)


/* The driver identification number, initialized at runtime */
static hid_t H5FD_HERMES_g = H5I_INVALID_HID;

/* Identifiers for HDF5's error API */
hid_t H5FDhermes_err_stack_g = H5I_INVALID_HID;
hid_t H5FDhermes_err_class_g = H5I_INVALID_HID;

/* File operations */
#define OP_UNKNOWN 0
#define OP_READ    1
#define OP_WRITE   2

using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::File;

/* POSIX I/O mode used as the third parameter to open/_open
 * when creating a new file (O_CREAT is set). */
#if defined(H5_HAVE_WIN32_API)
#define H5FD_HERMES_POSIX_CREATE_MODE_RW (_S_IREAD | _S_IWRITE)
#else
#define H5FD_HERMES_POSIX_CREATE_MODE_RW 0666
#endif

#define MAXADDR          (((haddr_t)1 << (8 * sizeof(off_t) - 1)) - 1)
#define SUCCEED 0
#define FAIL    (-1)

#ifdef __cplusplus
extern "C" {
#endif



/* Driver-specific file access properties */
typedef struct H5FD_hermes_fapl_t {
  hbool_t logStat; /* write to file name on flush */
  size_t  page_size;   /* page size */
} H5FD_hermes_fapl_t;

/* Prototypes */
static herr_t H5FD__hermes_term(void);
static herr_t  H5FD__hermes_fapl_free(void *_fa);
static H5FD_t *H5FD__hermes_open(const char *name, unsigned flags,
                                 hid_t fapl_id, haddr_t maxaddr);
static herr_t H5FD__hermes_close(H5FD_t *_file);
static int H5FD__hermes_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t H5FD__hermes_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD__hermes_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t H5FD__hermes_set_eoa(H5FD_t *_file, H5FD_mem_t type,
                                   haddr_t addr);
static haddr_t H5FD__hermes_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t H5FD__hermes_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id,
                                haddr_t addr, size_t size, void *buf);
static herr_t H5FD__hermes_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id,
                                 haddr_t addr, size_t size, const void *buf);


static const H5FD_class_t H5FD_hermes_g = {
  H5FD_CLASS_VERSION,      /* struct version       */
  H5FD_HERMES_VALUE,         /* value                */
  H5FD_HERMES_NAME,          /* name                 */
  MAXADDR,                   /* maxaddr              */
  H5F_CLOSE_STRONG,          /* fc_degree            */
  H5FD__hermes_term,         /* terminate            */
  NULL,                      /* sb_size              */
  NULL,                      /* sb_encode            */
  NULL,                      /* sb_decode            */
  sizeof(H5FD_hermes_fapl_t),/* fapl_size            */
  NULL,                      /* fapl_get             */
  NULL,                      /* fapl_copy            */
  H5FD__hermes_fapl_free,    /* fapl_free            */
  0,                         /* dxpl_size            */
  NULL,                      /* dxpl_copy            */
  NULL,                      /* dxpl_free            */
  H5FD__hermes_open,         /* open                 */
  H5FD__hermes_close,        /* close                */
  H5FD__hermes_cmp,          /* cmp                  */
  H5FD__hermes_query,        /* query                */
  NULL,                      /* get_type_map         */
  NULL,                      /* alloc                */
  NULL,                      /* free                 */
  H5FD__hermes_get_eoa,      /* get_eoa              */
  H5FD__hermes_set_eoa,      /* set_eoa              */
  H5FD__hermes_get_eof,      /* get_eof              */
  NULL,                      /* get_handle           */
  H5FD__hermes_read,         /* read                 */
  H5FD__hermes_write,        /* write                */
  NULL,                      /* read_vector          */
  NULL,                      /* write_vector         */
  NULL,                      /* read_selection       */
  NULL,                      /* write_selection      */
  NULL,                      /* flush                */
  NULL,                      /* truncate             */
  NULL,                      /* lock                 */
  NULL,                      /* unlock               */
  NULL,                      /* del                  */
  NULL,                      /* ctl                  */
  H5FD_FLMAP_DICHOTOMY       /* fl_map               */
};

/*-------------------------------------------------------------------------
 * Function:    H5FD_hermes_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the hermes driver
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_hermes_init(void) {
  hid_t ret_value = H5I_INVALID_HID; /* Return value */

  if (H5I_VFL != H5Iget_type(H5FD_HERMES_g)) {
    H5FD_HERMES_g = H5FDregister(&H5FD_hermes_g);
  }

  /* Set return value */
  ret_value = H5FD_HERMES_g;
  return ret_value;
} /* end H5FD_hermes_init() */

/*---------------------------------------------------------------------------
 * Function:    H5FD__hermes_term
 *
 * Purpose:     Shut down the VFD
 *
 * Returns:     SUCCEED (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD__hermes_term(void) {
  herr_t ret_value = SUCCEED;

  // vfd_dlife_helper_teardown(DLIFE_HELPER_VFD);
  fflush(DLIFE_HELPER_VFD->dlife_file_handle);

  /* Unregister from HDF5 error API */
  if (H5FDhermes_err_class_g >= 0) {
    if (H5Eunregister_class(H5FDhermes_err_class_g) < 0) {
      // TODO(llogan)
    }

    /* Destroy the error stack */
    if (H5Eclose_stack(H5FDhermes_err_stack_g) < 0) {
      // TODO(llogan)
    } /* end if */

    H5FDhermes_err_stack_g = H5I_INVALID_HID;
    H5FDhermes_err_class_g = H5I_INVALID_HID;
  }

  /* Reset VFL ID */
  H5FD_HERMES_g = H5I_INVALID_HID;

  HERMES->Finalize();


  return ret_value;
} /* end H5FD__hermes_term() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_hermes
 *
 * Purpose:     Modify the file access property list to use the H5FD_HERMES
 *              driver defined in this source file.  There are no driver
 *              specific properties.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_hermes(hid_t fapl_id, hbool_t logStat, size_t page_size) {
  H5FD_hermes_fapl_t fa; /* Hermes VFD info */
  herr_t ret_value = SUCCEED; /* Return value */


  /* Check argument */
  if (H5I_GENPROP_LST != H5Iget_type(fapl_id) ||
      TRUE != H5Pisa_class(fapl_id, H5P_FILE_ACCESS)) {
    // H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL,
    //                        "not a file access property list");
  }

  /* Set VFD info values */
  memset(&fa, 0, sizeof(H5FD_hermes_fapl_t));
  fa.logStat  = logStat;
  fa.page_size = page_size;

  /* Set the property values & the driver for the FAPL */
  if (H5Pset_driver(fapl_id, H5FD_HERMES, &fa) < 0) {
    // H5FD_HERMES_GOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL,
    //                        "can't set Hermes VFD as driver");
  }

  /* custom VFD code start */
  print_H5Pset_fapl_info("H5Pset_fapl_hermes", logStat, page_size);
  /* custom VFD code end */

done:
  H5FD_HERMES_FUNC_LEAVE_API;
}  /* end H5Pset_fapl_hermes() */


/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_fapl_free
 *
 * Purpose:    Frees the family-specific file access properties.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5FD__hermes_fapl_free(void *_fa) {
  H5FD_hermes_fapl_t *fa = (H5FD_hermes_fapl_t *)_fa;
  herr_t ret_value = SUCCEED;  /* Return value */

  free(fa);

  H5FD_HERMES_FUNC_LEAVE;
}


/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_open
 *
 * Purpose:     Create and/or opens a bucket in Hermes.
 *
 * Return:      Success:    A pointer to a new bucket data structure.
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD__hermes_open(const char *name, unsigned flags, hid_t fapl_id,
                  haddr_t maxaddr) {
  
  H5FD_hermes_t  *file = NULL; /* hermes VFD info          */
  int fd = -1;
  int o_flags = 0;
  
  /* custom VFD code start */
  unsigned long t_start = get_time_usec();
  const H5FD_hermes_fapl_t *fa   = NULL;
  H5FD_hermes_fapl_t new_fa = {0};

  /* Sanity check on file offsets */
  assert(sizeof(off_t) >= sizeof(size_t));

  H5FD_HERMES_INIT;

  // /* Check arguments */
  // if (!name || !*name)
  //   H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name");
  // if (0 == maxaddr || HADDR_UNDEF == maxaddr)
  //   H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr");
  // if (ADDR_OVERFLOW(maxaddr))
  //   H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, NULL, "bogus maxaddr");

  /* Get the driver specific information */
  H5E_BEGIN_TRY {
    fa = static_cast<const H5FD_hermes_fapl_t*>(H5Pget_driver_info(fapl_id));
  }
  H5E_END_TRY;
  if (!fa || (H5P_FILE_ACCESS_DEFAULT == fapl_id)) {
    ssize_t config_str_len = 0;
    char config_str_buf[128];
    if ((config_str_len =
         H5Pget_driver_config_str(fapl_id, config_str_buf, 128)) < 0) {
          std::cerr << "H5Pget_driver_config_str error" << std::endl;
    }
    char *saveptr = NULL;
    char* token = strtok_r(config_str_buf, " ", &saveptr);
    if (!strcmp(token, "true") || !strcmp(token, "TRUE") ||
        !strcmp(token, "True")) {
      new_fa.logStat = true;
    }
    token = strtok_r(0, " ", &saveptr);
    sscanf(token, "%zu", &(new_fa.page_size));
    fa = &new_fa;
  }
  /* custom VFD code end */


  /* Build the open flags */
  o_flags = (H5F_ACC_RDWR & flags) ? O_RDWR : O_RDONLY;
  if (H5F_ACC_TRUNC & flags) {
    o_flags |= O_TRUNC;
  }
  if (H5F_ACC_CREAT & flags) {
    o_flags |= O_CREAT;
  }
  if (H5F_ACC_EXCL & flags) {
    o_flags |= O_EXCL;
  }

#ifdef USE_HERMES
/* custom VFD code start */

  hermes::config::ClientConfig clientConfig;
  clientConfig.SetBaseAdapterPageSize(fa->page_size);
  size_t adp_page_size = clientConfig.GetBaseAdapterPageSize();
  // printf("page_size: %ld\n", adp_page_size);
  
/* custom VFD code end */
  auto fs_api = HERMES_POSIX_FS;
  bool stat_exists;
  AdapterStat stat;
  stat.flags_ = o_flags;
  stat.st_mode_ = H5FD_HERMES_POSIX_CREATE_MODE_RW;
  File f = fs_api->Open(stat, name);
  fd = f.hermes_fd_;
  HILOG(kDebug, "")


#else
  fd = open(name, o_flags);
#endif
  if (fd < 0) {
    // int myerrno = errno;
    return nullptr;
  }

  /* Create the new file struct */
  file = (H5FD_hermes_t*)calloc(1, sizeof(H5FD_hermes_t));
  if (file == NULL) {
    // TODO(llogan)
  }

  /* Pack file */
  if (name && *name) {
    file->filename_ = strdup(name);
  }
  file->fd = fd;
  file->op = OP_UNKNOWN;
  file->flags = flags;

#ifdef USE_HERMES
  file->eof = (haddr_t)fs_api->GetSize(f, stat_exists);
#else
  file->eof = stdfs::file_size(name);
#endif

  /* custom VFD code start */

  file->page_size = fa->page_size;
  file->my_fapl_id = fapl_id;
  file->logStat = fa->logStat;

  if(DLIFE_HELPER_VFD == nullptr){
    char file_path[256];
    parseEnvironmentVariable(file_path);
    DLIFE_HELPER_VFD = vfd_dlife_helper_init(file_path, file->logStat, file->page_size);
  }
  // file->vfd_file_info = add_vfd_file_node(name, file);
  file->vfd_file_info = add_vfd_file_node(DLIFE_HELPER_VFD, name, file);
  open_close_info_update("H5FD__hermes_open", file, file->eof, flags);

  print_open_close_info("H5FD__hermes_open", file, name, t_start, get_time_usec(), file->eof, file->flags);

  /* custom VFD code end */

  return (H5FD_t *)file;
} /* end H5FD__hermes_open() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_close
 *
 * Purpose:     Closes an HDF5 file.
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL, file not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5FD__hermes_close(H5FD_t *_file) {
  unsigned long t_start = get_time_usec();
  H5FD_hermes_t *file = (H5FD_hermes_t *)_file;
  herr_t ret_value = SUCCEED; /* Return value */
  assert(file);

  /* custom VFD code start */
  open_close_info_update("H5FD__hermes_close", file, file->eof, file->flags);
  print_open_close_info("H5FD__hermes_close", file, file->filename_, t_start, get_time_usec(), file->eof, file->flags);
  dump_vfd_file_stat_yaml(DLIFE_HELPER_VFD->dlife_file_handle, file->vfd_file_info);
  rm_vfd_file_node(DLIFE_HELPER_VFD, _file);
  /* custom VFD code end */

#ifdef USE_HERMES
  auto fs_api = HERMES_POSIX_FS;
  File f; f.hermes_fd_ = file->fd;
  bool stat_exists;
  fs_api->Close(f, stat_exists);
  HILOG(kDebug, "")
#else
  close(file->fd);
#endif
  if (file->filename_) {
    free(file->filename_);
  }
  free(file);

  return ret_value;
} /* end H5FD__hermes_close() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_cmp
 *
 * Purpose:     Compares two buckets belonging to this driver using an
 *              arbitrary (but consistent) ordering.
 *
 * Return:      Success:    A value like strcmp()
 *              Failure:    never fails (arguments were checked by the
 *                          caller).
 *
 *-------------------------------------------------------------------------
 */
static int H5FD__hermes_cmp(const H5FD_t *_f1, const H5FD_t *_f2) {
  const H5FD_hermes_t *f1        = (const H5FD_hermes_t *)_f1;
  const H5FD_hermes_t *f2        = (const H5FD_hermes_t *)_f2;
  int                  ret_value = 0;

  ret_value = strcmp(f1->filename_, f2->filename_);

  return ret_value;
} /* end H5FD__hermes_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_query
 *
 * Purpose:     Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5FD__hermes_query(const H5FD_t *_file,
                                 unsigned long *flags /* out */) {
  /* Set the VFL feature flags that this driver supports */
  /* Notice: the Mirror VFD Writer currently uses only the hermes driver as
   * the underying driver -- as such, the Mirror VFD implementation copies
   * these feature flags as its own. Any modifications made here must be
   * reflected in H5FDmirror.c
   * -- JOS 2020-01-13
   */
  herr_t ret_value = SUCCEED;

  if (flags) {
    *flags = 0;
  }                                            /* end if */

  return ret_value;
} /* end H5FD__hermes_query() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_get_eoa
 *
 * Purpose:     Gets the end-of-address marker for the file. The EOA marker
 *              is the first address past the last byte allocated in the
 *              format address space.
 *
 * Return:      The end-of-address marker.
 *
 *-------------------------------------------------------------------------
 */
static haddr_t H5FD__hermes_get_eoa(const H5FD_t *_file,
                                    H5FD_mem_t type) {
  (void) type;
  haddr_t ret_value = HADDR_UNDEF;

  const H5FD_hermes_t *file = (const H5FD_hermes_t *)_file;

  ret_value = file->eoa;

  return ret_value;
} /* end H5FD__hermes_get_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_set_eoa
 *
 * Purpose:     Set the end-of-address marker for the file. This function is
 *              called shortly after an existing HDF5 file is opened in order
 *              to tell the driver where the end of the HDF5 data is located.
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5FD__hermes_set_eoa(H5FD_t *_file,
                                   H5FD_mem_t type,
                                   haddr_t addr) {
  (void) type;
  herr_t ret_value = SUCCEED;

  H5FD_hermes_t *file = (H5FD_hermes_t *)_file;

  file->eoa = addr;

  return ret_value;
} /* end H5FD__hermes_set_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_get_eof
 *
 * Purpose:     Returns the end-of-file marker, which is the greater of
 *              either the filesystem end-of-file or the HDF5 end-of-address
 *              markers.
 *
 * Return:      End of file address, the first address past the end of the
 *              "file", either the filesystem file or the HDF5 file.
 *
 *-------------------------------------------------------------------------
 */
static haddr_t H5FD__hermes_get_eof(const H5FD_t *_file,
                                    H5FD_mem_t type) {
  (void) type;
  haddr_t ret_value = HADDR_UNDEF;

  const H5FD_hermes_t *file = (const H5FD_hermes_t *)_file;

  ret_value = file->eof;

  return ret_value;
} /* end H5FD__hermes_get_eof() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_read
 *
 * Purpose:     Reads SIZE bytes of data from FILE beginning at address ADDR
 *              into buffer BUF according to data transfer properties in
 *              DXPL_ID. Determine the number of file pages affected by this
 *              call from ADDR and SIZE. Utilize transfer buffer PAGE_BUF to
 *              read the data from Blobs. Exercise care for the first and last
 *              pages to prevent overwriting existing data.
 *
 * Return:      Success:    SUCCEED. Result is stored in caller-supplied
 *                          buffer BUF.
 *              Failure:    FAIL, Contents of buffer BUF are undefined.
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5FD__hermes_read(H5FD_t *_file, H5FD_mem_t type,
                                hid_t dxpl_id, haddr_t addr,
                                size_t size, void *buf) {
  unsigned long t_start = get_time_usec();
  (void) dxpl_id; (void) type;
  H5FD_hermes_t *file = (H5FD_hermes_t *)_file;
  herr_t ret_value = SUCCEED;
  

#ifdef USE_HERMES
  bool stat_exists;
  auto fs_api = HERMES_POSIX_FS;
  File f; f.hermes_fd_ = file->fd; IoStatus io_status;
  size_t count = fs_api->Read(f, stat_exists, buf, addr, size, io_status);
  HILOG(kDebug, "")



#else
  size_t count = read(file->fd, (char*)buf + addr, size);
#endif

  if (count < size) {
    // TODO(llogan)
  }

  /* custom VFD code start */


  unsigned long t_end = get_time_usec();
  read_write_info_update("H5FD__hermes_read", file->filename_, file->my_fapl_id ,_file,
    type, dxpl_id, addr, size, file->page_size, t_start, t_end);

  print_read_write_info("H5FD__hermes_read", file->filename_, file->my_fapl_id ,_file,
    type, dxpl_id, addr, size, file->page_size, t_start, t_end);
  


  /* custom VFD code end */

  return ret_value;
} /* end H5FD__hermes_read() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__hermes_write
 *
 * Purpose:     Writes SIZE bytes of data contained in buffer BUF to Hermes
 *              buffering system according to data transfer properties in
 *              DXPL_ID. Determine the number of file pages affected by this
 *              call from ADDR and SIZE. Utilize transfer buffer PAGE_BUF to
 *              put the data into Blobs. Exercise care for the first and last
 *              pages to prevent overwriting existing data.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5FD__hermes_write(H5FD_t *_file, H5FD_mem_t type,
                                 hid_t dxpl_id, haddr_t addr,
                                 size_t size, const void *buf) {
  unsigned long t_start = get_time_usec();
  (void) dxpl_id; (void) type;
  H5FD_hermes_t *file = (H5FD_hermes_t *)_file;
  herr_t ret_value = SUCCEED;
#ifdef USE_HERMES
  bool stat_exists;
  auto fs_api = HERMES_POSIX_FS;
  File f; f.hermes_fd_ = file->fd; IoStatus io_status;
  size_t count = fs_api->Write(f, stat_exists, buf, addr, size, io_status);
  HILOG(kDebug, "")

#else
  size_t count = write(file->fd, (char*)buf + addr, size);
#endif
  if (count < size) {
    // TODO(llogan)
  }
  
  
  /* custom VFD code start */

  unsigned long t_end = get_time_usec();
  read_write_info_update("H5FD__hermes_write", file->filename_, file->my_fapl_id ,_file,
    type, dxpl_id, addr, size, file->page_size, t_start, t_end);

  print_read_write_info("H5FD__hermes_write", file->filename_, file->my_fapl_id ,_file,
    type, dxpl_id, addr, size, file->page_size, t_start, t_end);
  

  /* custom VFD code end */

  return ret_value;
} /* end H5FD__hermes_write() */

/*
 * Stub routines for dynamic plugin loading
 */
H5PL_type_t
H5PLget_plugin_type(void) {
  TRANSPARENT_HERMES
  return H5PL_TYPE_VFD;
}

const void*
H5PLget_plugin_info(void) {
  TRANSPARENT_HERMES
  return &H5FD_hermes_g;
}

/** Initialize Hermes */
/*static __attribute__((constructor(101))) void init_hermes_in_vfd(void) {
  std::cout << "IN VFD" << std::endl;
  TRANSPARENT_HERMES;
}*/

}  // extern C
