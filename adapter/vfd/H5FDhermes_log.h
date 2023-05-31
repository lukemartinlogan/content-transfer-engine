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
 * Programmer:  Meng Tang
 *              Sep 2022
 *
 * Purpose: The hermes file driver using only the HDF5 public API
 *          and buffer datasets in Hermes buffering systems with
 *          multiple storage tiers.
 */
#ifndef _GNU_SOURCE
  #define _GNU_SOURCE
#endif

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <math.h>

#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <mpi.h>

/* HDF5 header for dynamic plugin loading */
#include "H5PLextern.h"

// #include "H5FDhermes.h"     /* Hermes file driver     */
#include "H5FDhermes.h"     /* Hermes file driver     */
// #include "H5FDhermes_err.h" /* TODO: error handling         */

// #ifdef ENABLE_HDF5_IO_LOGGING
// extern "C" {
#include "/qfs/people/tang584/scripts/local-co-scheduling/vol-datalife/src/datalife_vol_types.h" /* Connecting to vol         */
//   #include "/qfs/people/tang584/scripts/local-co-scheduling/vol-datalife/src/datalife_vol_int.h" /* Connecting to vol         */
// }
// #endif

#include <time.h>       // for struct timespec, clock_gettime(CLOCK_MONOTONIC, &end);
/* candice added functions for I/O traces end */

// typedef struct Dset_access_t {
//   // this is not used
//   char      dset_name[H5L_MAX_LINK_NAME_LEN];
//   haddr_t   dset_offset;
//   int       dset_ndim;
//   hssize_t    dset_npoints;
//   hsize_t   *dset_dim;
// } Dset_access_t;


static
unsigned long get_time_usec(void) {
    struct timeval tp;

    gettimeofday(&tp, NULL);
    return (unsigned long)((1000000 * tp.tv_sec) + tp.tv_usec);
}


/************/
/* Typedefs */
/************/
typedef struct H5FD_dlife_file_info_t vfd_file_dlife_info_t;



// /* The driver identification number, initialized at runtime */
// static hid_t H5FD_HERMES_g = H5I_INVALID_HID;



typedef struct VFDDatalifeHelper {
    /* VFDDatalifeHelper properties */
    char* dlife_file_path;
    FILE* dlife_file_handle;
    hbool_t logStat;
    char user_name[32];
    int pid;
    pthread_t tid;
    char proc_name[64];
    int ptr_cnt;
    int vfd_opened_files_cnt;
    vfd_file_dlife_info_t* vfd_opened_files;//linkedlist,
    size_t hermes_page_size;

} vfd_dlife_helper_t;

static vfd_dlife_helper_t* DLIFE_HELPER_VFD = nullptr;


struct H5FD_dlife_file_info_t { // used by VFD
    vfd_dlife_helper_t* vfd_dlife_helper;  //pointer shared among all layers, one per process.

    const char* file_name;
    unsigned long file_no;
    char* intent;
    unsigned long sorder_id; // need lock
    hid_t fapl_id;
    int file_read_cnt;
    int file_write_cnt;
    size_t file_size;
    // size_t total_read_bytes;
    // size_t total_write_bytes;
    int H5FD_MEM_DRAW_read_cnt;
    int H5FD_MEM_DRAW_write_cnt;
    size_t H5FD_MEM_DRAW_read_bytes;
    size_t H5FD_MEM_DRAW_write_bytes;

    /* common metadata access type */
    int H5FD_MEM_OHDR_read_cnt;
    int H5FD_MEM_OHDR_write_cnt;
    size_t H5FD_MEM_OHDR_read_bytes;
    size_t H5FD_MEM_OHDR_write_bytes;

    int H5FD_MEM_SUPER_read_cnt;
    int H5FD_MEM_SUPER_write_cnt;
    size_t H5FD_MEM_SUPER_read_bytes;
    size_t H5FD_MEM_SUPER_write_bytes;

    int H5FD_MEM_BTREE_read_cnt;
    int H5FD_MEM_BTREE_write_cnt;
    size_t H5FD_MEM_BTREE_read_bytes;
    size_t H5FD_MEM_BTREE_write_bytes;

    int H5FD_MEM_LHEAP_read_cnt;
    int H5FD_MEM_LHEAP_write_cnt;
    size_t H5FD_MEM_LHEAP_read_bytes;
    size_t H5FD_MEM_LHEAP_write_bytes;
    
    /* beloe types not added 
    H5FD_MEM_DEFAULT, 
    H5FD_MEM_GHEAP, 
    H5FD_MEM_NTYPES, 
    H5FD_MEM_NOLIST
    */

   int ref_cnt;
   unsigned long open_time;
   unsigned long close_time;

    vfd_file_dlife_info_t *next;
};

/* The description of a file/bucket belonging to this driver. */
typedef struct H5FD_hermes_t {
  H5FD_t         pub;         /* public stuff, must be first           */
  haddr_t        eoa;         /* end of allocated region               */
  haddr_t        eof;         /* end of file; current file size        */
  haddr_t        pos;         /* current file I/O position             */
  int            op;          /* last operation                        */
  int            fd;          /* the filesystem file descriptor        */
  char           *filename_;  /* the name of the file */
  unsigned       flags;       /* The flags passed from H5Fcreate/H5Fopen */

  /* custom VFD code start */
  hbool_t        logStat; /* write I/O stats to yaml file           */
  size_t         page_size;   /* page size */
  hid_t          my_fapl_id;     /* file access property list ID */
  vfd_dlife_helper_t *vfd_dlife_helper; /* pointer shared among all layers, one per process. */
  vfd_file_dlife_info_t * vfd_file_info; /* file info */

  /* custom VFD code end */

} H5FD_hermes_t;

/* function prototypes*/
std::string get_ohdr_type(H5F_mem_t type);
std::string get_mem_type(H5F_mem_t type);
// char * get_ohdr_type(H5F_mem_t type);
// char * get_mem_type(H5F_mem_t type);



void parseEnvironmentVariable(char* file_path) {
    const char* env_var = std::getenv("HDF5_VOL_CONNECTOR");
    if (env_var) {
        std::string env_string(env_var);

        size_t path_pos = env_string.find("path=");
        size_t level_pos = env_string.find("level=");
        size_t format_pos = env_string.find("format=");

        if (path_pos != std::string::npos) {
            size_t path_start = path_pos + 5;
            size_t path_end = level_pos != std::string::npos ? level_pos - 1 : format_pos - 1;
            std::string path = env_string.substr(path_start, path_end - path_start + 1);
            // std::strcpy(file_path, path.c_str());
            // Remove the semicolon from the file path
            path.erase(std::remove(path.begin(), path.end(), ';'), path.end());
            std::strcpy(file_path, "vfd-"); // use a different file
            std::strcat(file_path, path.c_str());
        } else {
            std::strcpy(file_path, "");
        }

    } else {
        std::strcpy(file_path, "");

    }

  // // Print the parsed values
  // std::cout << "File Path: " << file_path << std::endl;
  // std::cout << "DLife Level: " << vfd_dlife_level << std::endl;
  // std::cout << "DLife Line Format: " << vfd_dlife_line_format << std::endl;
}



/* candice added, print H5FD_mem_t H5FD_MEM_OHDR type more info */
std::string get_ohdr_type(H5F_mem_t type){
// char * get_ohdr_type(H5F_mem_t type){

  if (type == H5FD_MEM_FHEAP_HDR){
    // printf("- Access_Region_Mem_Type : H5FD_MEM_FHEAP_HDR \n");
    return "H5FD_MEM_FHEAP_HDR";

  } else if( type == H5FD_MEM_FHEAP_IBLOCK ){
    // printf("- Access_Region_Mem_Type : H5FD_MEM_FHEAP_IBLOCK \n");
    return "H5FD_MEM_FHEAP_IBLOCK";

  } else if( type == H5FD_MEM_FSPACE_HDR ){
    // printf("- Access_Region_Mem_Type : H5FD_MEM_FSPACE_HDR \n");
    return "H5FD_MEM_FSPACE_HDR";

  } else if( type == H5FD_MEM_SOHM_TABLE  ){
    // printf("- Access_Region_Mem_Type : H5FD_MEM_SOHM_TABLE  \n");
    return "H5FD_MEM_SOHM_TABLE";

  } else if( type == H5FD_MEM_EARRAY_HDR ){
    // printf("- Access_Region_Mem_Type : H5FD_MEM_EARRAY_HDR \n");
    return "H5FD_MEM_EARRAY_HDR";

  } else if( type == H5FD_MEM_EARRAY_IBLOCK ){
    // printf("- Access_Region_Mem_Type : H5FD_MEM_EARRAY_IBLOCK \n");
    return "H5FD_MEM_EARRAY_IBLOCK";

  } else if( type == H5FD_MEM_FARRAY_HDR  ){
    // printf("- Access_Region_Mem_Type : H5FD_MEM_FARRAY_HDR  \n");
    return "H5FD_MEM_FARRAY_HDR";

  } else {
    // printf("- Access_Region_Mem_Type : H5FD_MEM_OHDR \n");
    return "H5FD_MEM_OHDR";
  }
}

std::string get_mem_type(H5F_mem_t type){
// char * get_mem_type(H5F_mem_t type){
  switch(type) {
    case H5FD_MEM_DEFAULT:
      return "H5FD_MEM_DEFAULT";
    case H5FD_MEM_SUPER:
      return "H5FD_MEM_SUPER";
    case H5FD_MEM_BTREE:
      return "H5FD_MEM_BTREE";
    case H5FD_MEM_DRAW:
      return "H5FD_MEM_DRAW";
    case H5FD_MEM_GHEAP:
      return "H5FD_MEM_GHEAP";
    case H5FD_MEM_LHEAP:
      return "H5FD_MEM_LHEAP";
    case H5FD_MEM_NTYPES:
      return "H5FD_MEM_NTYPES";
    case H5FD_MEM_NOLIST:
      return "H5FD_MEM_NOLIST";
    case H5FD_MEM_OHDR:
      return "H5FD_MEM_OHDR";
    default:
      return "H5FD_MEM_UNKNOWN";
  }
}

void add_mem_type_stat(int rw, size_t access_size, H5FD_mem_t type, vfd_file_dlife_info_t * info){
  // r (read) = 1 and w (write) = 2
  switch(type) {
    case H5FD_MEM_DRAW:
      if(rw == 1){
        info->H5FD_MEM_DRAW_read_cnt++;
        info->H5FD_MEM_DRAW_read_bytes+=access_size;
      }
      else if(rw == 2){
        info->H5FD_MEM_DRAW_write_cnt++;
        info->H5FD_MEM_DRAW_write_bytes+=access_size;
      }
      break;
    case H5FD_MEM_OHDR:
      if(rw == 1){
        info->H5FD_MEM_OHDR_read_cnt++;
        info->H5FD_MEM_OHDR_read_bytes+=access_size;
      }
      else if(rw == 2){
        info->H5FD_MEM_OHDR_write_cnt++;
        info->H5FD_MEM_OHDR_write_bytes+=access_size;
      }
      break;
    case H5FD_MEM_SUPER:
      if(rw == 1){
        info->H5FD_MEM_SUPER_read_cnt++;
        info->H5FD_MEM_SUPER_read_bytes+=access_size;
      }
      else if(rw == 2){
        info->H5FD_MEM_SUPER_write_cnt++;
        info->H5FD_MEM_SUPER_write_bytes+=access_size;
      }
      break;
    case H5FD_MEM_BTREE:
      if(rw == 1){
        info->H5FD_MEM_BTREE_read_cnt++;
        info->H5FD_MEM_BTREE_read_bytes+=access_size;
      }
      else if(rw == 2){
        info->H5FD_MEM_BTREE_write_cnt++;
        info->H5FD_MEM_BTREE_write_bytes+=access_size;
      }
      break;
    case H5FD_MEM_LHEAP:
      if(rw == 1){
        info->H5FD_MEM_LHEAP_read_cnt++;
        info->H5FD_MEM_LHEAP_read_bytes+=access_size;
      }
      else if(rw == 2){
        info->H5FD_MEM_LHEAP_write_cnt++;
        info->H5FD_MEM_LHEAP_write_bytes+=access_size;
      }
      break;
    case H5FD_MEM_GHEAP:
      break;
    case H5FD_MEM_NTYPES:
      break;
    case H5FD_MEM_NOLIST:
      break;
    case H5FD_MEM_DEFAULT:
      break;
    default:
      break;
  }
}

void dump_vfd_file_stat_yaml(FILE *f, const vfd_file_dlife_info_t* info){
  if(!info){
    fprintf(f,"dump_vfd_file_stat_yaml(): vfd_file_dlife_info_t is NULL.\n");
    return;
  }

  fprintf(f, "- file-%ld:\n", info->sorder_id);
  fprintf(f, "  file_name: \"%s\"\n", info->file_name);
  fprintf(f, "  open_time: %ld\n", info->open_time);
  fprintf(f, "  close_time: %ld\n", get_time_usec());
  fprintf(f, "  file_intent: [%s]\n", info->intent);
  fprintf(f, "  file_no: %ld\n", info->file_no);
  // fprintf(f, "  fapl_id: %d\n", info->fapl_id);
  fprintf(f, "  file_read_cnt: %d\n", info->file_read_cnt);
  fprintf(f, "  file_write_cnt: %d\n", info->file_write_cnt);
  fprintf(f, "  file_size: %zu\n", info->file_size);
  fprintf(f, "  H5FD_MEM_DRAW: %d\n", info->H5FD_MEM_DRAW_read_cnt);
  fprintf(f, "    read_cnt: %d\n", info->H5FD_MEM_DRAW_read_cnt);
  fprintf(f, "    write_cnt: %d\n", info->H5FD_MEM_DRAW_write_cnt);
  fprintf(f, "    read_bytes: %zu\n", info->H5FD_MEM_DRAW_read_bytes);
  fprintf(f, "    write_bytes: %zu\n", info->H5FD_MEM_DRAW_write_bytes);

  fprintf(f, "  H5FD_MEM_OHDR:\n");
  fprintf(f, "    read_cnt: %d\n", info->H5FD_MEM_OHDR_read_cnt);
  fprintf(f, "    write_cnt: %d\n", info->H5FD_MEM_OHDR_write_cnt);
  fprintf(f, "    read_bytes: %zu\n", info->H5FD_MEM_OHDR_read_bytes);
  fprintf(f, "    write_bytes: %zu\n", info->H5FD_MEM_OHDR_write_bytes);

  fprintf(f, "  H5FD_MEM_SUPER:\n");
  fprintf(f, "    read_cnt: %d\n", info->H5FD_MEM_SUPER_read_cnt);
  fprintf(f, "    write_cnt: %d\n", info->H5FD_MEM_SUPER_write_cnt);
  fprintf(f, "    read_bytes: %zu\n", info->H5FD_MEM_SUPER_read_bytes);
  fprintf(f, "    write_bytes: %zu\n", info->H5FD_MEM_SUPER_write_bytes);

  fprintf(f, "  H5FD_MEM_BTREE:\n");
  fprintf(f, "    read_cnt: %d\n", info->H5FD_MEM_BTREE_read_cnt);
  fprintf(f, "    write_cnt: %d\n", info->H5FD_MEM_BTREE_write_cnt);
  fprintf(f, "    read_bytes: %zu\n", info->H5FD_MEM_BTREE_read_bytes);
  fprintf(f, "    write_bytes: %zu\n", info->H5FD_MEM_BTREE_write_bytes);

  fprintf(f, "  H5FD_MEM_LHEAP:\n");
  fprintf(f, "    read_cnt: %d\n", info->H5FD_MEM_LHEAP_read_cnt);
  fprintf(f, "    write_cnt: %d\n", info->H5FD_MEM_LHEAP_write_cnt);
  fprintf(f, "    read_bytes: %zu\n", info->H5FD_MEM_LHEAP_read_bytes);
  fprintf(f, "    write_bytes: %zu\n", info->H5FD_MEM_LHEAP_write_bytes);

}

std::string getFileIntentFlagsStr(unsigned int flags) {
    std::string intentFlagsStr;

    if (flags & H5F_ACC_RDWR)
        intentFlagsStr += "\"H5F_ACC_RDWR\",";
    else if (flags & H5F_ACC_RDONLY)
        intentFlagsStr += "\"H5F_ACC_RDONLY\",";
    if (flags & H5F_ACC_TRUNC)
        intentFlagsStr += "\"H5F_ACC_TRUNC\",";
    if (flags & H5F_ACC_CREAT)
        intentFlagsStr += "\"H5F_ACC_CREAT\",";
    if (flags & H5F_ACC_EXCL)
        intentFlagsStr += "\"H5F_ACC_EXCL\",";

    // Remove the trailing comma if there are any flags
    if (!intentFlagsStr.empty())
        intentFlagsStr.pop_back();

    return intentFlagsStr;
}

void open_close_info_update(const char* func_name, H5FD_hermes_t *file, size_t eof, int flags)
{
    // H5FD_hermes_t *file = (H5FD_hermes_t *)_file;
    vfd_file_dlife_info_t * info = (vfd_file_dlife_info_t *)file->vfd_file_info;
    if(!info->intent){
      std::string file_intent = getFileIntentFlagsStr(flags);
      info->intent = (char*)malloc((file_intent.length() + 1) * sizeof(char));
      strcpy(info->intent, file_intent.c_str());
    } else if (info->intent[0] == '\0'){
      std::string file_intent = getFileIntentFlagsStr(flags);
      strcpy(info->intent, file_intent.c_str());
      // printf("intent: [%s]\n", info->intent);
    }
    if (!info->file_size || info->file_size <= 0)
      info->file_size = eof;
}


void read_write_info_update(const char* func_name, char * file_name, hid_t fapl_id, H5FD_t *_file,
  H5FD_mem_t type, hid_t dxpl_id, haddr_t addr,
  size_t size, size_t blob_size, unsigned long t_start, unsigned long t_end){

    H5FD_hermes_t *file = (H5FD_hermes_t *)_file;
    vfd_file_dlife_info_t * info = (vfd_file_dlife_info_t *)file->vfd_file_info;

    if (info->file_name == NULL){
      info->file_name = file_name;
    }
    // if (file_info->fapl_id == -1){
    //   file_info->fapl_id = fapl_id;
    // }

    if(strcmp(func_name, read_func) == 0){
      TOTAL_VFD_READ += size;
      info->file_read_cnt++;
      add_mem_type_stat(1, size, type, info);
    }
    if(strcmp(func_name, write_func) == 0){
      TOTAL_VFD_WRITE += size;
      info->file_write_cnt++;
      add_mem_type_stat(1, size, type, info);
    }

    VFD_ACCESS_IDX+=1;

}

/* candice added, print/record info H5FD__hermes_open from */
void print_read_write_info(const char* func_name, char * file_name, hid_t fapl_id, void * obj,
  H5FD_mem_t type, hid_t dxpl_id, haddr_t addr,
  size_t size, size_t blob_size, unsigned long t_start, unsigned long t_end){

  size_t         start_page_index; /* First page index of tranfer buffer */
  size_t         end_page_index; /* End page index of tranfer buffer */
  size_t         num_pages; /* Number of pages of transfer buffer */
  haddr_t        addr_end = addr + size - 1;
  
  // VFD_ADDR = addr;
  start_page_index = addr/blob_size;
  end_page_index = addr_end/blob_size;
  num_pages = end_page_index - start_page_index + 1;

  START_ADDR = addr;
  END_ADDR = addr+size;
  ACC_SIZE = size;
  START_PAGE = start_page_index;
  END_PAGE = end_page_index;
  
  // H5O_token_t token;
  // H5O_info2_t oinfo;
  // H5I_type_t target_obj_type = H5Iget_type(obj);
  // H5I_type_t target_obj_type = H5I_FILE;
  // token = oinfo.token;
  

  // printf("{\"hermes_vfd\": ");
  // printf("{hermes_vfd: ");

  // Note:
  H5FD_t *_file = (H5FD_t *) obj;
  // const H5FD_class_t *f_cls = (H5FD_class_t *) _file->cls;
  // printf("\"f_cls->name\": \"%s\", ", f_cls->name); // "f_cls->fc_degree": "3",
  // printf("\"f_cls->fc_degree\": \"%d\", ", f_cls->fc_degree); //  "f_cls->name": "hermes", 
  // printf("\"f_cls->value\": \"%p\", ", f_cls->value);
  // printf("\"f_cls->dxpl_size\": \"%d\", ", f_cls->dxpl_size);
  // printf("\"f_cls->fapl_size\": \"%d\", ", f_cls->fapl_size);
  
  // printf("\"base_addr\": \"%ld\", ", _file->base_addr);
  // printf("\"maxaddr\": \"%ld\", ", _file->maxaddr);
  // printf("\"threshold\": \"%d\", ", _file->threshold);
  // printf("\"alignment\": \"%d\", ", _file->alignment);

  printf("{\"func_name\": %s, ", func_name);
  printf("\"io_access_idx\": %ld, ", VFD_ACCESS_IDX);
  
  
  // unsigned hash_id = KernighanHash(buf);

  printf("\"time(us)\": %ld, ", t_end);
  
  // printf("\"dset_name\": \"%s\", ", "");
  printf("\"file_no\": %ld, ", _file->fileno); // matches dset_name

  
  // printf("\"obj(decode)\": \"%p\", ", H5Pdecode(obj));
  // printf("\"dxpl_id\": \"%p\", ", dxpl_id);
  // printf("\"hash_id\": %ld, ", KernighanHash(buf));

  
  
  printf("\"addr\": [%ld, %ld], ", addr, (addr+size));
  printf("\"access_size\": %ld, ", size);

  // not used
  printf("\"file_pages\": [%ld, %ld], ", start_page_index,end_page_index);
  printf("\"mem_type\": \"%s\", ", get_mem_type(type).c_str());



  printf("\"TOTAL_VFD_READ\": %ld, ", TOTAL_VFD_READ);
  printf("\"TOTAL_VFD_WRITE\": %ld, ", TOTAL_VFD_WRITE);

  // printf("\"H5FD_HERMES_g\" : \"%p\"", H5FD_HERMES_g);
  // printf("}");


	int mdc_nelmts;
  size_t rdcc_nslots;
  size_t rdcc_nbytes;
  double rdcc_w0;
  if(H5Pget_cache(fapl_id, &mdc_nelmts, &rdcc_nslots, &rdcc_nbytes, &rdcc_w0) > 0){
      printf("\"H5Pget_cache-mdc_nelmts\": %d, ", mdc_nelmts); // TODO: ?
      printf("\"H5Pget_cache-rdcc_nslots\": %ld, ", rdcc_nslots);
      printf("\"H5Pget_cache-rdcc_nbytes\": %ld, ", rdcc_nbytes);
      printf("\"H5Pget_cache-rdcc_w0\": %f, ", rdcc_w0); // TODO: ?
  }

  hsize_t threshold;
  hsize_t alignment;
  H5Pget_alignment(fapl_id, &threshold, &alignment);
  void * buf_ptr_ptr;
  size_t buf_len_ptr;
  H5Pget_file_image(fapl_id, &buf_ptr_ptr, &buf_len_ptr);

  size_t buf_size;
  unsigned min_meta_perc;
  unsigned min_raw_perc;
  if(H5Pget_page_buffer_size(fapl_id, &buf_size, &min_meta_perc, &min_raw_perc) > 0){
      printf("\"H5Pget_page_buffer_size-buf_size\": %ld, ", buf_size);
      printf("\"H5Pget_page_buffer_size-min_meta_perc\": %d, ", min_meta_perc); // TODO: ?
      printf("\"H5Pget_page_buffer_size-min_raw_perc\": %ld, ", min_raw_perc);
  }

  printf("\"file_name\": \"%s\", ", strdup(file_name));
  printf("\"vfd_obj\": %ld, ", obj);

  printf("}\n");


  // printf("{hermes_vfd: ");
  // printf("{func_name: %s, ", func_name);
  // printf("obj: %p, ", obj);
  // printf("obj_addr: %p, ", (void *) &obj);
  // printf("dxpl_id: %p, ", dxpl_id);
  // printf("buf: %p, ", buf);
  // printf("buf_addr: %p, ", (void*)&buf);

  // printf("access_size: %ld, ", size);
  // printf("time(us): %ld, ", t_end);
  // printf("file_name: %p, ", file_name);
  
  // printf("start_address: %ld, ", addr);
  // printf("end_address: %ld, ", addr_end);
  // printf("start_page: %ld, ", start_page_index);
  // printf("end_page: %ld, ", end_page_index);
  // printf("num_pages: %d, ", num_pages);
  // printf("mem_type: %s", get_mem_type(type));
  // printf("}}\n");

  /* record and print end */
  
}


void print_open_close_info(const char* func_name, void * obj, const char * file_name, 
  unsigned long t_start, unsigned long t_end, size_t eof, int flags)
{
  H5FD_t *_file = (H5FD_t *) obj;

  // printf("{\"hermes_vfd\": ");
  // printf("{hermes_vfd: ");


  printf("{\"func_name\": \"%s\", ", func_name);
  printf("\"time(us)\": \"%ld\", ", t_end);
  // printf("start_time(us): %ld, ", t_start);
  // printf("start_end(us): %ld, ", t_end);
  // printf("start_elapsed(us): %ld, ", (t_end - t_start));
  // printf("\"obj\": \"%p\", ", obj);
  // printf("\"obj_addr\": \"%p\", ", (void *) &obj);
  
  printf("\"file_no\": %ld, ", _file->fileno); // matches dset_name

  // printf("\"maxaddr\": %d, ", _file->maxaddr); // 0 or -1
  // printf("\"base_addr\": %ld, ", _file->base_addr); // 0
  // printf("\"threshold\": %ld, ", _file->threshold); // 0 or 1
  // printf("\"alignment\": %ld, ", _file->alignment); // 0 or 1

  printf("\"file_size\": %ld, ", eof);
  
  // printf("\"file_size\": %ld, ", file->eof);
  // printf("\"file_size\": %ld, ", lseek(file->fd, 0, SEEK_END));
  

  /* identify flags */
  printf("\"file_intent\": [");

  if(H5F_ACC_RDWR & flags)
    printf("\"%s\",", "H5F_ACC_RDWR");
  else if(H5F_ACC_RDONLY & flags)
    printf("\"%s\",", "H5F_ACC_RDONLY");
  if (H5F_ACC_TRUNC & flags) {
    printf("\"%s\",", "H5F_ACC_TRUNC");
  }
  if (H5F_ACC_CREAT & flags) {
    printf("\"%s\",", "H5F_ACC_CREAT");
  }
  if (H5F_ACC_EXCL & flags) {
    printf("\"%s\",", "H5F_ACC_EXCL");
  }
  printf("]");

  printf("\"file_name\": \"%s\", ", file_name);

  // printf("}")
  printf("}\n");



  // printf("{hermes_vfd: ");
  // printf("{func_name: %s, ", func_name);
  // // printf("start_time(us): %ld, ", t_start);
  // // printf("start_end(us): %ld, ", t_end);
  // // printf("start_elapsed(us): %ld, ", (t_end - t_start));
  // printf("obj: %p, ", obj);
  // printf("obj_addr: %p, ", (void *) &obj);
  // printf("time(us): %ld, ", t_end);
  // printf("file_name: %s, ", file_name);
  // printf("file_name_hex: %p", file_name);
  // printf("}}\n");

  // initialize & reset
  TOTAL_VFD_READ = 0;
  TOTAL_VFD_WRITE = 0;
  VFD_ACCESS_IDX = 0; 

}

void print_H5Pset_fapl_info(const char* func_name, hbool_t logStat, size_t page_size)
{
  // printf("{\"hermes_vfd\": ");
  // printf("{hermes_vfd: ");
  
  printf("{\"TASK_ID\": \"%d\", ", TASK_ID);
  printf("{\"func_name\": \"%s\", ", func_name);
  printf("\"time(us)\": \"%ld\", ", get_time_usec());
  printf("\"logStat\": \"%s\", ", logStat ? "true" : "false");  
  printf("\"page_size\": \"%ld\", ", page_size);
  // printf("}")
  printf("}\n");
  TASK_ID++;

}






vfd_dlife_helper_t * vfd_dlife_helper_init( char* file_path, size_t page_size, hbool_t logStat)
{
   
    
    vfd_dlife_helper_t* new_helper = (vfd_dlife_helper_t *)calloc(1, sizeof(dlife_helper_t));

    if(logStat) {//write to file
        if(!file_path){
            printf("vfd_dlife_helper_init() failed, vfd-datalife file path is not set.\n");
            return NULL;
        }
    }

    new_helper->dlife_file_path = strdup(file_path);
    new_helper->logStat = logStat;
    new_helper->pid = getpid();
    // print new_helper pid
    printf("vol new_helper pid: %d\n", new_helper->pid);

    new_helper->tid = pthread_self();

    // new_helper->opened_files = NULL;
    // new_helper->opened_files_cnt = 0;

    /* VFD vars start */
    new_helper->vfd_opened_files = NULL;
    new_helper->vfd_opened_files_cnt = 0;
    new_helper->hermes_page_size = page_size;
    /* VFD vars end */

    getlogin_r(new_helper->user_name, 32);

    if(logStat)
        new_helper->dlife_file_handle = fopen(new_helper->dlife_file_path, "a");

    return new_helper;
}



void vfd_file_info_free(vfd_file_dlife_info_t* info);

void vfd_file_info_free(vfd_file_dlife_info_t* info)
{
#ifdef H5_HAVE_PARALLEL
    // TODO: VFD not support parallel yet.
#endif /* H5_HAVE_PARALLEL */
    if(info->file_name)
      free((void*)(info->file_name));
    free(info);
}

vfd_file_dlife_info_t* new_vfd_file_info(const char* fname, unsigned long file_no)
{
    vfd_file_dlife_info_t *info;

    info = (vfd_file_dlife_info_t *)calloc(1, sizeof(vfd_file_dlife_info_t));

    info->file_name = fname ? strdup(fname) : NULL;
    // info->file_name = malloc(sizeof(char) * (strlen(fname) + 1));
    // strcpy(info->file_name, fname);
    info->file_no = file_no;

    // dlLockAcquire(&myLock);
    info->sorder_id =++FILE_SORDER;
    // dlLockRelease(&myLock);

    info->open_time = get_time_usec();
    info->file_read_cnt = 0;
    info->file_write_cnt = 0;

    info->H5FD_MEM_DRAW_read_cnt = 0;
    info->H5FD_MEM_DRAW_write_cnt = 0;
    info->H5FD_MEM_DRAW_read_bytes = 0;
    info->H5FD_MEM_DRAW_write_bytes = 0;

    info->H5FD_MEM_OHDR_read_cnt = 0;
    info->H5FD_MEM_OHDR_write_cnt = 0;
    info->H5FD_MEM_OHDR_read_bytes = 0;
    info->H5FD_MEM_OHDR_write_bytes = 0;

    info->H5FD_MEM_SUPER_read_cnt = 0;
    info->H5FD_MEM_SUPER_write_cnt = 0;
    info->H5FD_MEM_SUPER_read_bytes = 0;
    info->H5FD_MEM_SUPER_write_bytes = 0;

    info->H5FD_MEM_BTREE_read_cnt = 0;
    info->H5FD_MEM_BTREE_write_cnt = 0;
    info->H5FD_MEM_BTREE_read_bytes = 0;
    info->H5FD_MEM_BTREE_write_bytes = 0;

    info->H5FD_MEM_LHEAP_read_cnt = 0;
    info->H5FD_MEM_LHEAP_write_cnt = 0;
    info->H5FD_MEM_LHEAP_read_bytes = 0;
    info->H5FD_MEM_LHEAP_write_bytes = 0;

    return info;
}

vfd_file_dlife_info_t* add_vfd_file_node(vfd_dlife_helper_t * helper, const char* file_name, void * obj)
{
  // std::cout << "add_vfd_file_node(): DLIFE_HELPER_VFD" << std::endl;
  unsigned long start = get_time_usec();
  vfd_file_dlife_info_t* cur;
  H5FD_t *_file = (H5FD_t *) obj;
  unsigned long file_no = _file->fileno;

  assert(helper);

  if(!helper->vfd_opened_files) //empty linked list, no opened file.
      assert(helper->vfd_opened_files_cnt == 0);

  // Search for file in list of currently opened ones
  cur = helper->vfd_opened_files;
  while (cur) {
      assert(cur->file_no);

      if (cur->file_no == file_no)
          break;

      cur = cur->next;
  }

  // Allocate and initialize new file node
  cur = new_vfd_file_info(file_name, file_no);

  // Add to linked list
  cur->next = helper->vfd_opened_files;
  helper->vfd_opened_files = cur;
  helper->vfd_opened_files_cnt++;

  // Increment refcount on file node
  cur->ref_cnt++;

  // FILE_LL_TOTAL_TIME += (get_time_usec() - start);
  return cur;
}


int rm_vfd_file_node(vfd_dlife_helper_t* helper, H5FD_t *_file);

//need a dumy node to make it simpler
int rm_vfd_file_node(vfd_dlife_helper_t* helper, H5FD_t *_file)
{
  // printf("rm_vfd_file_node(): DLIFE_HELPER_VFD\n");
  unsigned long start = get_time_usec();
  vfd_file_dlife_info_t* cur;
  vfd_file_dlife_info_t* last;
  unsigned long file_no = _file->fileno;

  assert(helper);
  assert(helper->vfd_opened_files);
  assert(helper->vfd_opened_files_cnt);
  assert(file_no);

  cur = helper->vfd_opened_files;
  last = cur;
  while(cur) {
      // Node found
      if(cur->file_no == file_no) {
          // Decrement file node's refcount
          cur->ref_cnt--;

          // If refcount == 0, remove file node & maybe print file stats
          if(cur->ref_cnt == 0) {

              // Unlink from list of opened files
              if(cur == helper->vfd_opened_files) //first node is the target
                  helper->vfd_opened_files = helper->vfd_opened_files->next;
              else
                  last->next = cur->next;

              // Free file info
              vfd_file_info_free(cur);

              // Update connector info
              helper->vfd_opened_files_cnt--;
              if(helper->vfd_opened_files_cnt == 0)
                  assert(helper->vfd_opened_files == NULL);
          }

          break;
      }

      // Advance to next file node
      last = cur;
      cur = cur->next;
  }

  // FILE_LL_TOTAL_TIME += (get_time_usec() - start);
  return helper->vfd_opened_files_cnt;
}

void vfd_dlife_helper_teardown(vfd_dlife_helper_t* helper){
  // printf("vfd_dlife_helper_teardown(): DLIFE_HELPER_VFD\n");

  if(helper){// not null

      if(helper->logStat){//no file
          fflush(helper->dlife_file_handle);
          fclose(helper->dlife_file_handle);
      }
      if(helper->dlife_file_path)
          free(helper->dlife_file_path);

      free(helper);
  }

}
