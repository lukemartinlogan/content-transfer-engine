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
// #include "H5FDhermes_err.h" /* error handling         */

// #ifdef ENABLE_HDF5_IO_LOGGING
#include "/qfs/people/tang584/scripts/local-co-scheduling/vol-datalife/src/datalife_vol_types.h" /* Connecting to vol         */
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

/************/
/* Typedefs */
/************/


// /* The driver identification number, initialized at runtime */
// static hid_t H5FD_HERMES_g = H5I_INVALID_HID;


static
unsigned long get_time_usec(void) {
    struct timeval tp;

    gettimeofday(&tp, NULL);
    return (unsigned long)((1000000 * tp.tv_sec) + tp.tv_usec);
}

/* function prototypes*/
std::string get_ohdr_type(H5F_mem_t type);
std::string get_mem_type(H5F_mem_t type);

/* candice added, print H5FD_mem_t H5FD_MEM_OHDR type more info */
std::string get_ohdr_type(H5F_mem_t type){

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
  char * type_str; //[20];
  switch(type) {
    case H5FD_MEM_DEFAULT:
      type_str = strdup("H5FD_MEM_DEFAULT");
      break;
    case H5FD_MEM_SUPER:
      type_str = strdup("H5FD_MEM_SUPER");
      break;
    case H5FD_MEM_BTREE:
      type_str = strdup("H5FD_MEM_BTREE");
      break;
    case H5FD_MEM_DRAW:
      type_str = strdup("H5FD_MEM_DRAW");
      break;
    case H5FD_MEM_GHEAP:
      type_str = strdup("H5FD_MEM_GHEAP");
      break;
    case H5FD_MEM_LHEAP:
      type_str = strdup("H5FD_MEM_LHEAP");
      break;
    case H5FD_MEM_NTYPES:
      type_str = strdup("H5FD_MEM_NTYPES");
      break;
    case H5FD_MEM_NOLIST:
      type_str = strdup("H5FD_MEM_NOLIST");
      break;
    case H5FD_MEM_OHDR:
      type_str = strdup("H5FD_MEM_OHDR");
      break;
    default:
      type_str = strdup("H5FD_MEM_DEFAULT");
      break;
  }
  return type_str;
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

  if(strcmp(func_name, read_func) == 0){
    TOTAL_VFD_READ += size;
  }
  if(strcmp(func_name, write_func) == 0){
    TOTAL_VFD_WRITE += size;
  }

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

  VFD_ACCESS_IDX+=1;

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

void print_H5Pset_fapl_info(const char* func_name, hbool_t persistence, size_t page_size)
{
  // printf("{\"hermes_vfd\": ");
  // printf("{hermes_vfd: ");
  
  printf("{\"TASK_ID\": \"%d\", ", TASK_ID);
  printf("{\"func_name\": \"%s\", ", func_name);
  printf("\"time(us)\": \"%ld\", ", get_time_usec());
  printf("\"persistence\": \"%s\", ", persistence ? "true" : "false");  
  printf("\"page_size\": \"%ld\", ", page_size);
  // printf("}")
  printf("}\n");
  TASK_ID++;

}

