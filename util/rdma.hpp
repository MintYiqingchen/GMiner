/*
 * Copyright (c) 2016 Shanghai Jiao Tong University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/projects/wukong
 *
 */

#pragma once
#pragma GCC diagnostic warning "-fpermissive"

#include <vector>
#include <string>
#include <iostream>     // std::cout
#include <fstream>      // std::ifstream
using namespace std;

#ifdef HAS_RDMA

// #include "utils/timer.hpp"
#include "rdmalib/rdmaio.hpp"
#include "util/global.hpp"

using namespace rdmaio;

class RDMA {
    class RDMA_Device {
        public:
            RdmaCtrl* ctrl = NULL;

            RDMA_Device(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<RdmaNodeInfo> & node_info);

            // 0 on success, -1 otherwise
            int RdmaRead(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off);

            int RdmaWrite(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off);

            int RdmaWriteSelective(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off);

            int RdmaWriteNonSignal(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
                Qp* qp = ctrl->get_rc_qp(dst_tid, dst_nid);
                int flags = 0;
                qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);
                return 0;
            }
        private:
            int num_threads_;
    };
    
    RDMA() { }

    ~RDMA() { 
        if (dev != NULL) delete dev;
        if (pd != NULL) delete pd;
    }

public:
    RDMA_Device *dev = NULL;
    void* pd = NULL;
    uint64_t pd_size;

    void init_dev(uint64_t pds, int num_threads) {
        pd_size = pds;
        pd = new char[pd_size];
        dev = new RDMA_Device(_num_workers, num_threads, _my_rank, pd, pd_size, _global_rdma_infos);
    }

    inline static bool has_rdma() { return true; }

    static RDMA &get_rdma() {
        static RDMA rdma;
        return rdma;
    }
};

#endif

void RDMA_init();