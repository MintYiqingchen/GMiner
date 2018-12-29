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

#ifndef RDMA_HPP_
#define RDMA_HPP_

#include <vector>
#include <string>
#include <iostream>     // std::cout
#include <fstream>      // std::ifstream
#include "util/global.hpp"
using namespace std;

#ifdef HAS_RDMA

#include "rdmalib/rdmaio.hpp"

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
    char* pd = NULL;
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

void RDMA_init(uint64_t pd_size, int num_threads) {
    // uint64_t t = timer::get_usec();

    // init RDMA device
    RDMA &rdma = RDMA::get_rdma();
    rdma.init_dev(pd_size, num_threads);

    // t = timer::get_usec() - t;
    // cout << "INFO: initializing RMDA done (" << t / 1000  << " ms)" << endl;
    cout << "INFO: initializing RMDA done" << endl;
}

#else

void RDMA_init(uint64_t pd_size, int thread_num) {
    std::cout << "This system is compiled without RDMA support." << std::endl;
}

#endif

#include "util/rdma.tpp"
#endif

