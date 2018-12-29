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
#ifdef HAS_RDMA
// ===================== RDMA_Device ======================
RDMA::RDMA_Device::RDMA_Device(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<RdmaNodeInfo> & node_infos) : num_threads_(num_threads){
    // record IPs of ndoes
    vector<string> ipset;
    for(const auto & node: node_infos)
        ipset.push_back(node.ibname);

    int rdma_port = node_infos[nid].rdma_port;
    // initialization of new librdma
    // nid, ipset, port, thread_id-no use, enable single memory region
    ctrl = new RdmaCtrl(nid, ipset, rdma_port, true);
    ctrl->open_device();
    ctrl->set_connect_mr(mem, mem_sz);
    ctrl->register_connect_mr();//single
    ctrl->start_server();
    for (uint j = 0; j < num_threads * 2; ++j) {
        for (uint i = 0; i < num_nodes; ++i) {
            Qp *qp = ctrl->create_rc_qp(j, i, 0, 1);
            assert(qp != NULL);
        }
    }

    while (1) {
        int connected = 0;
        for (uint j = 0; j < num_threads * 2; ++j) {
            for (uint i = 0; i < num_nodes; ++i) {
                Qp *qp = ctrl->create_rc_qp(j, i, 0, 1);
                if (qp->inited_) connected += 1;
                else {
                    if (qp->connect_rc()) {
                        connected += 1;
                    }
                }
            }
        }
        if (connected == num_nodes * num_threads * 2)
            break;
        else
            sleep(1);
    }
}
int RDMA::RDMA_Device::RdmaRead(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off){
    // virtual tid for read
    int vir_tid = dst_tid + num_threads_;

    Qp* qp = ctrl->get_rc_qp(vir_tid, dst_nid);
    qp->rc_post_send(IBV_WR_RDMA_READ, local, size, off, IBV_SEND_SIGNALED);
    if (!qp->first_send())
        qp->poll_completion();

    qp->poll_completion();
    return 0;
}
int RDMA::RDMA_Device::RdmaWrite(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
    Qp* qp = ctrl->get_rc_qp(dst_tid, dst_nid);

    // int flags = (qp->first_send() ? IBV_SEND_SIGNALED : 0);
    int flags = IBV_SEND_SIGNALED;
    qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);

    // if(qp->need_poll())
    qp->poll_completion();

    return 0;
    // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_WRITE);
}
int RDMA::RDMA_Device::RdmaWriteSelective(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
    Qp* qp = ctrl->get_rc_qp(dst_tid, dst_nid);
    int flags = (qp->first_send() ? IBV_SEND_SIGNALED : 0);
    // int flags = IBV_SEND_SIGNALED;

    qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);

    if (qp->need_poll()) qp->poll_completion();
        return 0;
    // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_WRITE);
}
// =========================================================

#endif


