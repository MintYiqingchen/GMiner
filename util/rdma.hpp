#ifndef RDMA_HPP_
#define RDMA_HPP_

#ifdef HAS_RDMA
#include <vector>
#include <string>
#include <iostream>     // std::cout
#include <fstream>      // std::ifstream
#include <thread>
#include <mutex>        // mutex
#include <unordered_map>
#include <stdlib.h>     // itoa
#include <string.h>     // memcpy, memset
#include <netdb.h>      // gai_strerror
#include <arpa/inet.h> // inet_ntop
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <util/serialization.hpp>
using namespace std;
struct RemoteReadMeta {
    uint64_t remote_addr;
    uint32_t rkey;
    size_t length;
};

class RdmaMgr {
    private:
        thread serv_thread_;
        bool inited, stop;
        int rank_;

        mutex table_mutex_;
        vector<unordered_map<int, rdma_cm_id*>> posi_conn_table_; // , pass_conn_table_;
        rdma_cm_id* listen_id_;
        vector<RdmaNodeInfo> rdma_infos_;
        RdmaMgr():inited(false), stop(false){};
        ~RdmaMgr() {}
    public:
        void serverListen();
        int clientConnect(int nid, int comm_tag);
        static RdmaMgr& Get(){
            static RdmaMgr rdma_mgr_;
            return rdma_mgr_;
        }
        void Start(int rank, const vector<RdmaNodeInfo>& global_rdma_infos){
            if (!inited){
                rank_ = rank;
                rdma_infos_ = global_rdma_infos;
                posi_conn_table_.resize(rdma_infos_.size());
                inited = true;
                thread listen(&RdmaMgr::serverListen, this);
                serv_thread_.swap(listen);
            }
        }
        void Stop(){
            stop = true;
            serv_thread_.detach();
        }
        int send(ibinstream* stream, int nid, int comm_id);
        int recv_obinstream(int nid, int comm_id, obinstream& stream);
};
#include "util/rdma.tpp"
#else
class RdmaMgr{
    public:
        static RdmaMgr& Get(){
            static RdmaMgr rdma_mgr_;
            return rdma_mgr_;
        }
        void Start(int rank, const vector<RdmaNodeInfo>& global_rdma_infos){}
        void Stop(){}
        void serverListen(){}
        int clientConnect(int nid, int comm_tag){}
        int RdmaMgr::send(ibinstream* stream, int nid, int comm_id);
        int RdmaMgr::recv_obinstream(int nid, int comm_id, obinstream& stream);
}
#endif
#endif

