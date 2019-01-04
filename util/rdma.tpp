#define MAX_ACCEPT_CONNECTION 1024
#define MAX_CQE_NUM 1024
#include <queue>

int RdmaMgr::send(ibinstream* stream, int nid, int comm_id){
    rdma_cm_id* id = posi_conn_table_[nid][comm_id];
    int ret;
    RemoteReadMeta meta;
    // 1. prepare send local addr, rkey, size
    ibv_mr* stream_mr = rdma_reg_msgs(id, stream->get_buf(), stream->size());
    RemoteReadMeta send_meta = {
        .remote_addr = reinterpret_cast<uintptr_t>(stream->get_buf()),
        .rkey = stream_mr->rkey,
        .length = stream->size()
    };
    ibv_mr* send_mr = rdma_reg_msgs(id, &send_meta, sizeof send_meta);
    if(send_mr == NULL){
        perror("metadata send mr failure");
        rdma_dereg_mr(stream_mr);
        return -1;
    }
    ret = rdma_post_send(id, NULL, &send_meta, sizeof send_meta, send_mr, 0);
    if(ret){
        perror("metadata post send failure");
        rdma_dereg_mr(send_mr);
        rdma_dereg_mr(stream_mr);
        return ret;
    }
    // 3. recv ack
    ibv_recv_wr ack_wr, *bad_wr;
    ibv_wc wc;
    ret = ibv_post_recv(id->qp, &ack_wr, &bad_wr);
    ret = ibv_poll_cq(id->recv_cq, 1, &wc);
    // 4. release resource
    rdma_dereg_mr(send_mr);
    rdma_dereg_mr(stream_mr);
    return ret;
}
int RdmaMgr::recv_obinstream(int nid, int comm_id, obinstream& stream){
    rdma_cm_id* id = posi_conn_table_[nid][comm_id];
    int ret;
    RemoteReadMeta meta;
    // 1. prepare recv remote addr, rkey, size
    ibv_mr* recv_mr = rdma_reg_msgs(id, &meta, sizeof meta);
    if(recv_mr == NULL){
        perror("metadata recv mr failure");
        return -1;
    }
    ret = rdma_post_recv(id, NULL, &meta, sizeof meta, recv_mr);
    printf("here\n");
    if(ret){
        perror("metadata post recv failure");
        rdma_dereg_mr(recv_mr);
        return ret;
    }
    // 2. wait response metadata
    ibv_wc recv_wc;
    ret = ibv_poll_cq(id->recv_cq, 1, &recv_wc);
    rdma_dereg_mr(recv_mr);
    if(ret == 0){
        perror("metadata recv comp failure");
        return -1;
    }
    printf("metadata recv comp\n");
    // 3. post a read
    char *resp = new char[meta.length];
    ibv_mr *read_mr = rdma_reg_read(id, resp, meta.length);
    if(read_mr == NULL){
        perror("response read mr failure");
    }
    printf("read reg\n");
    ret = rdma_post_read(id, NULL, resp, meta.length, read_mr, 0, meta.remote_addr, meta.rkey);
    if(ret){
        perror("post read failure");
        rdma_dereg_mr(read_mr);
        delete [] resp;
    }
    printf("post read\n");
    ret = ibv_poll_cq(id->recv_cq, 1, &recv_wc);
    printf("read finish\n");
    // 4. send ack
    ibv_send_wr wr, *bad_wr;
    memset(&wr, 0, sizeof wr);
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.send_flags = IBV_SEND_INLINE;
    ret = ibv_post_send(id->qp, &wr, &bad_wr);
    ret = ibv_poll_cq(id->send_cq, 1, &recv_wc);
    // 6. release resource
    rdma_dereg_mr(read_mr);
    stream = obinstream(resp, meta.length);
    return ret;
}
int RdmaMgr::clientConnect(int nid, int comm_tag){
    lock_guard<mutex> lk(table_mutex_);
    if(posi_conn_table_[nid].find(comm_tag) != posi_conn_table_[nid].end())
        return 0;

	ibv_qp_init_attr attr;
    rdma_cm_id *id;
    int port = rdma_infos_[nid].rdma_port;
    const char* ip = rdma_infos_[nid].hostname.data();

    // 1. create resource
    int device_num;
    ibv_context** contexts = rdma_get_devices(&device_num);
    if(contexts == NULL || device_num == 0)
        perror("rdma_get_devices");
    ibv_context* context = contexts[0];
    ibv_pd *pd = ibv_alloc_pd(context);
    if(pd == NULL){
        perror("ibv_alloc_pd");
        return -1;
    }
    // 2. set sockaddr
    struct sockaddr_in mysock;
    bzero(&mysock,sizeof(mysock));
    mysock.sin_family = AF_INET;
    mysock.sin_port = htons(port);
    mysock.sin_addr.s_addr = inet_addr(ip);
    
    // 3. query addr
    rdma_event_channel* channel = rdma_create_event_channel();
    int ret = rdma_create_id(channel, &id, NULL, RDMA_PS_TCP);
    if(ret){
        perror("rdma create id");
        return -1;
    }
    ret = rdma_resolve_addr(id, NULL, (sockaddr*)(&mysock), 2000);
    if(ret){
        perror("rdma resolve addr");
        return ret;
    }
    rdma_cm_event* event;
    ret = rdma_get_cm_event(channel, &event);
    if(ret){
        perror("rdma resolve addr");
        return ret;
    }
    // printf("%s\n", rdma_event_str(event->event));
    ret = rdma_ack_cm_event(event);

    // 4. create qp
    ibv_cq* recv_cq = ibv_create_cq(context, 2, NULL, NULL, NULL);
    ibv_cq* send_cq = ibv_create_cq(context, 2, NULL, NULL, NULL);
    memset(&attr, 0, sizeof attr);
	attr.cap.max_send_wr = attr.cap.max_recv_wr = 1;
	attr.cap.max_send_sge = attr.cap.max_recv_sge = 1;
	attr.cap.max_inline_data = 16;
	attr.qp_context = id;
	attr.sq_sig_all = 0;
    attr.send_cq = send_cq;
    attr.recv_cq = recv_cq;
    attr.qp_type = IBV_QPT_RC;
    ret = rdma_create_qp(id, pd, &attr);
    if(ret){
        perror("rdma create qp");
        return ret;
    }
    // 5. query route
    ret = rdma_resolve_route(id, 2000);
    if(ret){
        perror("rdma resolve route");
        return ret;
    }
    ret = rdma_get_cm_event(channel, &event);
    if(ret){
        perror("rdma resolve route");
        return ret;
    } // printf("%s\n", rdma_event_str(event->event));
    ret = rdma_ack_cm_event(event);

    // 6. wait connect
    ret = rdma_connect(id, NULL);
    if(ret){
        perror("rdma connect");
        return ret;
    }
    ret = rdma_get_cm_event(channel, &event);
    if(ret){
        perror("rdma get connect event");
        return ret;
    }
    if(event->event != RDMA_CM_EVENT_ESTABLISHED){
        cout << nid;
        printf(" %s\n", rdma_event_str(event->event));
        ret = rdma_ack_cm_event(event);
        return -1;
    }
    event->id = NULL;
    ret = rdma_ack_cm_event(event);

    /*
    // 7. send nid and comm_tag
    ibv_send_wr wr, *bad_wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uint64_t)comm_tag;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.imm_data = htonl((uint32_t)nid);
    ibv_sge sg;
    memset(&sg, 0, sizeof sg);
    wr.sg_list = &sg;
    if(ibv_post_send(id->qp, &wr, &bad_wr)){
        perror("send nid failure");
        return -1;
    }
    ibv_wc wc;
    ret = ibv_poll_cq(id->send_cq, 1, &wc);
    // 8. recv ack
    ibv_recv_wr recv_wr, *bad_recv_wr;
    memset(&recv_wr, 0, sizeof(recv_wr));
    recv_wr.wr_id = wr.wr_id;
    if(ibv_post_recv(id->qp, &recv_wr, &bad_recv_wr)){
        perror("recv connect ack failure");
        return -1;
    }
    ret = ibv_poll_cq(id->recv_cq, 1, &wc);
    if(ntohl(wc.imm_data) != 0){
        rdma_disconnect(id);
        return -1;
    }
    posi_conn_table_[nid][comm_tag] = id;
	return ret;
    */
    return -1;
}
void RdmaMgr::serverListen(){
    const char *server = rdma_infos_[rank_].hostname.data();
    int port = rdma_infos_[rank_].rdma_port;
    
    // 1. create source
    int device_num;
    ibv_context** contexts = rdma_get_devices(&device_num);
    if(device_num == 0 || contexts == NULL){
        perror("rdma_get_devices");
        return;
    }
    
    // 2. set sockaddr
    struct sockaddr_in mysock;
    bzero(&mysock,sizeof(mysock));
    mysock.sin_family = AF_INET;
    mysock.sin_port = htons(port);
    mysock.sin_addr.s_addr = inet_addr(server);
    
    // 3. bind a id 
    rdma_event_channel* channel = rdma_create_event_channel();
    int ret = rdma_create_id(channel, &listen_id_, NULL, RDMA_PS_TCP);
    if(ret){
        perror("rdma create id");
        return;
    }
    ret = rdma_bind_addr(listen_id_, (sockaddr*)(&mysock));
    if(ret){
        perror("rdma bind");
        return;
    }
    // 4. listen connection request
    ret = rdma_listen(listen_id_, MAX_ACCEPT_CONNECTION);
    if(ret){
        perror("rdma listen");
        return;
    }
    
    while(!stop){
        rdma_cm_event* event;
        ret = rdma_get_cm_event(channel, &event);
        if(ret){
            perror("rdma cm event");
            continue;
        }
        rdma_cm_id *id = event->id;
        event->id = NULL;
        rdma_ack_cm_event(event);
        // 5. alloc a qp with new_id
        ibv_context* context = contexts[0];
        ibv_cq* recv_cq = ibv_create_cq(context, 2, NULL, NULL, NULL);
        ibv_cq* send_cq = ibv_create_cq(context, 2, NULL, NULL, NULL);
        if(recv_cq == NULL || send_cq == NULL){
            rdma_reject(id, NULL, 0);
            perror("server create cq");
            continue;
        }
        ibv_qp_init_attr attr;
        memset(&attr, 0, sizeof attr);
        attr.cap.max_send_wr = attr.cap.max_recv_wr = 1;
        attr.cap.max_send_sge = attr.cap.max_recv_sge = 1;
        attr.cap.max_inline_data = 16;
        attr.qp_context = id;
        attr.sq_sig_all = 0;
        attr.send_cq = send_cq;
        attr.recv_cq = recv_cq;
        attr.qp_type = IBV_QPT_RC;
        if(id->qp)
            rdma_destroy_qp(id);
        char tmp[10];
        sprintf(tmp, "%d %d %d", (id->qp == NULL), (id->recv_cq == NULL), (id->pd == NULL));
        // cout << (id->qp == NULL) << " "<< (id->recv_cq == NULL) << " ";
        // cout << (id->pd == NULL) << endl;
        ret = rdma_create_qp(id, NULL, &attr);
        if(ret){
            rdma_reject(id, NULL, 0);
            perror("server rdma create qp");
            continue;
        }  
        rdma_accept(id, NULL);

        /*
        // 6. recv nid(imm_data) and comm_tag(wr_id)
        ibv_recv_wr recv_wr, *bad_recv_wr;
        memset(&recv_wr, 0, sizeof(recv_wr));
        while(ibv_post_recv(id->qp, &recv_wr, &bad_recv_wr)){
            perror("recv connect ack failure");
        }
        ibv_wc wc;
        ret = ibv_poll_cq(id->recv_cq, 1, &wc);
        int nid = ntohl(wc.imm_data);
        int comm_tag = wc.wr_id;
        cout << nid <<" "<<comm_tag <<endl;

        lock_guard<mutex> lk(table_mutex_);
        if(posi_conn_table_[nid].find(comm_tag) != posi_conn_table_[nid].end()){
            ibv_send_wr wr, *bad_wr;
            memset(&wr, 0, sizeof(wr));
            wr.wr_id = (uint64_t)comm_tag;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.opcode = IBV_WR_SEND_WITH_IMM;
            ibv_sge sg;
            memset(&sg, 0, sizeof sg);
            wr.sg_list = &sg;
            ibv_post_send(id->qp, &wr, &bad_wr);
            ibv_poll_cq(id->send_cq, 1, &wc);
            rdma_disconnect(id);
        }
        else{        
            posi_conn_table_[nid][comm_tag] = id;
        }
        */
    }
    // TODO(mintyi) free objects
}
