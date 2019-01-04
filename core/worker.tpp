//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class Master, class Slave, class AggregatorT>
Worker<Master, Slave, AggregatorT>::Worker()
{
	global_aggregator = NULL;
	global_agg = NULL;
}

template <class Master, class Slave, class AggregatorT>
void Worker<Master, Slave, AggregatorT>::set_aggregator(AggregatorT* ag, FinalT init_value)
{
	global_aggregator = ag;
	global_agg = new FinalT;
	*((FinalT*)global_agg) = init_value;
}

#include <fstream>
vector<RdmaNodeInfo> read_ib_cfg(){
    ifstream ib_file("ibs.cfg", ios_base::in);
    vector<RdmaNodeInfo> res;
    while(ib_file.good()){
        string r;
        ib_file >> r;
        res.push_back(RdmaNodeInfo(r, RDMA_PORT));
    }
    ib_file.close();
    return res;
}

void client_connect_all_channel(int nid){
    if(RdmaMgr::Get().clientConnect(nid, REQUEST_CHANNEL)){
		USE_RDMA = false;
	}
    if(RdmaMgr::Get().clientConnect(nid, RESPOND_CHANNEL)){
		USE_RDMA = false;
	}
}
template <class Master, class Slave, class AggregatorT>
void Worker<Master, Slave, AggregatorT>::run(const WorkerParams& params)
{
	if(USE_RDMA){
        vector<RdmaNodeInfo> infos = read_ib_cfg();
		RdmaMgr::Get().Start(_my_rank, infos);
		vector<thread> threads;
		for(int i = 0; i < _my_rank; ++ i){
			threads.push_back(std::move(thread(client_connect_all_channel, i)));
		}
		for(auto & th: threads)
		 	th.join();
        USE_RDMA = all_land(USE_RDMA);
        cout << USE_RDMA;
	}
	if (_my_rank == MASTER_RANK)
	{
		MasterT master;
		master.run(params);
	}
	else
	{
		SlaveT slave;
		slave.run(params);
	}
    RdmaMgr::Get().Stop();
}
