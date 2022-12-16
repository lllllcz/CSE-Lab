#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <random>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template <typename state_machine, typename command>
class raft {
    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

// #define RAFT_LOG(fmt, args...)
//     do {
//     } while (0);

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while (0);

public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:

    /* ----Persistent state on all server----  */
    int vote_for = 0;
    std::vector<log_entry<command>> log;
    std::vector<char> snapshot;

    /* ---- Volatile state on all server----  */
    int commit_index = 0;
    int last_applied = 0;

    /* ---- Volatile state on leader----  */
    std::vector<int> next_index;
    std::vector<int> match_index;

    int vote_count; // how many vote me
    std::vector<bool> voted_nodes; // who vote me

    // time
    std::chrono::system_clock::time_point last_action_time;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    std::chrono::system_clock::duration get_random_timeout(int lower_bound);

    inline int get_term(int index);
    std::vector<log_entry<command>> get_entries(int begin_index, int end_index);

};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization

    current_term = 0;
    vote_for = -1;
    log.assign(1, log_entry<command>());

    storage->restore_all(current_term, vote_for, log, snapshot);

    // volatile states
    commit_index = log.front().index;
    last_applied = log.front().index;

    // leader volatile states
    next_index.assign(num_nodes(), 1);
    match_index.assign(num_nodes(), 0);

    // candidate volatile states
    voted_nodes.assign(num_nodes(), false);

    // initialize time
    last_action_time = std::chrono::system_clock::now();
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here

//    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
//    term = current_term;
//    return true;

    std::unique_lock<std::mutex> lock(mtx);
    if (!is_leader(current_term)) {
        return false;
    }

    // no election, no new term
    term = current_term;
    // new command, new index
    index = log.back().index + 1;

    // persist log
    log_entry<command> entry(index, term, cmd);
    log.push_back(entry);
    storage->persist_log(log);

    next_index[my_id] = index + 1;
    match_index[my_id] = index;

    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    return false;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here

    std::unique_lock<std::mutex> lock(mtx);

    if (args.term < current_term) {
        return 0;
    }

    last_action_time = std::chrono::system_clock::now();
    reply.term = current_term;
    reply.voteGranted = false;

    if (args.term > current_term) {
        // set follower
        role = follower;
        current_term = args.term;
        vote_for = -1;

        storage->persist_metadata(current_term, vote_for);
    }

    if (
        (vote_for == -1 || vote_for == args.candidateId)
        &&
        (
            args.lastLogTerm > log.back().term
            ||
            (args.lastLogTerm == log.back().term && args.lastLogIndex >= log.back().index)
        )
    )
    {
        vote_for = args.candidateId;
        reply.voteGranted = true;

        storage->persist_metadata(current_term, vote_for);
    }
    
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply) {
    // Lab3: Your code here

    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        // set follower
        current_term = reply.term;
        role = follower;
        vote_for = -1;

        storage->persist_metadata(current_term, vote_for);
        return;
    }
    if (role != candidate) {
        return;
    }

    if (reply.voteGranted && !voted_nodes[target]) {
        voted_nodes[target] = true;
        ++vote_count;

        if (vote_count > num_nodes() / 2) {
            // set leader

            role = leader;

            int lastIndex = log.back().index;

            // reinitialize leader volatile states
            next_index.assign(num_nodes(), lastIndex + 1);
            match_index.assign(num_nodes(), 0);
            match_index[my_id] = lastIndex;

            // send initial ping
            for (int i = 0; i < num_nodes(); ++i) {
                if (i == my_id) continue;
                append_entries_args<command> args(current_term, my_id, commit_index);
                args.prevLogIndex = lastIndex;
                args.prevLogTerm = get_term(args.prevLogIndex);
                thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
            }
        }
    }

    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here

    std::unique_lock<std::mutex> lock(mtx);

    last_action_time = std::chrono::system_clock::now();
    reply.term = current_term;

    if (arg.term < current_term) {
        return 0;
    }

    if (arg.term > current_term || role == candidate) {
        // new term begin
        // set follower
        current_term = arg.term;
        vote_for = -1;
        role = follower;

        storage->persist_metadata(current_term, vote_for);
    }
    if (arg.prevLogIndex <= log.back().index && arg.prevLogTerm == get_term(arg.prevLogIndex)) {
        if (!arg.entries.empty()) {
            if (arg.prevLogIndex + 1 <= log.back().index) {
                auto start_pos = log.begin();
                start_pos = start_pos + arg.prevLogIndex + 1 - log.front().index;
                log.erase(start_pos, log.end());
            }
            for (auto a_entry : arg.entries) {
                log.push_back(a_entry);
            }
            storage->persist_log(log);
        }

        if (arg.leaderCommit > commit_index) {
            // update commit index
            commit_index = (arg.leaderCommit >= log.back().index) ? log.back().index : arg.leaderCommit;
        }

        reply.success = true;
    }

    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply) {
    // Lab3: Your code here

    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        // set self to follower
        role = follower;
        vote_for = -1;
        current_term = reply.term;

        storage->persist_metadata(current_term, vote_for);
        return;
    }
    if (!is_leader(current_term)) {
        return;
    }

    if (reply.success) {
        int node_match_index = match_index[node];
        int node_current_index = arg.prevLogIndex + arg.entries.size();
        match_index[node] = (node_match_index >= node_current_index) ? node_match_index : node_current_index;
        next_index[node] = match_index[node] + 1;

        std::vector<int> vec = match_index;
        std::sort(vec.begin(), vec.end(), std::less<int>());
        commit_index = std::max(commit_index, vec[(vec.size() + 1) / 2 - 1]);

    } else {
        next_index[node] = (next_index[node] >= arg.prevLogIndex) ? arg.prevLogIndex : next_index[node];
    }

    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /*
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
    }    
    */

    while (true) {
        if (is_stopped()) return;

        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        std::unique_lock<std::mutex> lock(mtx);

        std::chrono::system_clock::duration follower_timeout = get_random_timeout(300);
        std::chrono::system_clock::duration candidate_timeout = get_random_timeout(800);

        std::chrono::system_clock::duration delta_time = std::chrono::system_clock::now() - last_action_time;

        if ((role == follower && delta_time > follower_timeout)
            || (role == candidate && delta_time > candidate_timeout))
        {
            // run election
            role = candidate;

            // increment current term
            current_term += 1;

            // vote for self
            vote_for = my_id;
            vote_count = 1;
            voted_nodes.assign(num_nodes(), false);
            voted_nodes[my_id] = true;

            storage->persist_metadata(current_term, vote_for);

            // initial request arguments
            request_vote_args args(current_term, my_id, log.back().index, log.back().term);

            // send vote request to others
            for (int i = 0; i < num_nodes(); ++i) {
                if (i == my_id) continue;
                thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
            }

            // update election timer
            last_action_time = std::chrono::system_clock::now();
        }

    }

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /*
        while (true) {
            if (is_stopped()) return;
            // Lab3: Your code here
        }    
        */

    while (true) {
        if (is_stopped()) return;

        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        std::unique_lock<std::mutex> lock(mtx);

        if (is_leader(current_term)) {
            for (int i = 0; i < num_nodes(); ++i) {
                if (i == my_id || next_index[i] > log.back().index) {
                    continue;
                }
                if (next_index[i] > log.front().index) {
                    append_entries_args<command> args(current_term, my_id, commit_index);
                    args.prevLogIndex = next_index[i] - 1;
                    args.prevLogTerm = get_term(args.prevLogIndex);
                    args.entries = get_entries(next_index[i], log.back().index + 1);
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                }
            }
        }
        
    }

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /*
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
    }    
    */
    
    std::vector<log_entry<command>> entries;

    while (true) {
        if (is_stopped()) return;

        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        std::unique_lock<std::mutex> lock(mtx);

        if (commit_index > last_applied) {
            entries = get_entries(last_applied + 1, commit_index + 1);
            for (log_entry<command> &entry : entries) {
                state->apply_log(entry.cmd);
            }
            last_applied = commit_index;
        }

    }

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /*
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
    }    
    */

    while (true) {
        if (is_stopped()) return;

        std::this_thread::sleep_for(std::chrono::milliseconds(150));

        std::unique_lock<std::mutex> lock(mtx);

        if (role == leader) {
            // send_heartbeat();
            for (int i = 0; i < num_nodes(); ++i) {
                if (i == my_id)
                    continue;
                append_entries_args<command> args(current_term, my_id, commit_index);
                args.prevLogIndex = next_index[i] - 1;
                args.prevLogTerm = get_term(args.prevLogIndex);
                thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
            }
        }

    }

    return;
}

/******************************************************************

                        Other functions

*******************************************************************/

template <typename state_machine, typename command>
inline int raft<state_machine, command>::get_term(int index) {
    return log[index - log.front().index].term;
}

template <typename state_machine, typename command>
inline std::vector<log_entry<command>> raft<state_machine, command>::get_entries(int begin_index, int end_index) {
    std::vector<log_entry<command>> ret;
    if (begin_index < end_index) {
        ret.assign(log.begin() + begin_index - log.front().index, log.begin() + end_index - log.front().index);
    }
    return ret;
}

template<typename state_machine, typename command>
std::chrono::system_clock::duration raft<state_machine, command>::get_random_timeout(int lower_bound) {
    static std::random_device rd;
    static std::default_random_engine randomEngine(rd());
    return std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::milliseconds(randomEngine()%200+lower_bound));
}

#endif // raft_h