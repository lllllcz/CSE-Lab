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

#define RAFT_LOG(fmt, args...) \
    do {                       \
    } while (0);

    // #define RAFT_LOG(fmt, args...)                                                                                   \
//     do {                                                                                                         \
//         auto now =                                                                                               \
//             std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
//                 std::chrono::system_clock::now().time_since_epoch())                                             \
//                 .count();                                                                                        \
//         printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
//     } while (0);

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
    int vote_for;
    std::vector<log_entry<command>> log;
    std::vector<char> snapshot;

    /* ---- Volatile state on all server----  */
    int commitIndex;
    int lastApplied;

    /* ---- Volatile state on candidate----  */
    int vote_count;
    std::vector<bool> votedNodes;

    /* ---- Volatile state on leader----  */
    std::vector<int> nextIndex;
    std::vector<int> matchIndex;
    std::vector<int> matchCount;

    // times
    std::chrono::system_clock::time_point lastTime;
    std::chrono::system_clock::duration fTimeout;
    std::chrono::system_clock::duration cTimeout;

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
    void initTime();

    inline int getTerm(int index);
    std::vector<log_entry<command>> getEntries(int begin_index, int end_index);

    void setFollower(int term);
    void runElection();

    void sendHeartBeat();
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    stopped(false),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    storage(storage),
    state(state),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    current_term(0),
    role(follower) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization

    // recover from storage
    if (!storage->restore_all(current_term, vote_for, log, snapshot)) {
        current_term = 0;
        vote_for = -1;
        log.assign(1, log_entry<command>());
        snapshot.clear();

        storage->persist_metadata(current_term, vote_for);
        storage->persist_log(log);
        storage->persist_snapshot(snapshot);
    }
    if (!snapshot.empty()) {
        state->apply_snapshot(snapshot);
    }

    // volatile states
    commitIndex = log.front().index;
    lastApplied = log.front().index;

    // candidate volatile states
    vote_count = 0;
    votedNodes.assign(num_nodes(), false);

    // leader volatile states
    nextIndex.assign(num_nodes(), 1);
    matchIndex.assign(num_nodes(), 0);
    matchCount.clear();

    // initialize times
    lastTime = std::chrono::system_clock::now();
    initTime();
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
    if (role != leader) {
        return false;
    }

    term = current_term;
    index = log.back().index + 1;

    log_entry<command> entry(index, term, cmd);
    log.push_back(entry);
    nextIndex[my_id] = index + 1;
    matchIndex[my_id] = index;
    matchCount.push_back(1);

    if (!storage->append_log(std::vector<log_entry<command>>({entry}), log.size())) {
        storage->persist_log(log);
    }

    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here

    std::unique_lock<std::mutex> lock(mtx);

    snapshot = state->snapshot();

    if (lastApplied <= log.back().index) {
        log.erase(log.begin(), log.begin() + lastApplied - log.front().index);
    } else {
        log.clear();
    }

    storage->persist_snapshot(snapshot);
    storage->persist_log(log);

    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here

    std::unique_lock<std::mutex> lock(mtx);

    lastTime = std::chrono::system_clock::now();
    reply.term = current_term;
    reply.voteGranted = false;

    if (args.term < current_term) {
        return 0;
    }

    if (args.term > current_term) {
        setFollower(args.term);
    }

    if (vote_for == -1 || vote_for == args.candidateId) {
        if (args.lastLogTerm > log.back().term ||
            (args.lastLogTerm == log.back().term && args.lastLogIndex >= log.back().index)) {
            vote_for = args.candidateId;
            reply.voteGranted = true;

            storage->persist_metadata(current_term, vote_for);
        }
    }
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply) {
    // Lab3: Your code here

    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        setFollower(reply.term);
        return;
    }
    if (role != candidate) {
        return;
    }

    if (reply.voteGranted && !votedNodes[target]) {
        votedNodes[target] = true;
        ++vote_count;

        if (vote_count > num_nodes() / 2) {
            role = leader;

            // reinitialize leader volatile states
            nextIndex.assign(num_nodes(), log.back().index + 1);
            matchIndex.assign(num_nodes(), 0);
            matchIndex[my_id] = log.back().index;
            matchCount.assign(log.back().index - commitIndex, 0);

            // send initial ping
            sendHeartBeat();
        }
    }

    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here

    std::unique_lock<std::mutex> lock(mtx);
    lastTime = std::chrono::system_clock::now();
    reply.term = current_term;
    reply.success = false;

    if (arg.term < current_term) {
        return 0;
    }

    if (arg.term > current_term || role == candidate) {
        setFollower(arg.term);
    }
    if (arg.prevLogIndex <= log.back().index && arg.prevLogTerm == getTerm(arg.prevLogIndex)) {
        if (!arg.entries.empty()) {
            if (arg.prevLogIndex < log.back().index) {
                if (arg.prevLogIndex + 1 <= log.back().index) {
                    log.erase(log.begin() + arg.prevLogIndex + 1 - log.front().index, log.end());
                }
                log.insert(log.end(), arg.entries.begin(), arg.entries.end());
                storage->persist_log(log);
            } else {
                log.insert(log.end(), arg.entries.begin(), arg.entries.end());
                if (!storage->append_log(arg.entries, log.size())) {
                    storage->persist_log(log);
                }
            }
        }

        if (arg.leaderCommit > commitIndex) {
            commitIndex = std::min(arg.leaderCommit, log.back().index);
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
        setFollower(reply.term);
        return;
    }
    if (role != leader) {
        return;
    }

    if (reply.success) {
        int prev = matchIndex[node];
        matchIndex[node] = std::max(matchIndex[node], (int)(arg.prevLogIndex + arg.entries.size()));
        nextIndex[node] = matchIndex[node] + 1;

        int last = std::max(prev - commitIndex, 0) - 1;
        for (int i = matchIndex[node] - commitIndex - 1; i > last; --i) {
            ++matchCount[i];
            if (matchCount[i] > num_nodes() / 2 && getTerm(commitIndex + i + 1) == current_term) {
                commitIndex += i + 1;
                matchCount.erase(matchCount.begin(), matchCount.begin() + i + 1);
                break;
            }
        }
    } else {
        nextIndex[node] = std::min(nextIndex[node], arg.prevLogIndex);
    }

    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here

    std::unique_lock<std::mutex> lock(mtx);
    lastTime = std::chrono::system_clock::now();
    reply.term = current_term;

    if (args.term < current_term) {
        return 0;
    }

    if (args.term > current_term || role == candidate) {
        setFollower(args.term);
    }

    if (args.lastIncludedIndex <= log.back().index && args.lastIncludedTerm == getTerm(args.lastIncludedIndex)) {
        int end_index = args.lastIncludedIndex;

        if (end_index <= log.back().index) {
            log.erase(log.begin(), log.begin() + end_index - log.front().index);
        } else {
            log.clear();
        }
    } else {
        log.assign(1, log_entry<command>(args.lastIncludedIndex, args.lastIncludedTerm));
    }
    snapshot = args.snapshot;
    state->apply_snapshot(snapshot);

    lastApplied = args.lastIncludedIndex;
    commitIndex = std::max(commitIndex, args.lastIncludedIndex);

    storage->persist_log(log);
    storage->persist_snapshot(args.snapshot);

    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here

    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        setFollower(reply.term);
        return;
    }
    if (role != leader) {
        return;
    }

    matchIndex[node] = std::max(matchIndex[node], arg.lastIncludedIndex);
    nextIndex[node] = matchIndex[node] + 1;

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

    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    std::chrono::system_clock::time_point current_time;

    while (true) {
        if (is_stopped())
            return;

        lock.lock();
        current_time = std::chrono::system_clock::now();

        switch (role) {
            case follower:
                if (current_time - lastTime > fTimeout) {
                    runElection();
                }
                break;
            case candidate:
                if (current_time - lastTime > cTimeout) {
                    runElection();
                }
                break;
            case leader:
                // do nothing
                break;
        }

        lock.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
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
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

    while (true) {
        if (is_stopped())
            return;

        lock.lock();

        if (role == leader) {
            int last_log_index = this->log.back().index;
            for (int i = 0; i < num_nodes(); ++i) {
                if (i == my_id)
                    continue;
                if (nextIndex[i] <= last_log_index) {
                    if (nextIndex[i] > log.front().index) {
                        append_entries_args<command> args;
                        args.term = current_term;
                        args.leaderId = my_id;
                        args.leaderCommit = commitIndex;
                        args.prevLogIndex = nextIndex[i] - 1;
                        args.prevLogTerm = getTerm(args.prevLogIndex);
                        args.entries = getEntries(nextIndex[i], last_log_index + 1);
                        thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                    } else {
                        install_snapshot_args args;
                        args.term = current_term;
                        args.leaderId = my_id;
                        args.lastIncludedIndex = log.front().index;
                        args.lastIncludedTerm = log.front().term;
                        args.snapshot = snapshot;
                        thread_pool->addObjJob(this, &raft::send_install_snapshot, i, args);
                    }
                }
            }
        }

        lock.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
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

    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    std::vector<log_entry<command>> entries;

    while (true) {
        if (is_stopped())
            return;

        lock.lock();

        if (commitIndex > lastApplied) {
            entries = getEntries(lastApplied + 1, commitIndex + 1);
            for (log_entry<command> &entry : entries) {
                state->apply_log(entry.cmd);
            }
            lastApplied = commitIndex;
        }

        lock.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
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

    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

    while (true) {
        if (is_stopped())
            return;

        lock.lock();

        if (role == leader) {
            sendHeartBeat();
        }

        lock.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(150)); // Adjust param: ping period
    }

    return;
}

/******************************************************************

                        Other functions

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::initTime() {
    static std::random_device rd;
    static std::minstd_rand gen(rd());
    static std::uniform_int_distribution<int> follower_dis(300, 500);   // Adjust param
    static std::uniform_int_distribution<int> candidate_dis(800, 1000); // Adjust param
    fTimeout = std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::milliseconds(follower_dis(gen)));
    cTimeout = std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::milliseconds(candidate_dis(gen)));
}

template <typename state_machine, typename command>
inline int raft<state_machine, command>::getTerm(int index) {
    return log[index - log.front().index].term;
}

template <typename state_machine, typename command>
inline std::vector<log_entry<command>> raft<state_machine, command>::getEntries(int begin_index, int end_index) {
    std::vector<log_entry<command>> ret;
    if (begin_index < end_index) {
        ret.assign(log.begin() + begin_index - log.front().index, log.begin() + end_index - log.front().index);
    }
    return ret;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::setFollower(int term) {
    role = follower;
    current_term = term;
    vote_for = -1;

    storage->persist_metadata(current_term, vote_for);

    // re-randomize timeouts
    initTime();
}

template <typename state_machine, typename command>
void raft<state_machine, command>::runElection() {
    role = candidate;

    // increment current term
    ++current_term;

    // vote for self
    vote_for = my_id;
    vote_count = 1;
    votedNodes.assign(num_nodes(), false);
    votedNodes[my_id] = true;

    storage->persist_metadata(current_term, vote_for);

    // re-randomize timeouts
    initTime();

    // initial request arguments
    request_vote_args args{};
    args.term = current_term;
    args.candidateId = my_id;
    args.lastLogIndex = log.back().index;
    args.lastLogTerm = log.back().term;

    // send vote request to others
    for (int i = 0; i < num_nodes(); ++i) {
        if (i == my_id)
            continue;
        thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
    }

    // update election timer
    lastTime = std::chrono::system_clock::now();
}

template <typename state_machine, typename command>
void raft<state_machine, command>::sendHeartBeat() {
    static append_entries_args<command> args{};
    args.term = current_term;
    args.leaderId = my_id;
    args.leaderCommit = commitIndex;
    for (int i = 0; i < num_nodes(); ++i) {
        if (i == my_id)
            continue;
        args.prevLogIndex = nextIndex[i] - 1;
        args.prevLogTerm = getTerm(args.prevLogIndex);
        thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
    }
}

#endif // raft_h