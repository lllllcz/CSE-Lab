#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here
    void persist_metadata(int term, int vote);
    void persist_snapshot(const std::vector<char> &snapshot);
    void persist_log(const std::vector<log_entry<command>> &log_vector);

    bool append_log(const std::vector<log_entry<command>> &log, int new_size);

    bool restore_all(int &term, int &vote, std::vector<log_entry<command>> &log, std::vector<char> &snapshot);

private:
    std::mutex mtx;
    // Lab3: Your code here
    std::string metadata_dir;
    std::string log_dir;
    std::string snapshot_dir;

};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Lab3: Your code here
    metadata_dir = dir + "/metadata";
    log_dir = dir + "/log";
    snapshot_dir = dir + "/snapshot";
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
}


template <typename command>
void raft_storage<command>::persist_metadata(int term, int vote) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream metadata_file(metadata_dir, std::ios::out | std::ios::trunc | std::ios::binary);
    if (metadata_file.fail()) {
        return ;
    }

    metadata_file.write((const char *)&term, sizeof(int));
    metadata_file.write((const char *)&vote, sizeof(int));

    metadata_file.close();

}

template <typename command>
void raft_storage<command>::persist_log(const std::vector<log_entry<command>> &log_vector) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream log_file(log_dir, std::ios::out | std::ios::trunc | std::ios::binary);
    if (log_file.fail()) {
        return ;
    }

    int size = log_vector.size();
    log_file.write((const char *)&size, sizeof(int));

    for (const log_entry<command> &entry : log_vector) {
        log_file.write((const char *)&entry.index, sizeof(int));
        log_file.write((const char *)&entry.term, sizeof(int));

        size = entry.cmd.size();
        log_file.write((const char *)&size, sizeof(int));
        char *buf = new char[size];

        entry.cmd.serialize(buf, size);
        log_file.write(buf, size);

        delete[] buf;
    }

    log_file.close();

}

template <typename command>
void raft_storage<command>::persist_snapshot(const std::vector<char> &snapshot) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream snapshot_file(snapshot_dir, std::ios::out | std::ios::trunc | std::ios::binary);
    if (snapshot_file.fail()) {
        return ;
    }

    int size = snapshot.size();
    snapshot_file.write((const char *)&size, sizeof(int));
    snapshot_file.write(snapshot.data(), size);

    snapshot_file.close();

}

template <typename command>
bool raft_storage<command>::append_log(const std::vector<log_entry<command>> &log, int new_size) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream log_file(log_dir, std::ios::out | std::ios::in | std::ios::binary);
    if (log_file.fail()) {
        return false;
    }

    int size = 0;
    log_file.seekp(0, std::ios::end);
    for (const log_entry<command> &entry : log) {
        log_file.write((const char *)&entry.index, sizeof(int));
        log_file.write((const char *)&entry.term, sizeof(int));

        size = entry.cmd.size();
        log_file.write((const char *)&size, sizeof(int));

        char *buf = new char[size];

        entry.cmd.serialize(buf, size);
        log_file.write(buf, size);

        delete[] buf;
    }

    log_file.seekp(0, std::ios::beg);
    log_file.write((const char *)&new_size, sizeof(int));

    log_file.close();

    return true;
}

template <typename command>
bool raft_storage<command>::restore_all(int &term, int &vote, std::vector<log_entry<command>> &log,
                                        std::vector<char> &snapshot) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream file;
    file.open(metadata_dir, std::ios::in | std::ios::binary);
    if (file.fail() || file.eof()) { // no file or empty file
        return false;
    }

    file.read((char *)&term, sizeof(int));
    file.read((char *)&vote, sizeof(int));

    file.close();
    file.open(log_dir, std::ios::in | std::ios::binary);
    if (file.fail() || file.eof()) { // no file or empty file
        return false;
    }

    int size = 0;
    file.read((char *)&size, sizeof(int));
    log.resize(size);

    for (log_entry<command> &entry : log) {
        file.read((char *)&entry.index, sizeof(int));
        file.read((char *)&entry.term, sizeof(int));

        file.read((char *)&size, sizeof(int));

        char *buf = new char [size];

        file.read(buf, size);
        entry.cmd.deserialize(buf, size);

        delete[] buf;
    }

    file.close();
    file.open(snapshot_dir, std::ios::in | std::ios::binary);
    if (file.fail() || file.eof()) { // no file or empty file
        return false;
    }

    file.read((char *)&size, sizeof(int));
    snapshot.resize(size);

    for (char &c : snapshot) {
        file.read(&c, sizeof(char));
    }

    file.close();

    return true;
}

#endif // raft_storage_h