#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include "rpc.h"

#define MAX_LOG_SZ 131072

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires. 
 * 
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */
typedef unsigned long long txid_t;
enum cmd_type {
    CMD_BEGIN = 0,
    CMD_COMMIT,
    CMD_CREATE,
    CMD_PUT,
    CMD_REMOVE
};
struct fix_entry {
    uint64_t old_size;
    uint64_t new_size;
    int t;
    txid_t tx_id;
    extent_protocol::extentid_t inode_num;
};

class chfs_command {
public:

    cmd_type type = CMD_BEGIN;
    txid_t id = 0;
    extent_protocol::extentid_t inum = 0;
    std::string old_value = "";
    std::string new_value = "";

    // constructor
    chfs_command() {}
    chfs_command(cmd_type t, txid_t i, txid_t in, std::string o, std::string n)
        :type(t), id(i), inum(in), old_value(o), new_value(n) {}

    uint64_t size() const {
        uint64_t s = sizeof(cmd_type) + sizeof(txid_t) + sizeof(extent_protocol::extentid_t) +
                old_value.size() + new_value.size();
        return s;
    }

    std::string strToPrint() const {
        std::string res;
        switch (type) {
            case CMD_BEGIN: res = "begin";break;
            case CMD_COMMIT: res = "commit";break;
            case CMD_CREATE: res = "create";break;
            case CMD_PUT: res = "put";break;
            case CMD_REMOVE: res = "remove";break;
            default:
                break;
        }
        res = res + "\t" + std::to_string(id) + ' ' + std::to_string(inum) + '\t' +
                old_value + '\t' + new_value + '\n';
        return res;
    }
};

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to 
 * persist and recover data.
 * 
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template<typename command>
class persister {

public:
    persister(const std::string& file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(const chfs_command& log);
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint();

    // restored log data
    std::vector<command> log_entries;

    size_t log_file_size = 0;

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;

    txid_t current_tx_id = 0;

};

template<typename command>
persister<command>::persister(const std::string& dir){
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
}

template<typename command>
persister<command>::~persister() {
    // Your code here for lab2A

}

template<typename command>
void persister<command>::append_log(const chfs_command& log) {
    // Your code here for lab2A
    mtx.lock();
	std::lock_guard<std::mutex> lck(mtx, std::adopt_lock);

    if (log.type == CMD_BEGIN) current_tx_id = log.id;

    std::ofstream log_file(file_path_logfile, std::ios::out | std::ios::app | std::ios::binary);

    char * entry_buf = (char *)malloc(sizeof(fix_entry));
    fix_entry *entry = (fix_entry *)entry_buf;
    entry->old_size = log.old_value.size();
    entry->new_size = log.new_value.size();
    entry->t = log.type;
    entry->tx_id = current_tx_id;
    entry->inode_num = log.inum;

    log_file.write(entry_buf, sizeof(fix_entry));
    log_file_size += sizeof(fix_entry);
    if (entry->old_size > 0) {
        log_file.write(log.old_value.data(), entry->old_size);
        log_file_size += entry->old_size;
    }
    if (entry->new_size > 0) {
        std::string buf = log.new_value;
        log_file.write(buf.data(), buf.length());
        log_file_size += entry->new_size;
    }
    
    free(entry_buf);
    log_file.close();

}

template<typename command>
void persister<command>::checkpoint() {
    // Your code here for lab2A
    mtx.lock();
	std::lock_guard<std::mutex> lck(mtx, std::adopt_lock);

    // read all content of log
    std::ifstream read_all_log(file_path_logfile, std::ios::binary);
    std::filebuf *file_buf = read_all_log.rdbuf();

    long size = file_buf->pubseekoff(0, std::ios::end, std::ios::in);
    file_buf->pubseekpos(0, std::ios::in);
    char * buffer = new char[size];

    file_buf->sgetn(buffer, size);
    
    read_all_log.close();

    // write checkpoint file
    std::ofstream checkpoint_file(file_path_checkpoint, std::ios::app | std::ios::binary);

    checkpoint_file.write(buffer, size);

    checkpoint_file.close();
    delete[] buffer;

    // clear log file
    std::ofstream logfile(file_path_logfile, std::ios::trunc);
    logfile.close();
    log_file_size = 0;

    // save snapshot
    // std::ofstream checkpoint_file(file_path_checkpoint, std::ios::out | std::ios::binary);
    // checkpoint_file.write(buf, DISK_SIZE);
    // checkpoint_file.close();

}

template<typename command>
void persister<command>::restore_logdata() {
    // Your code here for lab2A
    mtx.lock();
	std::lock_guard<std::mutex> lck(mtx, std::adopt_lock);

    std::ifstream log_file(file_path_logfile, std::ios::in | std::ios::binary);
    log_entries.clear();
    if (!log_file.is_open()) {return;}

    while (!log_file.eof()) {
        char * entry_buf = (char *)malloc(sizeof(fix_entry));
        fix_entry *entry = (fix_entry *)entry_buf;
        log_file.read(entry_buf, sizeof(fix_entry));
        if (log_file.eof()) {free(entry_buf);break;}

        char * old_buf = nullptr;
        if (entry->old_size > 0) {
            old_buf = new char[entry->old_size+1];
            log_file.read((char *)old_buf, entry->old_size);
            old_buf[entry->old_size] = '\0';
        }
        char * new_buf = nullptr;
        if (entry->new_size > 0) {
            new_buf = new char[entry->new_size+1];
            log_file.read((char *)new_buf, entry->new_size);
            new_buf[entry->new_size] = '\0';
        }

        std::string oldval = "", newval = "";
        if (entry->old_size > 0) {
            for (uint64_t i = 0; i < entry->old_size; i++) {
                oldval.append(1, old_buf[i]);
            }
        }
        if (entry->new_size > 0) {
            for (uint64_t i = 0; i < entry->new_size; i++) {
                newval.append(1, new_buf[i]);
            }
        }
        chfs_command cmd((cmd_type) entry->t, entry->tx_id, entry->inode_num, oldval, newval);
        log_entries.push_back(cmd);

        free(entry_buf);
        if (old_buf) delete[] old_buf;
        if (new_buf) delete[] new_buf;
    }
    log_file.close();

};

template<typename command>
void persister<command>::restore_checkpoint() {
    // Your code here for lab2A
    // mtx.lock();
	// std::lock_guard<std::mutex> lck(mtx, std::adopt_lock);

    // char * buf = (char*)malloc(DISK_SIZE);

    // std::ifstream checkpoint_file(file_path_checkpoint, std::ios::in | std::ios::binary);
    // if (!checkpoint_file.is_open()) {return nullptr;}
    // checkpoint_file.read(buf, DISK_SIZE);
    // checkpoint_file.close();

    // return buf;
    mtx.lock();
	std::lock_guard<std::mutex> lck(mtx, std::adopt_lock);

    std::ifstream checkpoint_file(file_path_checkpoint, std::ios::in | std::ios::binary);
    log_entries.clear();
    if (!checkpoint_file.is_open()) {return;}

    while (!checkpoint_file.eof()) {
        char * entry_buf = (char *)malloc(sizeof(fix_entry));
        fix_entry *entry = (fix_entry *)entry_buf;
        checkpoint_file.read(entry_buf, sizeof(fix_entry));
        if (checkpoint_file.eof()) {free(entry_buf);break;}

        char * old_buf = nullptr;
        if (entry->old_size > 0) {
            old_buf = new char[entry->old_size+1];
            checkpoint_file.read((char *)old_buf, entry->old_size);
            old_buf[entry->old_size] = '\0';
        }
        char * new_buf = nullptr;
        if (entry->new_size > 0) {
            new_buf = new char[entry->new_size+1];
            checkpoint_file.read((char *)new_buf, entry->new_size);
            new_buf[entry->new_size] = '\0';
        }

        std::string oldval = "", newval = "";
        if (entry->old_size > 0) {
            for (uint64_t i = 0; i < entry->old_size; i++) {
                oldval.append(1, old_buf[i]);
            }
        }
        if (entry->new_size > 0) {
            for (uint64_t i = 0; i < entry->new_size; i++) {
                newval.append(1, new_buf[i]);
            }
        }
        chfs_command cmd((cmd_type) entry->t, entry->tx_id, entry->inode_num, oldval, newval);
        log_entries.push_back(cmd);

        free(entry_buf);
        if (old_buf) delete[] old_buf;
        if (new_buf) delete[] new_buf;
    }
    checkpoint_file.close();


};

using chfs_persister = persister<chfs_command>;

#endif // persister_h