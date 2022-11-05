// chfs client.  implements FS operations using extent and lock server
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "chfs_client.h"
#include "extent_client.h"

/* 
 * Your code here for Lab2A:
 * Here we treat each ChFS operation(especially write operation such as 'create', 
 * 'write' and 'symlink') as a transaction, your job is to use write ahead log 
 * to achive all-or-nothing for these transactions.
 */

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    // return ! isfile(inum);

    extent_protocol::attr a;

	if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    }
    printf("isdir: %lld is not a dir\n", inum);
    return false;
}

bool
chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    }
    printf("issymlink: %lld is not a symlink\n", inum);
    return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    // 2-b
    tx_id += 1;
    ec->begin_tx(tx_id);

    std::string buf;
    EXT_RPC(ec->get(ino, buf));
    buf.resize(size);
    EXT_RPC(ec->put(ino, buf));

    ec->commit_tx(tx_id);

release:
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    // 2-a
    bool found;
    std::string buf;
    dir_content *entry = new dir_content();

    EXT_RPC(lookup(parent, name, found, ino_out));
    if (found) {
        printf("found\n");
        return IOERR;
    }

    tx_id += 1;
    ec->begin_tx(tx_id);

    EXT_RPC(ec->create(extent_protocol::T_FILE, ino_out));
    EXT_RPC(ec->get(parent, buf));

    entry->len = strlen(name);
    memcpy(entry->name, name, entry->len);
    entry->inum = ino_out;
    buf.append((char *)entry, sizeof(dir_content));
    
    EXT_RPC(ec->put(parent, buf));
    ec->commit_tx(tx_id);
    
    delete entry;

release:
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    // 2-c
    bool found;
    std::string buf;
    dir_content *entry = new dir_content();
    
    if (!isdir(parent)) {
        return IOERR;
    }
    EXT_RPC(lookup(parent, name, found, ino_out));
    if (found) {
        printf("found\n");
        return IOERR;
    }
    EXT_RPC(ec->get(parent, buf));

    tx_id += 1;
    ec->begin_tx(tx_id);
    
    EXT_RPC(ec->create(extent_protocol::T_DIR, ino_out));

    entry->len = strlen(name);
    memcpy(entry->name, name, entry->len);
    entry->inum = ino_out;
    buf.append((char *)entry, sizeof(dir_content));

    EXT_RPC(ec->put(parent, buf));
    ec->commit_tx(tx_id);

    delete entry;

release:
    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    // 2-a
    std::list<dirent> dir_entries;
    extent_protocol::attr a;

    EXT_RPC(ec->getattr(parent, a));
    if (a.type != extent_protocol::T_DIR) {
        return IOERR;
    }

    readdir(parent, dir_entries);
    while (dir_entries.size() != 0) {
        dirent dir_ent = dir_entries.front();
        if (dir_ent.name == std::string(name)) {
            found = true;
            ino_out = dir_ent.inum;
            return r;
        }
        dir_entries.pop_front();
    }
    // for (std::list<dirent>::iterator it = dir_entries.begin(); it != dir_entries.end(); ++it)
    // {
    //     if (it->name == std::string(name)) {
    //         found = true;
    //         ino_out = it->inum;
    //         return r;
    //     }
    // }

    found = false;
release:
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    // 2-a
    std::string buf;
    extent_protocol::attr a;
    const char *ptr;

    EXT_RPC(ec->getattr(dir, a));
    if (a.type != extent_protocol::T_DIR) {
        return IOERR;
    }
    EXT_RPC(ec->get(dir, buf));

    ptr = buf.c_str();
    {
        unsigned long long content_size = sizeof(struct dir_content);
        for (uint32_t i = 0; i < buf.size() / content_size; i++) {
            struct dir_content content;
            memcpy(&content, ptr + i * content_size, content_size);
            struct dirent entry;
            entry.inum = content.inum;
            entry.name.assign(content.name, content.len);
            
            list.push_back(entry);
        }
    }

release:
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    // 2-b
    std::string buf;

    EXT_RPC(ec->get(ino, buf));
    
    {
        size_t t = buf.size() - off;
        if(t > 0) {
            data = buf.substr(off, (size < t) ? size : t);
        }
        else {
            data = std::string("");
        }
    }

release:
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    // 2-b
    std::string new_data;
	new_data.assign(data, size);
    
    std::string buf, tail;
    EXT_RPC(ec->get(ino, buf));
   
	tail = off+size < buf.size() ? buf.substr(off+size, buf.size()) : "";

	buf.resize(off, '\0');
    buf.append(new_data);
	buf.append(tail);
    
    tx_id += 1;
    ec->begin_tx(tx_id);

    EXT_RPC(ec->put(ino, buf));
    ec->commit_tx(tx_id);
	bytes_written = size;

release:
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    // 2-c
    bool found;
    std::string buf;
    inum id;
    std::list<dirent> entries;

    EXT_RPC(lookup(parent, name, found, id));
    if(!found){
        printf("empty\n");
        return NOENT;
    }
    
    tx_id += 1;
    ec->begin_tx(tx_id);

    EXT_RPC(ec->remove(id));
    EXT_RPC(readdir(parent, entries));

    while (entries.size() != 0) {
        dirent dir_ent = entries.front();
        if(dir_ent.inum != id){
            dir_content ent_disk;
            ent_disk.inum = dir_ent.inum;
            ent_disk.len = dir_ent.name.size();
            memcpy(ent_disk.name, dir_ent.name.data(), ent_disk.len);
            buf.append((char *) (&ent_disk), sizeof(dir_content));
        }
        entries.pop_front();
    }

    EXT_RPC(ec->put(parent, buf));
    ec->commit_tx(tx_id);

release:
    return r;
}

int chfs_client::readlink(inum ino, std::string &buf) 
{
    int r = OK;

    EXT_RPC(ec->get(ino, buf));

release:
    return r;
}

int
chfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out)
{
    int r = OK;
    bool found;
    inum id;
    dir_content *ent = new dir_content();
    std::string buf;

    EXT_RPC(ec->get(parent, buf));
    EXT_RPC(lookup(parent, name, found, id));
    if (found) {
        return EXIST;
    }
    
    tx_id += 1;
    ec->begin_tx(tx_id);

    EXT_RPC(ec->create(extent_protocol::T_SYMLINK, ino_out));
    EXT_RPC(ec->put(ino_out, std::string(link)));

    ent->inum = ino_out;
    ent->len = strlen(name);
    memcpy(ent->name, name, ent->len);
    buf.append((char *)ent, sizeof(dir_content));

    EXT_RPC(ec->put(parent, buf));
    ec->commit_tx(tx_id);

    delete ent;

release:
    return r;
}
