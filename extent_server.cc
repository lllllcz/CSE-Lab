// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "extent_server.h"
#include "persister.h"

extent_server::extent_server() 
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here

  // Your code here for Lab2A: recover data on startup
  _persister->restore_checkpoint();
  int entries_size = 0;
  if (!_persister->log_entries.empty()) {
    entries_size = _persister->log_entries.size();
    for (int i = 0; i < entries_size; i++) 
    {
      // redo
      chfs_command cmd = _persister->log_entries.at(i);
      extent_protocol::extentid_t id = cmd.inum & 0x7fffffff;
      const char * cbuf = nullptr;
      int size, type_record;

      switch (cmd.type)
      {
      case CMD_CREATE:
        type_record = atoi(cmd.new_value.c_str());
        im->realloc_inode(id, type_record);
        break;
      case CMD_PUT:
        cbuf = cmd.new_value.c_str();
        size = cmd.new_value.size();
        im->write_file(id, cbuf, size);
        break;
      case CMD_REMOVE:
        im->remove_file(id);
        break;
      default:
        break;
      }
    }
  }

  _persister->restore_logdata();
  if (_persister->log_entries.empty()) return;
  
  entries_size = _persister->log_entries.size();
  txid_t abort_id = 0;
  if (_persister->log_entries.back().type != CMD_COMMIT) {
    abort_id = _persister->log_entries.back().id;
    for (int i = entries_size-1; _persister->log_entries.at(i).id != abort_id; i--) {
      // undo
      chfs_command cmd = _persister->log_entries.at(i);

      extent_protocol::extentid_t id = cmd.inum & 0x7fffffff;
      const char * cbuf = nullptr;
      int size, type_record;

      switch (cmd.type)
      {
      case CMD_CREATE:
        im->remove_file(id);
        break;
      case CMD_PUT:
        cbuf = cmd.old_value.c_str();
        size = cmd.old_value.size();
        im->write_file(id, cbuf, size);
        break;
      case CMD_REMOVE:
        type_record = atoi(cmd.new_value.c_str());
        im->realloc_inode(id, type_record);
        cbuf = cmd.old_value.c_str();
        size = cmd.old_value.size();
        im->write_file(id, cbuf, size);
        break;
      default:
        break;
      }
    }
  }
  for (int i = 0; i < entries_size; i++) 
  {
    // redo
    chfs_command cmd = _persister->log_entries.at(i);

    if (abort_id == cmd.id) {break;}

    extent_protocol::extentid_t id = cmd.inum & 0x7fffffff;
    const char * cbuf = nullptr;
    int size, type_record;

    switch (cmd.type)
    {
    case CMD_CREATE:
      type_record = atoi(cmd.new_value.c_str());
      im->realloc_inode(id, type_record);
      break;
    case CMD_PUT:
      cbuf = cmd.new_value.c_str();
      size = cmd.new_value.size();
      im->write_file(id, cbuf, size);
      break;
    case CMD_REMOVE:
      im->remove_file(id);
      break;
    default:
      break;
    }
  }
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  printf("\textent_server: create inode\n");
  id = im->alloc_inode(type);

  chfs_command cmd(CMD_CREATE, 0, id, "", std::to_string(type));
  _persister->append_log(cmd);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  std::string old;
  get(id, old);

  id &= 0x7fffffff;
  
  chfs_command cmd(CMD_PUT, 0, id, old, buf);
  _persister->append_log(cmd);
  
  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);
  
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("\textent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("\textent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("\textent_server: write %lld\n", id);

  std::string old;
  get(id, old);
  
  id &= 0x7fffffff;

  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);

  chfs_command cmd(CMD_REMOVE, 0, id, old, std::to_string(attr.type));
  _persister->append_log(cmd);

  im->remove_file(id);
 
  return extent_protocol::OK;
}

int extent_server::begin_tx(txid_t tx_id)
{
  chfs_command cmd(CMD_BEGIN, tx_id, 0, "", "");
  _persister->append_log(cmd);
  return extent_protocol::OK;
}

int extent_server::commit_tx(txid_t tx_id)
{
  chfs_command cmd(CMD_COMMIT, tx_id, 0, "", "");
  _persister->append_log(cmd);
  if (_persister->log_file_size >= MAX_LOG_SZ) {
    _persister->checkpoint();
  }
  return extent_protocol::OK;
}

