// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  pthread_mutex_lock(&mtx);

  if (lock_recorder.find(lid) == lock_recorder.end()) {
    // nobody have used
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    lock_manager[lid] = cond;
  } 
  else {
    if (lock_recorder[lid]) {
      // locked, wait
      while (lock_recorder[lid]) {
        pthread_cond_wait(&lock_manager[lid], &mtx);
      }
    }
  }

  // now, this client hold the lock
  lock_recorder[lid] = true;

  pthread_mutex_unlock(&mtx);
  
  r = ret;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  pthread_mutex_lock(&mtx);

  // release lock
  lock_recorder[lid] = false;
  pthread_cond_signal(&lock_manager[lid]);

  pthread_mutex_unlock(&mtx);

  r = ret;
  return ret;
}