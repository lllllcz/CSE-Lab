//
// Created by 10092 on 2022/10/2.
//

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  // Part_1_A

  if (id < 0 || id >= BLOCK_NUM) {
    printf("\t\tdisk: error! wrong block id\n");
    return;
  }
  if (buf == NULL) {
    printf("\t\tdisk: error! empty pointer \"buf\"\n");
    return;
  }

  memcpy(buf, this->blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  // Part_1_A

  if (id < 0 || id >= BLOCK_NUM) {
    printf("\t\tdisk: error! wrong block id\n");
    return;
  }
  if (buf == NULL) {
    printf("\t\tdisk: error! empty pointer \"buf\"\n");
    return;
  }

  memcpy(this->blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  // Part_1_B

  int i = IBLOCK(INODE_NUM, sb.nblocks) + 1;
  for(; i < BLOCK_NUM; i++) {
    if(!using_blocks[i]){
      using_blocks[i] = 1;
      return i;
    }
  }
  // char *buf = (char *)malloc(BLOCK_SIZE);
  // int bitmap_block_number = (BLOCK_NUM / BPB);

  // // scan bitmap
  // for (int i = 0; i < bitmap_block_number; i++) {
  //   d->read_block(i+2, buf);
  //   for (int j = 0; j < BLOCK_SIZE; j++) {
  //     if (buf[j] != -1) {
  //       int b = buf[j];
  //       int offset = 7;
  //       for (; (b >> offset)&1; offset--) {}
  //       b |= (1 << offset);
  //       buf[j] = b;
  //       d->write_block(i+2, buf);
  //       int block_id = i * BPB + j * 8 + (7 - offset);
  //       free(buf);
  //       return block_id;
  //     }
  //   }
  // }

  printf("\tbm: error! alloc block error\n");
  // free(buf);
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /*
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  // Part_1_B

  using_blocks[id] = 0;

  // char *buf = (char *)malloc(BLOCK_SIZE);
  // const int bitmap_block = id / BPB;
  // d->read_block(bitmap_block+2, buf);

  // const int block_offset = id % BPB;
  // int byte_value = buf[block_offset/8];
  // const int byte_offset = block_offset % 8;

  // byte_value &= ~(1 << (7 - byte_offset));

  // buf[block_offset/8] = byte_value;
  // d->write_block(bitmap_block+2, buf);
  // free(buf);

  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  // int bits = IBLOCK(INODE_NUM, BLOCK_NUM) + 1;
  // for(int i = 0; i < bits / BPB; ++i) {
  //   char *buf = (char *)malloc(BLOCK_SIZE);
  //   memset(buf, -1, BLOCK_SIZE);
  //   d->write_block(2 + i, buf);
  //   free(buf);
  // }
  // int rest = bits % BPB;
  // if(bits % BPB > 0) {
  //   char *buf = (char *)malloc(BLOCK_SIZE);
  //   memset(buf, -1, rest / 8);
  //   char tmp = 0;
  //   for(int j = 0; j < rest % 8; ++j) {
  //     tmp |= 1 << (7 - j);
  //   }
  //   buf[rest / 8] = tmp;
  //   d->write_block(2 + bits / BPB, buf);
  //   free(buf);
  // }

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\t\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /*
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  // Part_1_A
  int inode_block = -1;
  char *buf = (char *)malloc(BLOCK_SIZE);
  inode_t * new_inode =(inode_t *) buf;

  if (type == extent_protocol::T_DIR) {
    inode_block = IBLOCK(1, BLOCK_NUM);
    bzero(new_inode, sizeof(inode_t));
    new_inode->type = type;
    new_inode->atime = time(NULL);
    new_inode->mtime = time(NULL);
    new_inode->ctime = time(NULL);
    bm->write_block(inode_block, buf);
    free(buf);
    return 1;
  }

  int i = 2;
  for (; i < INODE_NUM; i++) {
    inode_block = IBLOCK(i, BLOCK_NUM);
    bm->read_block(inode_block, buf);
    if (new_inode->type == 0) {
      bzero(new_inode, sizeof(inode_t));
      new_inode->type = type;
      new_inode->atime = time(NULL);
      new_inode->mtime = time(NULL);
      new_inode->ctime = time(NULL);
      bm->write_block(inode_block, buf);
      free(buf);
      return i;
    }
  }
  printf("\tim: error! alloc inode error\n");
  free(buf);
  return 0;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /*
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  // Part_1_C
  inode_t *ino = get_inode(inum);
  if (ino) {
    ino->type = 0;
    put_inode(inum, ino);
    free(ino);
  }

  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode*
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino;
  /*
   * your code goes here.
   */
  // Part_1_A
  int inode_block = -1;
  char *buf = (char *)malloc(BLOCK_SIZE);

  inode_block = IBLOCK(inum, BLOCK_NUM);
  bm->read_block(inode_block, buf);
  ino = (inode_t *) buf;
  if (ino->type == 0) {
    printf("\tim: error! no such inode\n");
    free(buf);
    return NULL;
  }

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  // printf("\t\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  // Part_1_B

  inode_t * file_inode = get_inode(inum);
  if (file_inode == NULL) {return;}
  *size = file_inode->size;
  long unsigned int block_count = (*size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  char *buf = (char *)malloc(block_count * BLOCK_SIZE);

  if (block_count <= NDIRECT) {
    for (long unsigned int i = 0; i < block_count; i++) {
      bm->read_block(file_inode->blocks[i], buf+i*BLOCK_SIZE);
    }
  } else if (block_count <= MAXFILE) {
    for (long unsigned int i = 0; i < NDIRECT; i++) {
      bm->read_block(file_inode->blocks[i], buf+i*BLOCK_SIZE);
    }
    blockid_t *indirect_block = (blockid_t *)malloc(BLOCK_SIZE);
    bm->read_block(file_inode->blocks[NDIRECT], (char *)indirect_block);
    for(long unsigned int i = 0; i < block_count - NDIRECT; ++i) {
      bm->read_block(indirect_block[i], buf + (NDIRECT + i) * BLOCK_SIZE);
    }
    free(indirect_block);
  } else {
    printf("\tim: error! file is too large to read\n");
  }

  *buf_out = buf;
  free(file_inode);
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf
   * is larger or smaller than the size of original inode
   */
  // Part_1_B

  inode_t * file_inode = get_inode(inum);
  if (file_inode == NULL) {return;}
  long unsigned int old_block_count = (file_inode->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  long unsigned int new_block_count = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  if (new_block_count > MAXFILE) {return;}

  if (old_block_count < new_block_count) {
    if (old_block_count > NDIRECT) {
      blockid_t *indirect_block = (blockid_t *)malloc(BLOCK_SIZE);
      bm->read_block(file_inode->blocks[NDIRECT], (char *)indirect_block);
      for (long unsigned int j = old_block_count - NDIRECT; j+NDIRECT < new_block_count; j++) {
        blockid_t id = bm->alloc_block();
        indirect_block[j] = id;
      }
      bm->write_block(file_inode->blocks[NDIRECT], (char *)indirect_block);
      free(indirect_block);
    }
    else {
      long unsigned int i = old_block_count;
      for (; i < NDIRECT && i < new_block_count; i++) {
        blockid_t id = bm->alloc_block();
        file_inode->blocks[i] = id;
      }

      if (i == NDIRECT && i < new_block_count) {
        // indirect
        blockid_t *indirect_block = (blockid_t *) malloc(BLOCK_SIZE);
        blockid_t indirect_block_id = bm->alloc_block();
        file_inode->blocks[NDIRECT] = indirect_block_id;
        for (long unsigned int j = 0; j + NDIRECT < new_block_count; j++) {
          blockid_t id = bm->alloc_block();
          indirect_block[j] = id;
        }
        bm->write_block(indirect_block_id, (char *) indirect_block);
        free(indirect_block);
      }
    }
  }
  else if (old_block_count > new_block_count) {
    if (new_block_count > NDIRECT) {
      blockid_t *indirect_block = (blockid_t *)malloc(BLOCK_SIZE);
      bm->read_block(file_inode->blocks[NDIRECT], (char *)indirect_block);
      for (long unsigned int j = new_block_count - NDIRECT; j+NDIRECT < old_block_count; j++) {
        bm->free_block(indirect_block[j]);
        indirect_block[j] = 0;
      }
      bm->write_block(file_inode->blocks[NDIRECT], (char *)indirect_block);
      free(indirect_block);
    }
    else {
      if (old_block_count > NDIRECT) {
        blockid_t *indirect_block = (blockid_t *)malloc(BLOCK_SIZE);
        bm->read_block(file_inode->blocks[NDIRECT], (char *)indirect_block);
        for (long unsigned int j = 0; j+NDIRECT < old_block_count; j++) {
          bm->free_block(indirect_block[j]);
        }
        free(indirect_block);
        bm->free_block(file_inode->blocks[NDIRECT]);
        file_inode->blocks[NDIRECT] = 0;
      }
      for (long unsigned int i = new_block_count; i < old_block_count && i < NDIRECT; i++) {
        bm->free_block(file_inode->blocks[i]);
        file_inode->blocks[i] = 0;
      }
    }
  }

  file_inode->size = size;

  if (new_block_count <= NDIRECT) {
    for (long unsigned int i = 0; i < new_block_count; i++) {
      bm->write_block(file_inode->blocks[i], buf+i*BLOCK_SIZE);
    }
  } else if (new_block_count <= MAXFILE) {
    for (long unsigned int i = 0; i < NDIRECT; i++) {
      bm->write_block(file_inode->blocks[i], buf+i*BLOCK_SIZE);
    }
    blockid_t *indirect_block = (blockid_t *)malloc(BLOCK_SIZE);
    bm->read_block(file_inode->blocks[NDIRECT], (char *)indirect_block);
    for(long unsigned int i = 0; i < new_block_count - NDIRECT; ++i) {
      bm->write_block(indirect_block[i], buf + (NDIRECT + i) * BLOCK_SIZE);
    }
    free(indirect_block);
  } else {
    printf("\tim: error! file is too large to write\n");
  }

  file_inode->atime = time(NULL);
  file_inode->mtime = time(NULL);
  file_inode->ctime = time(NULL);
  put_inode(inum, file_inode);
  free(file_inode);
  return;
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  // Part_1_A
  struct inode *attr = get_inode(inum);
  if(attr != NULL) {
    a.type = attr->type;
    a.atime = attr->atime;
    a.mtime = attr->mtime;
    a.ctime = attr->ctime;
    a.size = attr->size;
    free(attr);
  }
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider both the data block and inode of the file
   */
  // Part_1_C

  inode_t * file_inode = get_inode(inum);
  if (file_inode == NULL) {return;}
  long unsigned int block_count = (file_inode->size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  if (block_count > MAXFILE) {return;}

  long unsigned int i = 0;
  for (; i < block_count && i < NDIRECT; i++) {
    bm->free_block(file_inode->blocks[i]);
  }

  if (i == NDIRECT && i < block_count) {
    blockid_t *indirect_block = (blockid_t *)malloc(BLOCK_SIZE);
    bm->read_block(file_inode->blocks[NDIRECT], (char *)indirect_block);
    for (long unsigned int j = 0; j+NDIRECT < block_count; j++) {
      bm->free_block(indirect_block[j]);
    }
    free(indirect_block);
    bm->free_block(file_inode->blocks[NDIRECT]);
    file_inode->blocks[NDIRECT] = 0;
  }

  bzero(file_inode, sizeof(inode_t));
  put_inode(inum, file_inode);
  free(file_inode);
  return;
}
