# psync

Agent-less file Synchronizations from multiple hosts in parallel.

Client requirements:
- SSH, SFTP
- gnu/find
- gnu/du
- md5sum
- printf
- Posix compliant shell

### Action diagram
  - Resolve files and directories
  - Get files full path and size
  - Sort files by size
  - Compute remote hash
  - Download
  - Compute local hash


### Parallelization - multiprocessing

The tool employ at least three connections per host, that are used by three parallel processes for filepath resolution and size computation, remote hashing and actual download.  Files are processed from smallest to biggest one by one. 

Parallelization tries to leverage  CPU bound and I/O bound tasks, eq. hashing and transferring.

