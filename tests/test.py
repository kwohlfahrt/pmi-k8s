#!/usr/bin/env python3

from mpi4py import MPI
import numpy as np
import sys

size = int(sys.argv[1])

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
print("size:", comm.Get_size(), "rank:", rank)

sendbuf = np.full(10, rank, dtype='i')
recvbuf = np.empty([size, 10], dtype='i')
comm.Allgather(sendbuf, recvbuf)

print(recvbuf)

for i in range(size):
    assert np.allclose(recvbuf[i,:], i)
