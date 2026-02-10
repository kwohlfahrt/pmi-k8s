#!/usr/bin/env python3

from mpi4py import MPI
import numpy as np
import sys

expected_size = int(sys.argv[1])

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
assert size == expected_size

sendbuf = np.full(10, rank, dtype='i')
recvbuf = np.empty([size, 10], dtype='i')
comm.Allgather(sendbuf, recvbuf)

expected = np.arange(size)[:, np.newaxis]
assert np.allclose(expected, recvbuf)
