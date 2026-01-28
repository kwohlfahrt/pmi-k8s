#include <mpi.h>
#include <stdio.h>
#include <assert.h>

int main(int argc, char** argv) {
    assert(0 == MPI_Init(NULL, NULL));

    int world_size;
    assert(0 == MPI_Comm_size(MPI_COMM_WORLD, &world_size));

    int world_rank;
    assert(0 == MPI_Comm_rank(MPI_COMM_WORLD, &world_rank));

    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;
    assert(0 == MPI_Get_processor_name(processor_name, &name_len));

    printf("Hello world from processor %s, rank %d out of %d processors\n",
           processor_name, world_rank, world_size);

    assert(0 == MPI_Finalize());
}
