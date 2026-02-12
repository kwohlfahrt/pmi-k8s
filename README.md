# Kubernetes PMIx

This program allows MPI programs to run in Kubernetes jobs, without requiring
SSH for process launch or coordination. It does this by implementing a PMIx
server using the [OpenPMIx reference library][OpenPMIx].

> [!CAUTION]
> This software is currently at the proof-of-concept stage, and has several
> known bugs and incomplete features. See the "Issues" tab for more details,
> contributions and advice are very welcome.

## Usage

The `pmi-k8s` binary must be included in your container image:

```Dockerfile
FROM my-mpi-base AS my-mpi-image

COPY --link --from=ghcr.io/kwohlfahrt/pmi-k8s:latest /usr/local/bin/pmi-k8s /usr/local/bin/

ENTRYPOINT [ "/usr/local/bin/pmi-k8s" ]
```

Then, define your `Job`:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: my-mpi-job
spec:
  completions: 2
  parallelism: 2
  completionMode: Indexed
  template:
    spec:
      serviceAccountName: my-mpi-sa  # Must have permission to read jobs & pods
      containers:
        - name: test
          image: my-mpi-image
          args:
            - --nproc=2
            - --
            # Remaining args are the command to be executed `--nproc` times in this pod
          env:
            - name: JOB_NAME
              valueFrom:
                fieldRef:
                  apiVersion: v1
                  fieldPath: metadata.labels['batch.kubernetes.io/job-name']
      restartPolicy: Never
```

See the `tests/` directory for a complete, tested example.

[OpenPMIx]: https://github.com/openpmix/openpmix
