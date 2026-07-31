# Kubernetes PMIx

This program allows MPI programs to run in Kubernetes jobs, without requiring
SSH for process launch or coordination. It does this by implementing a PMIx
server using the [OpenPMIx reference library][OpenPMIx].

> [!WARNING]
> This software is currently at the early testing stage. The core functionality
> has been implemented, but not yet tested at large scale. Some advanced
> features are missing, see the "Issues" tab for more details. Contributions
> and advice are very welcome.

## Usage

`pmi-k8s` can be used in two ways - as a direct job launcher, or as a sidecar
for a container containing an unmodified MPI program.

See the `tests/` directory for complete, tested examples.

### Direct Launch

The `pmi-k8s` binary must be included in your container image, where
`my-mpi-base` is the image containing your MPI program:

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

### Sidecar

In sidecar mode, the main job image does not need to be modified, but the job
spec is slightly more complex:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: pmi-k8s-test-sidecar
spec:
  activeDeadlineSeconds: 60
  backoffLimit: 1
  completionMode: Indexed
  completions: 2
  parallelism: 2
  template:
    spec:
      containers:
      - args:
        - |
          pids=()
          for f in /mnt/env/*.env; do
              echo ================
              cat $f
              ( set -a; source "$f"; set +a; exec ./main.py 4 ) &
              pids+=($!)
          done

          for pid in "${pids[@]}"; do
              wait "$pid" || exit $?
          done
        command:
        - bash
        - -c
        env:
        - name: JOB_NAME
          valueFrom:
            fieldRef:
              apiVersion: v1
              fieldPath: metadata.labels['batch.kubernetes.io/job-name']
        image: pmi-k8s-mpi:test
        imagePullPolicy: IfNotPresent
        name: test
        volumeMounts:
        - mountPath: /mnt/env
          name: env
        - mountPath: /mnt/temp
          name: temp
      initContainers:
      - args:
        - --nproc=2
        - --env-dir=/mnt/env
        env:
        - name: TMPDIR
          value: /mnt/temp
        image: pmi-k8s:latest
        imagePullPolicy: IfNotPresent
        name: pmi-k8s
        readinessProbe:
          exec:
            command:
            - stat
            - /mnt/env/ready
        restartPolicy: Always
        volumeMounts:
        - mountPath: /mnt/env
          name: env
        - mountPath: /mnt/temp
          name: temp
      restartPolicy: Never
      serviceAccountName: pmi-k8s-test-sidecar
      volumes:
      - emptyDir: {}
        name: env
      - emptyDir: {}
        name: temp
```

The key is that `pmi-k8s` and the main container share a temporary directory,
and the main container imports the environment written by `pmi-k8s`.

[OpenPMIx]: https://github.com/openpmix/openpmix
