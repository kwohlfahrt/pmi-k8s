FROM python:3

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt update && apt-get install -y openmpi-bin

RUN pip install --disable-pip-version-check --root-user-action=ignore \
    mpi4py numpy

WORKDIR /app

COPY ./test.py ./main.py

COPY --link --from=pmi-k8s /usr/local/bin/pmi-k8s /usr/local/bin/

ENTRYPOINT [ "/usr/local/bin/pmi-k8s" ]
