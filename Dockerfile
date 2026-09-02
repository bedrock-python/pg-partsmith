# syntax=docker/dockerfile:1
#
# The image is the product for anyone who is not writing Python: a Go or Ruby
# team runs partition maintenance as a CronJob and never installs this library.
# So size is a feature, not an afterthought -- the wheel tree is built once and
# the build stage is thrown away, leaving the interpreter, the venv and nothing
# that compiled it.

ARG PYTHON_IMAGE=python:3.13-slim-bookworm

FROM ${PYTHON_IMAGE} AS builder

WORKDIR /src
COPY pyproject.toml README.md LICENSE CHANGELOG.md ./
COPY pg_partsmith ./pg_partsmith

# The venv is the whole payload: it is copied into the runtime stage as-is, so
# pip, its caches and the build backend never reach the published image.
# pip is uninstalled once it has done its job: the runtime image installs
# nothing, and pip's vendored copies of msgpack, urllib3 and friends are
# exactly what a scanner finds and an operator then has to explain away.
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir ".[cli]" \
    && /opt/venv/bin/pip uninstall -y pip setuptools wheel \
    && find /opt/venv -name '__pycache__' -type d -prune -exec rm -rf {} +


FROM ${PYTHON_IMAGE} AS runtime

ARG VERSION=0.0.0
LABEL org.opencontainers.image.title="pg-partsmith" \
      org.opencontainers.image.description="PostgreSQL partition lifecycle management with a plan you can read before it runs" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.source="https://github.com/bedrock-python/pg-partsmith" \
      org.opencontainers.image.documentation="https://bedrock-python.github.io/pg-partsmith/guide/cli/" \
      org.opencontainers.image.licenses="Apache-2.0"

# A fixed high UID, so a Kubernetes runAsUser can name the same one and a
# mounted document can be made readable to it without guessing.
RUN useradd --uid 65532 --user-group --create-home --shell /usr/sbin/nologin partsmith

# The base image ships pip too, and ensurepip carries a wheel of it: neither
# is run here, and both vendor the msgpack/urllib3 a scanner then reports.
# Removed by path rather than through pip itself, so the step cannot depend
# on which of the two the base image happens to provide.
RUN rm -rf /usr/local/lib/python3*/site-packages/pip /usr/local/lib/python3*/site-packages/pip-*.dist-info 
    /usr/local/lib/python3*/site-packages/setuptools* /usr/local/lib/python3*/site-packages/wheel* 
    /usr/local/lib/python3*/ensurepip /usr/local/bin/pip*

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 65532:65532
WORKDIR /home/partsmith

# The entrypoint is the command itself, so a Compose service or a CronJob names
# only what it wants done: ["plan", "-c", "/etc/partitions.yaml", "--check"].
ENTRYPOINT ["pg-partsmith"]
CMD ["--help"]
