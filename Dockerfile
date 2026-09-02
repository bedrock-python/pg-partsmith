# syntax=docker/dockerfile:1
#
# The image is the product for anyone who is not writing Python: a Go or Ruby
# team runs partition maintenance as a CronJob and never installs this library.
# So it is built the way such a team would judge it -- small, with nothing in
# it that is not needed to run one command and exit.
#
# Two stages. The builder is a full Python image with uv, and everything that
# installs, compiles or strips happens there. The runtime is distroless: glibc,
# OpenSSL, CA certificates, a timezone database, and nothing else -- no shell,
# no package manager, no pip. The interpreter, its standard library and the
# virtualenv are copied over, plus exactly the shared libraries the extension
# modules we keep link against.

ARG PYTHON_IMAGE=python:3.14-slim-bookworm
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.12.9
ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian12:nonroot

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS builder

# binutils for strip; nothing else is installed into the image being built.
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends binutils >/dev/null \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /bin/uv

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /src
COPY pyproject.toml uv.lock README.md LICENSE CHANGELOG.md ./
COPY pg_partsmith ./pg_partsmith

# Installed from the lock file and nothing else: the image contains the
# versions the test suite ran against, not whatever the index served today.
# The project itself goes in as a real package, not an editable link to /src,
# and is always rebuilt: the cache keys built wheels by version, and the
# version does not change between two commits of the same release.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --extra cli --python /usr/local/bin/python3 \
        --reinstall-package pg-partsmith

# What the runtime image will not have: rich and its dependencies exist for
# typer's coloured --help, which a CronJob log never sees (typer falls back to
# plain help without them). Debug symbols in the compiled extensions
# are most of their size. Then the parts of the standard library nobody runs
# in a container -- the IDE, the GUI toolkit, the test suite, the pager data,
# the base image's own pip, which would otherwise ride along with the standard
# library -- and the extension modules whose libraries the runtime leaves out.
RUN cd /opt/venv/lib/python3.*/site-packages \
    && rm -rf rich rich-*.dist-info pygments pygments-*.dist-info markdown_it markdown_it_py-*.dist-info \
              mdurl mdurl-*.dist-info sqlalchemy/testing \
    && find /opt/venv -name '*.so' -exec strip --strip-unneeded {} + \
    && find /opt/venv \( -name '*.pyx' -o -name '*.pxd' -o -name '*.c' -o -name '*.h' -o -name '*.pyi' \) -delete \
    && cd /usr/local/lib/python3.* \
    && rm -rf idlelib tkinter turtledemo turtle.py test pydoc_data lib2to3 ensurepip config-3.* \
              site-packages/pip site-packages/pip-*.dist-info site-packages/setuptools* site-packages/wheel* \
              lib-dynload/_tkinter* lib-dynload/_curses* lib-dynload/readline* lib-dynload/_dbm* \
              lib-dynload/_gdbm* lib-dynload/_sqlite3* lib-dynload/_test* lib-dynload/xxlimited* \
              lib-dynload/_ctypes_test* lib-dynload/_xxtestfuzz* \
    && strip --strip-unneeded /usr/local/lib/libpython3.*.so.1.0 lib-dynload/*.so \
    && python3 -m compileall -q -j0 --invalidation-mode unchecked-hash /usr/local/lib/python3.*

# The shared libraries the kept extension modules need beyond what distroless
# provides (glibc, libgcc, libstdc++, OpenSSL). Staged under the same paths
# they will occupy, so one COPY places them. The multiarch directory comes
# from the interpreter, so the arm64 build stages arm64 libraries.
RUN set -eu \
    && arch="$(python3 -c 'import sysconfig; print(sysconfig.get_config_var("MULTIARCH"))')" \
    && mkdir -p "/staging/usr/lib/${arch}" \
    && for lib in libz.so.1 libbz2.so.1.0 liblzma.so.5 libffi.so.8 libuuid.so.1; do \
         cp "/usr/lib/${arch}/${lib}" "/staging/usr/lib/${arch}/"; \
       done


FROM ${RUNTIME_IMAGE} AS runtime

ARG VERSION=0.0.0
LABEL org.opencontainers.image.title="pg-partsmith" \
      org.opencontainers.image.description="PostgreSQL partition lifecycle management with a plan you can read before it runs" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.source="https://github.com/bedrock-python/pg-partsmith" \
      org.opencontainers.image.documentation="https://bedrock-python.github.io/pg-partsmith/guide/cli/" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.base.name="gcr.io/distroless/cc-debian12:nonroot"

COPY --from=builder /staging/ /
COPY --from=builder /usr/local/bin/python3* /usr/local/bin/
COPY --from=builder /usr/local/lib/libpython3*.so.1.0 /usr/local/lib/
COPY --from=builder /usr/local/lib/python3.14 /usr/local/lib/python3.14
COPY --from=builder /opt/venv /opt/venv

# TYPER_USE_RICH=0: typer assumes rich is installed unless told otherwise, and
# rich is one of the things this image leaves out. Plain --help is what a
# CronJob log shows anyway.
ENV PATH="/opt/venv/bin:/usr/local/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHOME=/usr/local \
    TYPER_USE_RICH=0

# distroless' nonroot is 65532 -- the same fixed UID this image has always
# run as, so a runAsUser that named it keeps naming it.
USER 65532:65532
WORKDIR /home/nonroot

# The entrypoint is the command itself, so a Compose service or a CronJob names
# only what it wants done: ["plan", "-c", "/etc/partitions.yaml", "--check"].
# There is no shell to fall back to; --write and --ok-if-locked cover the two
# things a wrapper used to do.
ENTRYPOINT ["/opt/venv/bin/python", "-m", "pg_partsmith.cli"]
CMD ["--help"]
