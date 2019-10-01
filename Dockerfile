# Build the Linux binary and place it into an Alpine based container.

# ------------------------------------------------------------------------------
# Stage 1: build Linux binary
#
# CentOS 7.2 was released in Nov 2015 and its glibc should thus be compatible
# with all reasonable recent Linux distributions.
# ------------------------------------------------------------------------------
FROM centos:7.2.1511 as builder

# Build Python 3.7.2 from source.
ARG PYTHONVERSION=3.7.2

# Dependencies.
RUN yum update -y

# Fixes a problem when building on Dockerhub. See this post for more info:
# https://github.com/CentOS/sig-cloud-instance-images/issues/15
RUN yum install -y yum-plugin-ovl

# Install the core dependencies to build Python and run PyInstaller.
RUN yum install -y \
  bzip2-devel \
  db4-devel \
  expat-devel \
  gdbm-devel \
  libffi-devel \
  libpcap-devel \
  ncurses-devel \
  openssl-devel \
  readline-devel \
  sqlite-devel \
  tk-devel \
  wget \
  which \
  xz-devel \
  zlib-devel
RUN yum groupinstall -y "Development Tools"

# Create working directory.
RUN mkdir /src
WORKDIR /src

# Download and build Python.
RUN wget https://www.python.org/ftp/python/$PYTHONVERSION/Python-$PYTHONVERSION.tgz
RUN tar -xzvf Python-$PYTHONVERSION.tgz \
  && cd Python-$PYTHONVERSION \
  && ./configure --enable-shared \
  && make -j2 \
  && make install

# Add shared Python libraries to linker path.
ENV LD_LIBRARY_PATH=/usr/local/lib

# Install utility packages to build binary programs from Python scripts.
RUN pip3 install pipenv pyinstaller awscli

# Clone the repo and build the binary.
RUN git clone https://github.com/olitheolix/square.git --depth=1
WORKDIR /src/square
RUN pipenv install --system --deploy --dev
RUN pyinstaller square/square.py --onefile


# ------------------------------------------------------------------------------
# Stage 2: copy Linux binary into an Alpine based image.
# ------------------------------------------------------------------------------
FROM frolvlad/alpine-glibc
COPY --from=builder /src/square/dist/square /usr/local/bin
RUN chmod a+x /usr/local/bin/square
