FROM ubuntu:bionic

# Install build tools
RUN apt update
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt install -y  \
    build-essential \
    git \
    ninja-build \
    libssl-dev \
    libjemalloc-dev \
    default-jdk  \
    software-properties-common  \
    lsb-release  \
    libtool  \
    autoconf  \
    unzip  \
    wget  \
    gnupg  \
    ca-certificates  \
    gcc-8  \
    g++-8

# Use GCC 8
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 700 --slave /usr/bin/g++ g++ /usr/bin/g++-7
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 800 --slave /usr/bin/g++ g++ /usr/bin/g++-8

# Use latest cmake
RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null
RUN apt-add-repository "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main"
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 6AF7F09730B3F0A4

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF
RUN echo "deb https://download.mono-project.com/repo/ubuntu stable-bionic main" | tee /etc/apt/sources.list.d/mono-official-stable.list

# Install cmake and mono
RUN apt update && apt install -y cmake mono-devel liblz4-dev
RUN rm -rf /var/lib/apt/lists/*

RUN echo "export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")" >> ~/.bashrc
RUN echo "export CC=/usr/bin/gcc-8; export CXX=/usr/bin/g++-8" >> ~/.bashrc