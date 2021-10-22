FROM ubuntu:latest
RUN apt update && apt install -y sudo apt-transport-https curl gnupg git build-essential gcc-10 g++-10&& curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor > bazel.gpg && mv bazel.gpg /etc/apt/trusted.gpg.d/ && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 100 --slave /usr/bin/g++ g++ /usr/bin/g++-10 --slave /usr/bin/gcov gcov /usr/bin/gcov-10
ADD . .
RUN ./rocksdb_install.sh
RUN ./bazelisk build embeddingstore:main

FROM ubuntu:latest
RUN apt update && apt install -y sudo
ADD rocksdb_install.sh .
RUN ./rocksdb_install.sh
COPY --from=0 ./bazel-bin/embeddingstore/main .
ENV EMBEDDINGHUB_PORT=7462
EXPOSE $EMBEDDINGHUB_PORT
ENTRYPOINT ./main