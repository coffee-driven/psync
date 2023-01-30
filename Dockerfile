FROM docker.io/bitnami/minideb:latest

RUN apt update \
    && apt install -y openssh-server \
    && useradd -ms /bin/bash bob \
    && mkdir /home/bob/.ssh

COPY public /home/bob/.ssh/authorized_keys
COPY sshd_config /etc/ssh/sshd_config
COPY starter.sh /starter.sh

RUN chown -R bob:bob /home/bob/.ssh \
    && chmod +x /starter.sh

CMD ["/starter.sh"]
