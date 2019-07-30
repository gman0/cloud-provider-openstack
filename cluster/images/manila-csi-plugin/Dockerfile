FROM alpine:3.7
LABEL maintainers="Kubernetes Authors"
LABEL description="Manila CSI Plugin"

RUN apk add --no-cache rsync

ADD manila-csi-plugin /bin/

ENTRYPOINT ["/bin/manila-csi-plugin"]
