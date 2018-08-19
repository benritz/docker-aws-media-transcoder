FROM alpine:latest

COPY media-transcoder /opt/media-transcoder

RUN apk --update add exiftool \
					imagemagick \
					ffmpeg \
					optipng \
					ghostscript \
					nodejs \ 
					npm && \
    rm /var/cache/apk/* && \
    cd /opt/media-transcoder && \
	npm install

ENTRYPOINT cd /opt/media-transcoder && \
			npm start