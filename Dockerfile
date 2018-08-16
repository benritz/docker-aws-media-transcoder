FROM alpine:latest

RUN apk --update add exiftool \
					imagemagick \
					ffmpeg \
					optipng \
					ghostscript \
					python \
					py-pip && \
	pip install --upgrade awscli==1.14.5 && \
	apk -v --purge del py-pip && \
    rm /var/cache/apk/*
    
ENTRYPOINT cd /tmp && \
	OUTPUT_FILE=`echo "$INPUT_FILE" | cut -f 1 -d '.'`.txt && \
	LOCAL_INPUT_FILE=/tmp/`basename $INPUT_FILE` && \
	LOCAL_OUTPUT_FILE=`echo "$LOCAL_INPUT_FILE" | cut -f 1 -d '.'`.txt && \
	aws s3 cp "$INPUT_FILE" "$LOCAL_INPUT_FILE" && \
	exiftool -json -s -g1 "$LOCAL_INPUT_FILE" > "$LOCAL_OUTPUT_FILE" && \
	aws s3 cp "$LOCAL_OUTPUT_FILE" "$OUTPUT_FILE"