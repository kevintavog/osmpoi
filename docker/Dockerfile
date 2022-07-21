FROM gcr.io/distroless/java17-debian11
# FROM gcr.io/distroless/java17-debian11-debug

WORKDIR /app
COPY ./osmpoi-all.jar /app/osmpoi.jar

EXPOSE 5000

CMD ["/app/osmpoi.jar", "-e", "http://jupiter.external:6200"]
