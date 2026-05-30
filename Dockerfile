FROM atdr.meo.ws/archiveteam/grab-base:nss

WORKDIR /grab
COPY . /grab/
RUN chmod +x /grab/start.sh

# Dashboard UI port
EXPOSE 8080

# Override entrypoint to wrap the base image's CMD with the dashboard.
# The base image CMD is forwarded as "$@" to start.sh, so the pipeline
# starts exactly as before while tee'ing output to the dashboard.
ENTRYPOINT ["/grab/start.sh"]
