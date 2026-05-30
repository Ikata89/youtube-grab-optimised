FROM atdr.meo.ws/archiveteam/grab-base:nss

# ONBUILD in the base image already copies project files to /grab and runs
# warrior-install.sh. We just need to mark our entrypoint wrapper executable
# and expose the dashboard port.
RUN chmod +x /grab/start.sh

EXPOSE 8080

ENTRYPOINT ["/grab/start.sh"]
