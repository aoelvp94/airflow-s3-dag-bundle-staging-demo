FROM astrocrpublic.azurecr.io/runtime:3.3-7

# Install the proof-of-concept archive bundle class. It must be importable as
# a normal package (the bundle classpath is resolved with import_string, so
# plugins/ is not a reliable home for it).
COPY --chown=astro:0 bundle_pkg /tmp/bundle_pkg
RUN pip install --no-cache-dir /tmp/bundle_pkg
