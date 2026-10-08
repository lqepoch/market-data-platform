# Runtime-only LocalTest image. No Rust builder, package installation, credentials, or providers.
# Pass an immutable MDP_RUNTIME_BASE reference when building. Production Drive deployments must
# additionally supply a reviewed runtime layer with a pinned rclone executable.
# Deliberately invalid sentinel so an omitted build argument cannot select a floating image tag.
ARG MDP_RUNTIME_BASE=registry.invalid/lqepoch/mdp-runtime@sha256:0000000000000000000000000000000000000000000000000000000000000000
FROM ${MDP_RUNTIME_BASE}

COPY --chown=10001:10001 mdp /usr/local/bin/mdp

USER 10001:10001
EXPOSE 8088
ENTRYPOINT ["/usr/local/bin/mdp"]
CMD ["serve", "--bind", "0.0.0.0:8088", "--local-test-root", "/data/local-test-store", "--cache-dir", "/var/cache/mdp"]
