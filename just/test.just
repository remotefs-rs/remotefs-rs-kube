# Run all unit, integration, binary, and documentation tests.
[group('test')]
test args="":
  cargo test --lib {{ args }}
  cargo test --doc {{ args }}

# Run the cluster-backed integration suite against Minikube (needs MINIKUBE_IP).
[group('test')]
integration args="":
  cargo test --lib --features integration-tests,tokio {{ args }} -- --test-threads=1

# Start the Minikube cluster the `integration-tests` feature connects to.
[group('test')]
minikube_up:
  minikube start

# Stop the Minikube cluster.
[group('test')]
minikube_down:
  minikube stop

# Generate an LCOV coverage report.
[group('test')]
coverage output="lcov.info" args="":
  cargo llvm-cov --lib --lcov --output-path {{ output }} {{ args }}
