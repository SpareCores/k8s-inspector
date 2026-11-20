# deploy

Kustomize base of the controller: the CRD (`crd/`), ServiceAccount, RBAC, default ConfigMap and Deployment
(`controller/`). `kubectl apply -k deploy` installs it into the namespace `inspector`.

## Platform objects

The Deployment expects objects that the cluster platform provides (listed at the top of
`controller/deployment.yaml`): ConfigMap `k8s-inspector-platform`, Secrets `k8s-inspector-git-key` and
`k8s-inspector-keeper`, and Secret `inspector-secrets` for the benchmark secrets.

## Pin the images

The base references both images by a placeholder digest (`@sha256:000...0`): it cannot pull, so an unpinned install
fails loudly instead of running `:main`. Pin real digests in the overlay of your cluster. CI pushes
`ghcr.io/sparecores/k8s-inspector` and `ghcr.io/sparecores/k8s-inspector-sc-run` (multi-arch indexes) tagged `main` and
the commit SHA; get a digest with
`docker buildx imagetools inspect ghcr.io/sparecores/k8s-inspector:<sha> --format '{{.Manifest.Digest}}'`.

```yaml
# overlay/kustomization.yaml
resources: [../k8s-inspector/deploy]
images:
- name: ghcr.io/sparecores/k8s-inspector
  digest: sha256:<controller digest>
patches:
- target: {kind: Deployment, name: k8s-inspector}
  patch: |-
    - op: replace
      path: /spec/template/spec/containers/0/env/<index of K8SI_SC_RUN_IMAGE>/value
      value: ghcr.io/sparecores/k8s-inspector-sc-run@sha256:<sc-run digest>
```

The `sc-run` image is an environment variable, not a container image, so the `images:` transformer does not reach it.

## RBAC

`controller/rbac.yaml` grants what the controller calls and nothing more. When a module gets a new Kubernetes call,
change the rules and `tests/test_deploy.py` together.
