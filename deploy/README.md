# deploy

Kustomize base of the controller: the CRD (`crd/`), ServiceAccount, RBAC, Deployment, the
PriorityClass `benchmark` and the ResourceQuota `pods: 60` (`controller/`). `kubectl apply -k deploy` installs it into
the namespace `inspector`.

## Platform objects

The Deployment expects objects that the cluster platform provides (listed at the top of
`controller/deployment.yaml`):

- ConfigMap `k8s-inspector-platform`. Required keys: `K8SI_BUCKET`, `K8SI_BUCKET_REGION`, `K8SI_ROLE_ARN`. Optional
  keys (the controller starts without them): `K8SI_GIT_URL`, `K8SI_GIT_BRANCH`, `KEEPER_API_BASE`, `K8SI_KEY_PREFIX`.
- Secrets `k8s-inspector-git-key` (`ssh-privatekey`) and `k8s-inspector-keeper` (`api-key`): both optional, the
  Pod starts without them.
- Secret `inspector-secrets` for the benchmark secrets.
- ConfigMap `k8s-inspector` (key `config.yaml`, the settings, see
  [configuration](../docs/reference/configuration.md)): created and managed by the platform, not shipped here. The
  controller reads it through the API every ~30 s, so a change applies without a restart; the Deployment does not
  mount or hash it. Without it the defaults apply and no vendor is enabled.

## DBaaS

Managed-database benchmarks need, besides the controller, what the cluster platform provides (see
[add a DBaaS vendor](../docs/how-to/add-dbaas-vendor.md#what-the-platform-provides)); nothing here is needed while
`dbaas.vendors` is empty in the settings:

- the image `ghcr.io/sparecores/k8s-inspector-dbaas`, pinned in `K8SI_DBAAS_IMAGE` (the Deployment has a placeholder);
- the ServiceAccount `k8s-inspector-dbaas` of the provisioner Jobs (`controller/dbaas-serviceaccount.yaml`, shipped here,
  no Role: the Jobs do not call the Kubernetes API);
- the controller's Role also allows `jobs` (create, get, list, watch, delete) for them;
- platform: Secret `k8s-inspector-dbaas-state` (key `passphrase`), Secrets `k8s-inspector-dbaas-<vendor>` with the
  vendors' cloud credentials, `dbaas.vendors.<vendor>.networks.<region>` in the settings ConfigMap, and the bucket role's
  access to `dbaas-state/` (read, write, list, delete). The Jobs use the controller's role: their ServiceAccount token is
  projected with the audience `sts.amazonaws.com`, so the role's trust policy must accept the ServiceAccount
  `k8s-inspector-dbaas` (and any vendor ServiceAccount) as well as `k8s-inspector`.

## Pin the images

The base references the images by a placeholder digest (`@sha256:000...0`): it cannot pull, so an unpinned install
fails loudly instead of running `:main`. Pin real digests in the overlay of your cluster. CI pushes
`ghcr.io/sparecores/k8s-inspector`, `ghcr.io/sparecores/k8s-inspector-sc-run` and
`ghcr.io/sparecores/k8s-inspector-dbaas` (multi-arch indexes) tagged `main` and
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

The `sc-run` and DBaaS images are environment variables, not container images, so the `images:` transformer does not reach them (patch `K8SI_DBAAS_IMAGE` the same way).

## Priority and quota

Benchmark Pods reference the PriorityClass `benchmark` (`value: -10`, `preemptionPolicy: Never`, not the global default):
they rank below every default Pod and never evict anything. The ResourceQuota `benchmark-pods` caps the namespace at 60
Pods. The controller's ClusterRole needs no PriorityClass rights, because Pods only name the class.

## RBAC

`controller/rbac.yaml` grants what the controller calls and nothing more. When a module gets a new Kubernetes call,
change the rules and `tests/test_deploy.py` together.
