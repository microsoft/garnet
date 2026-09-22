# garnet



![Version: 0.3.0](https://img.shields.io/badge/Version-0.3.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 2.1.8](https://img.shields.io/badge/AppVersion-2.1.8-informational?style=flat-square) 

A Helm chart for Microsoft Garnet

**Homepage:** <https://github.com/microsoft/garnet>



## Source Code

* <https://github.com/microsoft/garnet.git>

## Usage

[Helm](https://helm.sh) must be installed to use the charts. Please refer to
Helm's [documentation](https://helm.sh/docs) to get started.

To install the Garnet chart (using an OCI-based registry):

```sh
helm upgrade --install garnet oci://ghcr.io/microsoft/helm-charts/garnet
 ```

To uninstall the chart:

```sh
helm delete garnet
```



## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Affinity |
| config.existingSecret | string | `""` | Garnet secret (if you want to use an existing secret). This secret must contains a key called 'garnet.conf'. |
| config.garnetConf | string | `""` | The garnet.conf data content. |
| containers.args | list | `[]` | Containers args |
| containers.port | int | `6379` | Containers port |
| dnsConfig | object | `{}` | DNS config |
| dnsPolicy | string | `"ClusterFirst"` | DNS policy |
| extraVolumeMounts | list | `[]` | Extra Volume Mounts |
| extraVolumes | list | `[]` | Extra Volumes |
| fullnameOverride | string | `""` | Chart full name override |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| image.registry | string | `"ghcr.io"` | Image registry |
| image.repository | string | `"microsoft/garnet"` | Image repository |
| image.tag | string | `""` | Overrides the image tag whose default is the chart appVersion. |
| imagePullSecrets | list | `[]` | Image pull secrets |
| initContainers | list | `[]` | Init containers |
| livenessProbe | object | `{"failureThreshold":3,"periodSeconds":15,"tcpSocket":{"port":"garnet"},"timeoutSeconds":2}` | Containers livenessProbe (tcpSocket on the garnet port); only takes over after startupProbe succeeds. Set to null to disable (for example `--set livenessProbe=null`, or `livenessProbe: null` in a values file). |
| nameOverride | string | `""` | Chart name override |
| nodeSelector | object | `{}` | Node Selector labels |
| persistence.enabled | bool | `false` | persistence enabled |
| persistence.storageDir | string | `""` | The Storage directory for tiered records (hybrid log), if storage tiering (--storage-tier) is enabled. Default: "/data" |
| podAnnotations | object | `{}` | Pod annotations |
| podDisruptionBudget | object | `{"enabled":false,"maxUnavailable":"","minAvailable":1}` | Pod Disruption Budget; disabled by default (a PDB with minAvailable 1 on a single replica would block node drains). Rendered only when enabled is true. |
| podDisruptionBudget.enabled | bool | `false` | Create a PodDisruptionBudget |
| podDisruptionBudget.maxUnavailable | string | `""` | Maximum number/percentage of pods that may be unavailable; when set, it is rendered instead of minAvailable |
| podDisruptionBudget.minAvailable | int | `1` | Minimum number/percentage of pods that must be available (used when maxUnavailable is empty) |
| podSecurityContext | object | `{}` | Pod Security Context |
| readinessProbe | object | `{"failureThreshold":3,"periodSeconds":10,"tcpSocket":{"port":"garnet"},"timeoutSeconds":2}` | Containers readinessProbe (tcpSocket on the garnet port). Set to null to disable (for example `--set readinessProbe=null`, or `readinessProbe: null` in a values file). |
| resources | object | `{}` | Resources |
| securityContext | object | `{}` | Security Context |
| service.annotations | object | `{}` | Service annotations |
| service.ipFamilies | list | `["IPv4"]` | Service ipFamilies |
| service.ipFamilyPolicy | string | `"SingleStack"` | Service ipFamilyPolicy SingleStack|PreferDualStack|RequireDualStack |
| service.port | int | `6379` | Service port |
| service.type | string | `"ClusterIP"` | Service type |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account |
| serviceAccount.automount | bool | `false` | Automatically mount the service account token |
| serviceAccount.create | bool | `false` | Specifies whether a service account should be created |
| serviceAccount.name | string | `""` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template |
| startupProbe | object | `{"failureThreshold":60,"periodSeconds":5,"tcpSocket":{"port":"garnet"}}` | Startup probe (tcpSocket on the garnet port). Garnet only starts answering once checkpoint/AOF recovery finishes, which can take minutes for large datasets; this probe allows 5 minutes (periodSeconds 5 x failureThreshold 60) before liveness takes over. Set to null to disable (for example `--set startupProbe=null`, or `startupProbe: null` in a values file). |
| statefulSet.annotations | object | `{}` | StatefulSet annotations |
| statefulSet.replicas | int | `1` | StatefulSet replicas |
| statefulSet.revisionHistoryLimit | int | `1` | StatefulSet revisionHistoryLimit |
| statefulSet.updateStrategy.type | string | `"RollingUpdate"` | StatefulSet updateStrategy type |
| tolerations | list | `[]` | Tolerations |
| volumeClaimTemplates.requestsStorage | string | `"1Gi"` | Volume Claim Templates Requests Storage |
| volumeClaimTemplates.storageClassName | string | `"local-storage"` | Volume Claim Templates Storage Class Name |


----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)
