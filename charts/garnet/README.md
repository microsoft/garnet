# garnet

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 1.0.12](https://img.shields.io/badge/AppVersion-1.0.12-informational?style=flat-square)

A Helm chart for Microsoft garnet

**Homepage:** <https://github.com/microsoft/garnet>

## Source Code

* <https://github.com/microsoft/garnet.git>

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Affinity |
| containers.args | list | `["--port","6379","-m","128m","-i","128m"]` | Containers args |
| containers.livenessProbe | object | `{}` | Containers livenessProbe |
| containers.port | int | `6379` | Containers port |
| containers.readinessProbe | object | `{}` | Containers livenessProbe |
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
| nameOverride | string | `""` | Chart name override |
| nodeSelector | object | `{}` | Node Selector labels |
| persistence.enabled | bool | `false` | persistence enabled |
| podAnnotations | object | `{}` | Pod annotations |
| podSecurityContext | object | `{}` | Pod Security Context |
| resources | object | `{}` | Resources |
| securityContext | object | `{}` | Security Context |
| service.annotations | object | `{}` | Service annotations |
| service.port | int | `6379` | Service port |
| service.type | string | `"ClusterIP"` | Service type |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account |
| serviceAccount.create | bool | `false` | Specifies whether a service account should be created |
| serviceAccount.name | string | `""` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template |
| serviceAccount.token | bool | `false` | Creates the token object |
| statefulSet.annotations | object | `{}` | StatefulSet annotations |
| statefulSet.replicas | int | `1` | StatefulSet replicas |
| statefulSet.revisionHistoryLimit | int | `1` | StatefulSet revisionHistoryLimit |
| statefulSet.updateStrategy.type | string | `"RollingUpdate"` | StatefulSet updateStrategy type |
| tolerations | list | `[]` | Tolerations |
| volumeClaimTemplates.requestsStorage | string | `"1Gi"` | Volume Claim Templates Requests Storage |
| volumeClaimTemplates.storageClassName | string | `"local-storage"` | Volume Claim Templates Storage Class Name |

