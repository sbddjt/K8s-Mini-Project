pipeline {
  agent any

  parameters {
    booleanParam(
      name: 'RESET_MINIPROJECT',
      defaultValue: false,
      description: '재배포 전 miniproject 네임스페이스와 Helm Release를 삭제해 깨끗하게 설치합니다.'
    )
  }

  options {
    timestamps()
    disableConcurrentBuilds()
    timeout(time: 45, unit: 'MINUTES')
  }

  environment {
    HARBOR_REGISTRY = "10.0.2.111:80"
    HARBOR_PROJECT  = "k8s-mini"
    IMAGE_TAG       = "${env.BUILD_NUMBER}"
    HARBOR_CREDS    = "harbor-credentials"
    NAMESPACE       = "miniproject"
    NFS_NAMESPACE   = "kube-system"
    NFS_RELEASE     = "nfs-provisioner"
    NFS_CHART_PATH  = "k8s-manifests/nfs-storage"
    PROJECT_DIR     = "."
    NFS_PVC_LIST    = "mongo-data-mongo-0 redis-data-redis-0 kafka-data-kafka-0"
    NFS_PV_LIST     = "mongo-pv-nfs-0 redis-pv-nfs-0 kafka-pv-nfs-0"
    HELM_TIMEOUT    = "10m"
    HELM_INFRA_RELEASE = "mobility-infra"
    HELM_INFRA_CHART_PATH = "k8s-manifests/mobility-infra"
    HELM_COMMAND_RELEASE = "mobility-command"
    HELM_COMMAND_CHART_PATH = "k8s-manifests/mobility-command"
    HELM_QUERY_RELEASE = "mobility-query"
    HELM_QUERY_CHART_PATH = "k8s-manifests/mobility-query"
    HELM_FRONTEND_RELEASE = "mobility-frontend"
    HELM_FRONTEND_CHART_PATH = "k8s-manifests/mobility-frontend"
    MONITORING_NAMESPACE = "monitoring"
    MONITORING_DIR = "k8s-manifests/monitoring"
    NFS_STORAGE_MANIFEST = "k8s-manifests/nfs-storage.yaml"
    HARBOR_EMAIL = "ci@k8s-mini.local"
    LEGACY_NFS_PROVISIONER = "cluster.local/nfs-provisioner-nfs-subdir-external-provisioner"
    SONAR_HOST_URL  = "http://10.0.2.110:9000"
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Set project path') {
      steps {
        script {
          if (fileExists('K8s-Mini-Project')) {
            env.PROJECT_DIR = 'K8s-Mini-Project'
          } else {
            env.PROJECT_DIR = '.'
          }
        }
      }
    }

    stage('Prepare reset namespace (optional)') {
      when {
        expression {
          return params.RESET_MINIPROJECT
        }
      }
      steps {
        script {
          sh '''
            set -eu
            cd ${PROJECT_DIR}

            echo "[reset] removing helm releases in namespace: ${NAMESPACE}"
            for release in ${HELM_INFRA_RELEASE} ${HELM_COMMAND_RELEASE} ${HELM_QUERY_RELEASE} ${HELM_FRONTEND_RELEASE}; do
              helm uninstall "${release}" -n ${NAMESPACE} --ignore-not-found=true || true
            done

            kubectl delete namespace ${NAMESPACE} --ignore-not-found=true --wait=true || true
            echo "[reset] namespace ${NAMESPACE} cleanup done"
          '''
        }
      }
    }

    stage('Set image tag') {
      steps {
        script {
          def shortSha = sh(script: "git rev-parse --short=7 HEAD", returnStdout: true).trim()
          env.IMAGE_TAG = "${env.BUILD_NUMBER}-${shortSha}"
        }
      }
    }

    stage('SonarQube Analysis') {
      steps {
        withSonarQubeEnv('sonarqube') {
          sh '''
            set -eu
            cd ${PROJECT_DIR}

            sonar-scanner --version

            for SERVICE_DIR in src-command/ingest src-command/consumer src-query/api src-query/consumer frontend; do
              echo "=== Analyzing: ${SERVICE_DIR} ==="
              sonar-scanner \
                -Dproject.settings=${WORKSPACE}/${SERVICE_DIR}/sonar-project.properties \
                -Dsonar.projectBaseDir=${WORKSPACE}/${SERVICE_DIR} \
                -Dsonar.host.url=${SONAR_HOST_URL} \
                -Dsonar.token=${SONAR_AUTH_TOKEN}
            done
          '''
        }
      }
    }

    stage('Quality Gate') {
      steps {
        timeout(time: 5, unit: 'MINUTES') {
          waitForQualityGate abortPipeline: false
        }
      }
    }

    stage('Docker Build & Push') {
      steps {
        withCredentials([usernamePassword(
          credentialsId: env.HARBOR_CREDS,
          usernameVariable: 'HARBOR_USER',
          passwordVariable: 'HARBOR_PASS'
        )]) {
          sh '''
            set -eu

            echo "${HARBOR_PASS}" | docker login ${HARBOR_REGISTRY} -u "${HARBOR_USER}" --password-stdin
            cd ${PROJECT_DIR}

            docker build -t ${HARBOR_REGISTRY}/${HARBOR_PROJECT}/telemetry-ingest-api:${IMAGE_TAG} ./src-command/ingest
            docker push ${HARBOR_REGISTRY}/${HARBOR_PROJECT}/telemetry-ingest-api:${IMAGE_TAG}

            docker build -t ${HARBOR_REGISTRY}/${HARBOR_PROJECT}/telemetry-mongo-consumer:${IMAGE_TAG} ./src-command/consumer
            docker push ${HARBOR_REGISTRY}/${HARBOR_PROJECT}/telemetry-mongo-consumer:${IMAGE_TAG}

            docker build -t ${HARBOR_REGISTRY}/${HARBOR_PROJECT}/mobility-query-api:${IMAGE_TAG} ./src-query/api
            docker push ${HARBOR_REGISTRY}/${HARBOR_PROJECT}/mobility-query-api:${IMAGE_TAG}

            docker build -t ${HARBOR_REGISTRY}/${HARBOR_PROJECT}/mobility-query-consumer:${IMAGE_TAG} ./src-query/consumer
            docker push ${HARBOR_REGISTRY}/${HARBOR_PROJECT}/mobility-query-consumer:${IMAGE_TAG}

            docker build -t ${HARBOR_REGISTRY}/${HARBOR_PROJECT}/k8s-mini-frontend:${IMAGE_TAG} ./frontend
            docker push ${HARBOR_REGISTRY}/${HARBOR_PROJECT}/k8s-mini-frontend:${IMAGE_TAG}
          '''
        }
      }
    }

    stage('Prepare Registry Auth') {
      steps {
        withCredentials([usernamePassword(
          credentialsId: env.HARBOR_CREDS,
          usernameVariable: 'HARBOR_USER',
          passwordVariable: 'HARBOR_PASS'
        )]) {
          sh '''
            set -eu
            cd ${PROJECT_DIR}
            kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

            kubectl create secret docker-registry harbor-secret \
              --namespace ${NAMESPACE} \
              --docker-server=${HARBOR_REGISTRY} \
              --docker-username="${HARBOR_USER}" \
              --docker-password="${HARBOR_PASS}" \
              --docker-email="${HARBOR_EMAIL}" \
              --dry-run=client -o yaml \
              | kubectl apply -f -

            kubectl patch serviceaccount default -n ${NAMESPACE} \
              -p '{"imagePullSecrets":[{"name":"harbor-secret"}]}'
          '''
        }
      }
    }

    stage('Reset NFS stateful volumes') {
      steps {
        sh '''
          set -eu
          cd ${PROJECT_DIR}

          for pvc in ${NFS_PVC_LIST}; do
            if kubectl get pvc "${pvc}" -n ${NAMESPACE} >/dev/null 2>&1; then
              pvc_phase=$(kubectl get pvc "${pvc}" -n ${NAMESPACE} -o jsonpath='{.status.phase}')
              pvc_prov=$(kubectl get pvc "${pvc}" -n ${NAMESPACE} -o jsonpath='{.metadata.annotations.volume\\.kubernetes\\.io/storage-provisioner}' 2>/dev/null || true)
              pvc_prov_beta=$(kubectl get pvc "${pvc}" -n ${NAMESPACE} -o jsonpath='{.metadata.annotations.volume\\.beta\\.kubernetes\\.io/storage-provisioner}' 2>/dev/null || true)

              if [ "${pvc_phase}" != "Bound" ] || \
                 [ "${pvc_prov}" = "${LEGACY_NFS_PROVISIONER}" ] || \
                 [ "${pvc_prov_beta}" = "${LEGACY_NFS_PROVISIONER}" ]; then
                echo "[storage] deleting pvc ${pvc} (phase=${pvc_phase}, provisioner=${pvc_prov}${pvc_prov_beta:+/${pvc_prov_beta}})"
                kubectl delete pvc "${pvc}" -n ${NAMESPACE} --ignore-not-found=true
              else
                echo "[storage] keep pvc ${pvc} (phase=${pvc_phase}, provisioner=${pvc_prov}${pvc_prov_beta:+/${pvc_prov_beta}})"
              fi
            fi
          done

          for pv in ${NFS_PV_LIST}; do
            if kubectl get pv "${pv}" >/dev/null 2>&1; then
              pv_phase=$(kubectl get pv "${pv}" -o jsonpath='{.status.phase}')
              pv_claim=$(kubectl get pv "${pv}" -o jsonpath='{.spec.claimRef.name}' 2>/dev/null || true)
              pv_claim_ns=$(kubectl get pv "${pv}" -o jsonpath='{.spec.claimRef.namespace}' 2>/dev/null || true)
              if [ "${pv_phase}" = "Released" ]; then
                echo "[storage] deleting released pv ${pv} (claim=${pv_claim_ns}/${pv_claim})"
                kubectl delete pv "${pv}" --ignore-not-found=true
              elif [ "${pv_phase}" = "Bound" ] && [ "${pv_claim}" != "" ] && \
                   ! kubectl get pvc "${pv_claim}" -n "${pv_claim_ns}" >/dev/null 2>&1; then
                echo "[storage] deleting orphan bound pv ${pv} (missing claim=${pv_claim_ns}/${pv_claim})"
                kubectl delete pv "${pv}" --ignore-not-found=true
              else
                echo "[storage] keep pv ${pv} (phase=${pv_phase}, claim=${pv_claim_ns}/${pv_claim})"
              fi
            fi
          done
        '''
      }
    }

    stage('Remove legacy NFS provisioner') {
      steps {
        sh '''
          set -eu
          cd ${PROJECT_DIR}

          for ns in ${NFS_NAMESPACE} default; do
            kubectl delete deployment -n "${ns}" nfs-subdir-external-provisioner --ignore-not-found=true || true
            kubectl delete statefulset -n "${ns}" nfs-subdir-external-provisioner --ignore-not-found=true || true
            kubectl delete daemonset -n "${ns}" nfs-subdir-external-provisioner --ignore-not-found=true || true
            kubectl delete role,rolebinding,serviceaccount -n "${ns}" nfs-provisioner-nfs-subdir-external-provisioner --ignore-not-found=true || true
          done

          if command -v helm >/dev/null 2>&1; then
            helm uninstall ${NFS_RELEASE} -n ${NFS_NAMESPACE} --ignore-not-found=true || true
          fi

          if kubectl get storageclass nfs-storage >/dev/null 2>&1; then
            kubectl annotate storageclass nfs-storage "storageclass.kubernetes.io/is-default-class-" --overwrite || true
            kubectl annotate storageclass nfs-storage "storageclass.kubernetes.io/is-default-class=true" --overwrite || true
          fi
        '''
      }
    }

  stage('Deploy NFS Infra') {
      steps {
        sh '''
          set -eu
          cd ${PROJECT_DIR}
          SC_EXISTS=0

          if [ -f "${NFS_STORAGE_MANIFEST}" ]; then
            echo "[nfs] apply nfs-storage manifest"
            NFS_PROVISIONER=$(awk '/^provisioner:/{print $2}' "${NFS_STORAGE_MANIFEST}" | head -n 1)
            NFS_RECLAIM=$(awk '/^reclaimPolicy:/{print $2}' "${NFS_STORAGE_MANIFEST}" | head -n 1)
            NFS_BINDMODE=$(awk '/^volumeBindingMode:/{print $2}' "${NFS_STORAGE_MANIFEST}" | head -n 1)

            if kubectl get sc nfs-storage >/dev/null 2>&1; then
              CURRENT_PROVISIONER=$(kubectl get sc nfs-storage -o jsonpath='{.provisioner}')
              CURRENT_RECLAIM=$(kubectl get sc nfs-storage -o jsonpath='{.reclaimPolicy}')
              CURRENT_BINDMODE=$(kubectl get sc nfs-storage -o jsonpath='{.volumeBindingMode}')
              if [ "${CURRENT_PROVISIONER}" != "${NFS_PROVISIONER}" ] || \
                 [ "${CURRENT_RECLAIM}" != "${NFS_RECLAIM}" ] || \
                 [ "${CURRENT_BINDMODE}" != "${NFS_BINDMODE}" ]; then
                echo "[nfs] existing nfs-storage differs from manifest, recreate"
                kubectl delete storageclass nfs-storage --ignore-not-found=true
              else
                echo "[nfs] existing nfs-storage matches manifest"
              fi
            fi

            kubectl apply -f "${NFS_STORAGE_MANIFEST}"
            echo "[nfs] waiting for storageclass nfs-storage"
            for i in $(seq 1 30); do
              if kubectl get sc nfs-storage >/dev/null 2>&1; then
                kubectl get sc nfs-storage
                SC_EXISTS=1
                break
              fi
              sleep 2
              if [ "$i" = "30" ]; then
                echo "[nfs] storageclass nfs-storage not found"
                exit 1
              fi
            done
          elif [ -d "${NFS_CHART_PATH}" ]; then
            echo "[nfs] installing/updating nfs subdir provisioner"

            NFS_VALUES=""
            if [ -f "${NFS_CHART_PATH}/values.yaml" ]; then
              NFS_VALUES="-f ${NFS_CHART_PATH}/values.yaml"
            fi

            helm upgrade --install ${NFS_RELEASE} ${NFS_CHART_PATH} \
              -n ${NFS_NAMESPACE} --create-namespace \
              --wait --timeout ${HELM_TIMEOUT} --atomic --cleanup-on-fail \
              ${NFS_VALUES}

            kubectl rollout status deployment/nfs-subdir-external-provisioner \
              -n ${NFS_NAMESPACE} --timeout=120s

            kubectl get sc nfs-storage
            POD_NAME=$(kubectl get pod -n ${NFS_NAMESPACE} \
              -l app.kubernetes.io/name=nfs-subdir-external-provisioner \
              -o jsonpath='{.items[0].metadata.name}')
            if [ -z "${POD_NAME}" ]; then
              POD_NAME=$(kubectl get pod -n ${NFS_NAMESPACE} \
                -l app=nfs-subdir-external-provisioner \
                -o jsonpath='{.items[0].metadata.name}')
            fi
            if [ -z "${POD_NAME}" ]; then
              echo "[nfs] provisioner pod not found"
              exit 1
            fi

            kubectl -n ${NFS_NAMESPACE} exec "${POD_NAME}" -- sh -c "touch /persistentvolumes/.probe && rm -f /persistentvolumes/.probe"
            echo "[nfs] probe write check passed"
            echo "[nfs] waiting for storageclass nfs-storage"
            for i in $(seq 1 30); do
              if kubectl get sc nfs-storage >/dev/null 2>&1; then
                kubectl get sc nfs-storage
                SC_EXISTS=1
                break
              fi
              sleep 2
              if [ "$i" = "30" ]; then
                echo "[nfs] storageclass nfs-storage not found"
                exit 1
              fi
            done
          else
            echo "[nfs] chart path not found: ${NFS_CHART_PATH}. fallback to existing default storageclass."
            if kubectl get sc nfs-storage >/dev/null 2>&1; then
              SC_EXISTS=1
            fi
          fi

          if [ "${SC_EXISTS}" = "0" ]; then
            if kubectl get sc -l storageclass.kubernetes.io/is-default-class=true -o name >/dev/null 2>&1; then
              sc_name=$(kubectl get sc -l storageclass.kubernetes.io/is-default-class=true -o jsonpath='{.items[0].metadata.name}')
            else
              sc_name=$(kubectl get sc -o jsonpath='{.items[0].metadata.name}')
            fi
            if [ -n "${sc_name}" ]; then
              echo "[nfs] nfs-storage not found. fallback storageclass=${sc_name}"
            else
              echo "[nfs] no storageclass found. cannot provision PersistentVolumeClaims."
              exit 1
            fi
          fi
        '''
      }
    }

    stage('Deploy to Kubernetes') {
      steps {
        withCredentials([file(credentialsId: 'mobility-secret-values', variable: 'SECRET_VALUES_FILE')]) {
          sh '''
            set -eu
            cd ${PROJECT_DIR}
            SC_VALUE=$(kubectl get sc nfs-storage -o jsonpath='{.metadata.name}' 2>/dev/null || true)
            if [ -z "${SC_VALUE}" ]; then
              if kubectl get sc -l storageclass.kubernetes.io/is-default-class=true -o jsonpath='{.items[0].metadata.name}' >/dev/null 2>&1; then
                SC_VALUE=$(kubectl get sc -l storageclass.kubernetes.io/is-default-class=true -o jsonpath='{.items[0].metadata.name}')
              else
                SC_VALUE=$(kubectl get sc -o jsonpath='{.items[0].metadata.name}')
              fi
            fi
            if [ -z "${SC_VALUE}" ]; then
              echo "[infra] no storageclass available"
              exit 1
            fi
            echo "[infra] using storageClass=${SC_VALUE}"

            helm upgrade --install ${HELM_INFRA_RELEASE} ${HELM_INFRA_CHART_PATH} \
              -n ${NAMESPACE} --create-namespace \
              --wait --timeout ${HELM_TIMEOUT} --atomic --cleanup-on-fail \
              -f ${SECRET_VALUES_FILE} \
              --set persistence.storageClass=${SC_VALUE}

            helm upgrade --install ${HELM_COMMAND_RELEASE} ${HELM_COMMAND_CHART_PATH} \
              -n ${NAMESPACE} --create-namespace \
              --wait --timeout ${HELM_TIMEOUT} --atomic --cleanup-on-fail \
              -f ${SECRET_VALUES_FILE} \
              --set command.api.image=${HARBOR_REGISTRY}/${HARBOR_PROJECT}/telemetry-ingest-api:${IMAGE_TAG} \
              --set command.consumer.image=${HARBOR_REGISTRY}/${HARBOR_PROJECT}/telemetry-mongo-consumer:${IMAGE_TAG}

            helm upgrade --install ${HELM_QUERY_RELEASE} ${HELM_QUERY_CHART_PATH} \
              -n ${NAMESPACE} --create-namespace \
              --wait --timeout ${HELM_TIMEOUT} --atomic --cleanup-on-fail \
              -f ${SECRET_VALUES_FILE} \
              --set query.api.image=${HARBOR_REGISTRY}/${HARBOR_PROJECT}/mobility-query-api:${IMAGE_TAG} \
              --set query.consumer.image=${HARBOR_REGISTRY}/${HARBOR_PROJECT}/mobility-query-consumer:${IMAGE_TAG}

            helm upgrade --install ${HELM_FRONTEND_RELEASE} ${HELM_FRONTEND_CHART_PATH} \
              -n ${NAMESPACE} --create-namespace \
              --wait --timeout ${HELM_TIMEOUT} --atomic --cleanup-on-fail \
              -f ${SECRET_VALUES_FILE} \
              --set frontend.image=${HARBOR_REGISTRY}/${HARBOR_PROJECT}/k8s-mini-frontend:${IMAGE_TAG}
          '''
        }
      }
    }

    stage('Apply Monitoring Resources') {
      steps {
        sh '''
          set -eu
          cd ${PROJECT_DIR}

          if ! kubectl get namespace "${MONITORING_NAMESPACE}" >/dev/null 2>&1; then
            echo "[monitoring] namespace ${MONITORING_NAMESPACE} not found, skip monitoring apply"
            exit 0
          fi

          for crd in \
            servicemonitors.monitoring.coreos.com \
            podmonitors.monitoring.coreos.com \
            prometheusrules.monitoring.coreos.com
          do
            if ! kubectl get crd "${crd}" >/dev/null 2>&1; then
              echo "[monitoring] CRD ${crd} not found, skip monitoring apply"
              exit 0
            fi
          done

          kubectl apply -f ${MONITORING_DIR}/miniproject-servicemonitor.yaml
          kubectl apply -f ${MONITORING_DIR}/miniproject-podmonitor.yaml
          kubectl apply -f ${MONITORING_DIR}/mobility-alert-rules.yaml
          kubectl apply -f ${MONITORING_DIR}/mobility-monitoring-ingress.yaml

          bash ${MONITORING_DIR}/apply-grafana-dashboards.sh ${MONITORING_NAMESPACE}
        '''
      }
    }

    stage('Verify Workload Readiness') {
      steps {
        sh '''
          set -eu
          cd ${PROJECT_DIR}
          kubectl get pvc -n ${NAMESPACE}
          kubectl get pod -n ${NAMESPACE}

          PENDING_PVC=$(kubectl get pvc -n ${NAMESPACE} --no-headers 2>/dev/null | awk '$2 != "Bound" {count++} END {print count+0}')
          if [ "${PENDING_PVC}" -gt 0 ]; then
            echo "[verify] warning: some PVCs are not Bound (count=${PENDING_PVC})"
          fi

          if kubectl get pod -n ${NAMESPACE} --field-selector=status.phase=Pending --no-headers 2>/dev/null | grep -q .; then
            echo "[verify] warning: pending pods exist"
            kubectl get pod -n ${NAMESPACE} --field-selector=status.phase=Pending --no-headers
          fi
        '''
      }
    }
  }

  post {
    success {
      echo "Jenkins Deploy Success: ${env.JOB_NAME} #${env.BUILD_NUMBER} (${env.IMAGE_TAG})"
    }
    failure {
      echo "Jenkins Deploy Failed: ${env.JOB_NAME} #${env.BUILD_NUMBER} (${env.IMAGE_TAG})"
    }
    always {
      sh "docker logout ${HARBOR_REGISTRY} || true"
    }
  }
}
