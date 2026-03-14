pipeline {
    agent {
        kubernetes {
            yaml """
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: docker-cli
    image: docker:cli
    command: ['cat']
    tty: true
    volumeMounts:
    - name: docker-sock
      mountPath: /var/run/docker.sock
    resources:
      requests:
        cpu: "100m"
        memory: "256Mi"
      limits:
        cpu: "500m"
        memory: "1024Mi"
  - name: helm-kubectl
    image: dtzar/helm-kubectl:3.12.0
    command: ['cat']
    tty: true
  volumes:
  - name: docker-sock
    hostPath:
      path: /var/run/docker.sock
        """
        }
    }

    environment {
        NAMESPACE = "dagster" 
        // 🎯 แก้ชื่อ Deployment ให้ตรงตามที่ kubectl get deploy เห็น
        DEPLOYMENT_NAME = "dagster-release-dagster-user-deployments-my-data-pipeline" 
        
        REGISTRY = "ghcr.io"
        GH_USER = "Choppertnt"
        IMAGE_NAME = "ghcr.io/choppertnt/dagster-assets"
  
        IMAGE_TAG  = ""
        
        GH_CREDENTIALS_ID = "ghcr-auth" 
    }

        stages {
            stage('0. Setup Environment') {
            steps {
                script {

                    IMAGE_TAG = sh(script: "git rev-parse --short HEAD", returnStdout: true).trim()
                    echo "📦 Current Commit SHA: ${IMAGE_TAG}"
                }
            }
        stage('1. Build and Tag') {
            steps {
                container('docker-cli') {
                    sh "docker build -t ${IMAGE_NAME}:${IMAGE_TAG} ."
                    sh "docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${IMAGE_NAME}:latest"
                }
            }
        }

        stage('2. Push to GHCR') {
            steps {
                container('docker-cli') {
                    withCredentials([usernamePassword(credentialsId: env.GH_CREDENTIALS_ID, usernameVariable: 'USER', passwordVariable: 'PASS')]) {
                        sh "echo ${PASS} | docker login ${REGISTRY} -u ${USER} --password-stdin"
                        sh "docker push ${IMAGE_NAME}:${IMAGE_TAG}"
                        sh "docker push ${IMAGE_NAME}:latest"
                    }
                }
            }
        }

        stage('3. Deploy via Helm') {
            options {
                timeout(time: 5, unit: 'MINUTES') 
            }
            steps {
                container('helm-kubectl') {
                    script {
                        echo "🚀 Deploying version ${IMAGE_TAG} to K3s..."
                        sh """
                        helm repo add dagster https://dagster-io.github.io/helm
                        helm repo update
                        helm upgrade --install dagster-release dagster/dagster \
                          -n ${NAMESPACE} \
                          -f values.yaml \
                          --set dagster-user-deployments.deployments[0].image.tag=${IMAGE_TAG}
                        """
                        echo "⏳ Waiting for rollout to finish..."
                        sh "kubectl rollout status deployment/${DEPLOYMENT_NAME} -n ${NAMESPACE}"
                    }
                }
            }
            // 🎯 Post เฉพาะของ Stage 3 (Rollback เมื่อพัง)
            post {
                failure {
                    container('helm-kubectl') {
                        echo "❌ Deploy พัง! กำลัง Rollback..."
                        sh "kubectl rollout undo deployment/${DEPLOYMENT_NAME} -n ${NAMESPACE}"
                    }
                }
            }
        }
    } // จบ stages

    // 🎯 Post รวมของทั้ง Pipeline (Cleanup ขยะ)
    post {
        always {
            container('docker-cli') {
                echo "🧹 Cleaning up dangling images..."
                sh "docker image prune -f"
            }
        }
        success {
            echo "🎉 งานเสร็จสมบูรณ์! Version ${IMAGE_TAG} รันอยู่บน K3s แล้ว"
        }
    }
}