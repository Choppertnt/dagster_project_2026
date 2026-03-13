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
        // 🎯 ใช้ Build Number ต่อท้ายเพื่อความเท่และไม่ซ้ำ
        IMAGE_TAG = "v1.0.${env.BUILD_NUMBER}"
        
        GH_CREDENTIALS_ID = "ghcr-auth" 
    }

    stages {
        stage('1. Build and Tag') {
            steps {
                sh "docker build -t ${IMAGE_NAME}:${IMAGE_TAG} ."
                sh "docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${IMAGE_NAME}:latest"
            }
        }

        stage('2. Push to GHCR') {
            steps {
                withCredentials([usernamePassword(credentialsId: env.GH_CREDENTIALS_ID, usernameVariable: 'USER', passwordVariable: 'PASS')]) {
                    sh "echo ${PASS} | docker login ${REGISTRY} -u ${USER} --password-stdin"
                    sh "docker push ${IMAGE_NAME}:${IMAGE_TAG}"
                    sh "docker push ${IMAGE_NAME}:latest"
                }
            }
        }

        stage('3. Deploy via Helm') {
            steps {
                script {
                    echo "Deploying version ${IMAGE_TAG} to K3s..."
                    // 🎯 ใช้ helm upgrade --set เพื่อความยั่งยืน
                    // หมายเหตุ: เช็คโครงสร้าง path ของ tag ใน values.yaml ของนายให้ดี
                    sh """
                    helm upgrade --install dagster-release dagster/dagster \
                      -n ${NAMESPACE} \
                      -f values.yaml \
                      --set dagster-user-deployments.deployments[0].image.tag=${IMAGE_TAG}
                    """
                    sh "kubectl rollout status deployment/${DEPLOYMENT_NAME} -n ${NAMESPACE}"
                }
            }
        }
    }

    post {
        success {
            echo "Successfully deployed ${IMAGE_TAG}!"
        }
        always {
            container('docker-cli') {
                echo "Cleaning up dangling images..."
                sh "docker image prune -f"
            }
        }
    }
}