pipeline {
    agent any

    environment {
        NAMESPACE = "thanathorn55551-dev"
        APP_NAME = "dagster-assets"
        DEPLOYMENT_NAME = "dagster-release-dagster-user-deployments-my-data-pipeline"
        
        IMAGE_TAG = "v1.0.${env.BUILD_NUMBER}"
    }

    stages {
        stage('1. Build Image on OpenShift') {
            steps {
                script {
                    echo "เริ่ม Build Image ด้วย Tag: ${IMAGE_TAG}..."
                    sh "oc start-build ${APP_NAME} --from-dir=. --follow -n ${NAMESPACE}"
                    sh "oc tag ${APP_NAME}:latest ${APP_NAME}:${IMAGE_TAG} -n ${NAMESPACE}"
                }
            }
        }

        stage('2. Deploy (Rolling Update)') {
            steps {
                script {
                    echo "อัปเดต Dagster Deployment ไปใช้เวอร์ชัน ${IMAGE_TAG}..."
                    
                    def FULL_IMAGE_URL = "image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/${APP_NAME}:${IMAGE_TAG}"
                    
                    // 🎯 จุดที่อัปเกรด: สั่งเปลี่ยนทั้ง Image และตัวแปร DAGSTER_CURRENT_IMAGE
                    sh """
                        # 1. เปลี่ยน Image ของ Container
                        oc set image deployment/${DEPLOYMENT_NAME} \
                        dagster-user-deployments=${FULL_IMAGE_URL} \
                        -n ${NAMESPACE}
                        
                        # 2. เปลี่ยนชื่อในตัวแปรที่หน้าเว็บ Dagster ใช้แสดงผล
                        oc set env deployment/${DEPLOYMENT_NAME} \
                        DAGSTER_CURRENT_IMAGE=${FULL_IMAGE_URL} \
                        -n ${NAMESPACE}
                    """
                }
            }
        }
    }
}