pipeline {
    agent any

    environment {
        NAMESPACE = "thanathorn55551-dev"
        APP_NAME = "dagster-assets"
        DEPLOYMENT_NAME = "dagster-release-dagster-user-deployments-my-data-pipeline"
        
        // สร้าง Tag เวอร์ชันใหม่จากเลข Build รอบนั้นๆ
        IMAGE_TAG = "v1.0.${env.BUILD_NUMBER}"
    }

    stages {
        stage('1. Build Image on OpenShift') {
            steps {
                script {
                    echo "เริ่ม Build Image ด้วย Tag: ${IMAGE_TAG}..."
                    // ตอนนี้ Jenkins อยู่ในโฟลเดอร์โค้ดที่โหลดจาก Git แล้ว
                    // สั่งใช้ --from-dir=. เพื่อแพ็คไฟล์ทั้งหมดส่งให้ OpenShift Build
                    sh "oc start-build ${APP_NAME} --from-dir=. --follow -n ${NAMESPACE}"
                    
                    // แปะป้าย Tag ใหม่ให้ Image 
                    sh "oc tag ${APP_NAME}:latest ${APP_NAME}:${IMAGE_TAG} -n ${NAMESPACE}"
                }
            }
        }

        stage('2. Deploy (Rolling Update)') {
            steps {
                script {
                    echo "อัปเดต Dagster Deployment ไปใช้เวอร์ชัน ${IMAGE_TAG}..."
                    
                    // 🌟 สร้างที่อยู่เต็มๆ ของ Image ใน OpenShift
                    def FULL_IMAGE_URL = "image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/${APP_NAME}:${IMAGE_TAG}"
                    
                    // 🎯 จุดที่แก้: ระบุเป้าหมายเป็น dagster-user-deployments=... แบบเจาะจง!
                    sh """
                        oc set image deployment/${DEPLOYMENT_NAME} \
                        dagster-user-deployments=${FULL_IMAGE_URL} \
                        -n ${NAMESPACE}
                    """
                }
            }
        }
    }
}