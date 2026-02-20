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
                    // สั่งเปลี่ยน Image ใน Deployment ให้ OpenShift สลับ Pod เก่า-ใหม่ แบบไร้รอยต่อ
                    sh """
                        oc set image deployment/${DEPLOYMENT_NAME} \
                        *=${APP_NAME}:${IMAGE_TAG} \
                        -n ${NAMESPACE}
                    """
                }
            }
        }
    }
}