@Library('jenkins-joylib@v1.0.3') _

pipeline {

    agent {
        label joyCommonLabels(image_ver: '19.4.0')
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '10'))
        timestamps()
    }

    stages {
        stage('check') {
            steps{
                sh('make check')
            }
        }
        stage('format-check') {
            steps{
                sh('make fmtcheck')
            }
        }
        stage('test') {
            steps{
                sh('make test-unit')
            }
        }
    }
}
