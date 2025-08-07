@Library('ni-utils') _

//env
env['jenkins_build_number'] = "${env.BRANCH_NAME}-${env.BUILD_NUMBER}"

//service name is extrapolated from repository name check
def repoName = currentBuild.rawBuild.project.parent.displayName

// Define pod
def pod = libraryResource 'com/naturalint/python-agent.yaml'

def template_vars = [
    'python_image': 'naturalintelligence/python:3.12.0',
    'build_label': repoName,
    'jenkins_build_number': env['jenkins_build_number']
]
pod = renderTemplate(pod, template_vars)
print pod

// Define sharedLibrary
def sharedLibrary = new com.naturalint.pythonMultiSvc()

// Set slack channel
def slackChannel = "python-web-services"
def project = "dataapps"
def services = [
    'mcp-apache-spark-history-server': [
        agentImage: 'python:3.12.0',
        buildFromRoot: true
    ]
]

def packages = [
//     'cli': [
//         agentImage: 'python:3.12.0'
//     ]
]
def buildAll = true

// Custom unit test command
// def unitTestCommand="python -m pytest tests/ -v"
// def intTestCommand="uv run pytest -v --cov=src --cov-report=term-missing --cov-report=xml:coverage.xml"

// Args for pipeline
def initiateData = [project: project, agentImage: template_vars['python_image'],  services: services, packages: packages, triggerStaticJob: "Operations_Manual_Service_Deployment"]
def compileData = [run: false, artifacts: ["DockerHub", "ECR"]]
def testData = [run: false]
def artifactData = [run: true]
def intTestData = [run: false]
def deploymentData = [run: false]
def buildCommands = [
    initiateData: initiateData,
    compileData: compileData,
    testData: testData,
    artifactData: artifactData,
    intTestData: intTestData,
    deploymentData: deploymentData
]

// Load properties
new com.naturalint.properties.data.pythonSvc().pythonMultiSvc([services: services.keySet().toList(), buildAll: buildAll])

timestamps {
    commonPipelineMultiPod(sharedLibrary, repoName, buildCommands, pod, slackChannel)
}
