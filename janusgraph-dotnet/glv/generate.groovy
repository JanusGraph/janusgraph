def engine = new groovy.text.GStringTemplateEngine()
def csprojTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/JanusGraph.Net.Extensions.csproj.template")).make(["projectVersion":projectVersion, "tinkerpopVersion":tinkerpopVersion])
def csprojFile = new File("${projectBaseDir}/src/JanusGraph.Net.Extensions/JanusGraph.Net.Extensions.csproj")
csprojFile.newWriter().withWriter{ it << csprojTemplate }