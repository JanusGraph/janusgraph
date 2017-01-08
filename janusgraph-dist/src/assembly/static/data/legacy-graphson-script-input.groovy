/* legacy-graphson-script-input.groovy
 *
 * Can be used as a script file for ScriptInputFormat to read/load
 * GraphSON exports from older deployments that depended on older
 * versions of TinkerPop.
 */
def parse(line, factory) {
  def slurper = new JsonSlurper()
  def properties = slurper.parseText(line)
  def outE = properties.remove("_outE")
  def inE = properties.remove("_inE")
  def vid = properties.remove("_id")
  def vlabel = properties.remove("_label") ?: Vertex.DEFAULT_LABEL
  def vertex = factory.vertex(vid, vlabel)
  properties.each { def key, def value ->
    vertex.property(key, value)
  }
  if (outE != null) {
    outE.each { def e ->
      def eid = e.remove("_id")
      def elabel = e.remove("_label") ?: Edge.DEFAULT_LABEL
      def inV = factory.vertex(e.remove("_inV"))
      edge = factory.edge(vertex, inV, elabel)
      e.each { def key, def value ->
        edge.property(key, value)
      }
    }
  }
  if (inE != null) {
    inE.each { def e ->
      def eid = e.remove("_id")
      def elabel = e.remove("_label")
      def outV = factory.vertex(e.remove("_outV"))
      edge = factory.edge(outV, vertex, elabel)
      e.each { def key, def value ->
        edge.property(key, value)
      }
    }
  }
  return vertex
}
