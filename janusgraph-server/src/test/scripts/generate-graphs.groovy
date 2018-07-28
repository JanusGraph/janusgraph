// an init script that returns a Map allows explicit setting of global bindings.
import org.janusgraph.example.GraphOfTheGodsFactory;

def globals = [:]

globals << [hook : [
  onStartUp: { ctx ->
    GraphOfTheGodsFactory.loadWithoutMixedIndex(gods, true)
  }
] as LifeCycleHook]

globals << [gods_traversal : gods.traversal()]