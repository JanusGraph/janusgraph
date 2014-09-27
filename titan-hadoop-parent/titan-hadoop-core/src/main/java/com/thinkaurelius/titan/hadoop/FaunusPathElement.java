package com.thinkaurelius.titan.hadoop;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.PIPELINE_TRACK_PATHS;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for Faunus vertices and relations.
 *
 * This class must implement Hadoop's Configurable even if no code calls setConf or getConf directly.
 * The reason to implement Configurable is interoperability with Hadoop's
 * WritableSerialization.WritableDeserializer class.  The deserializer calls ReflectionUtils.newInstance, which in
 * turn calls {@code x instanceof Configurable} and then calls {@code setConf} when true.  IOW, implementing
 * Configurable is important because that's how path elements get their configuration after being reflectively
 * instantiated with the no-arg constructor.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class FaunusPathElement extends FaunusElement implements WritableComparable<FaunusElement>, Configurable {

    private static final Logger log =
            LoggerFactory.getLogger(FaunusPathElement.class);

    static final Tracker DEFAULT_TRACK = new Tracker();

    protected Tracker tracker;
    protected long pathCounter = 0;
    protected Configuration configuration;
    private org.apache.hadoop.conf.Configuration hadoopConf;

    public FaunusPathElement(final long id) {
        this(ModifiableHadoopConfiguration.immutableWithResources(), id);
    }

    public FaunusPathElement(final Configuration configuration, final long id) {
        super(id);
        this.configuration = configuration;
        if (this.configuration.get(PIPELINE_TRACK_PATHS)) {
            tracker = new Tracker((this instanceof FaunusVertex) ?
                    new FaunusVertex.MicroVertex(this.id) :
                    new StandardFaunusEdge.MicroEdge(this.id));
        } else {
            tracker = DEFAULT_TRACK;
        }
    }

    // Supports configuration after Hadoop deserializes instances of this class
    @Override
    public void setConf(org.apache.hadoop.conf.Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
        this.configuration = ModifiableHadoopConfiguration.of(hadoopConf);

        boolean trackPaths = this.configuration.get(PIPELINE_TRACK_PATHS);
        if (trackPaths) {
            this.tracker = new Tracker((this instanceof FaunusVertex) ?
                    new FaunusVertex.MicroVertex(this.id) :
                    new StandardFaunusEdge.MicroEdge(this.id));
        }
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConf() {
        return hadoopConf;
    }

    public Configuration getFaunusConf() {
        return configuration;
    }

    @Override
    public FaunusSchemaManager getTypeManager() {
        return FaunusSchemaManager.getTypeManager(configuration);
    }

    @Override
    public FaunusVertexQuery query() {
        return new FaunusVertexQuery(this);
    }

    //##################################
    // Path Handling
    //##################################

    protected static class Tracker {

        final boolean trackPaths;
        final List<List<MicroElement>> paths;
        final MicroElement microVersion;

        public Tracker(MicroElement microVersion) {
            this.trackPaths = true;
            this.microVersion = microVersion;
            this.paths = new ArrayList<List<MicroElement>>();
        }

        public Tracker(List<List<MicroElement>> paths, MicroElement microVersion) {
            this.trackPaths = true;
            this.microVersion = microVersion;
            this.paths = paths;
        }

        private Tracker() {
            this.trackPaths = false;
            this.paths = null;
            this.microVersion = null;
        }

    }

    public boolean hasTrackPaths() {
        return tracker.trackPaths;
    }

    private void checkPathsEnabled() {
        Preconditions.checkState(hasTrackPaths(),"Path calculations are not enabled");
    }

    public void addPath(final List<MicroElement> path, final boolean append) throws IllegalStateException {
        checkPathsEnabled();
        if (append) path.add(tracker.microVersion);
        tracker.paths.add(path);
        log.trace("Added path {} to {} (append={})", path, this, append);
    }

    public void addPaths(final List<List<MicroElement>> paths, final boolean append) throws IllegalStateException {
        checkPathsEnabled();
        if (append) {
            for (final List<MicroElement> path : paths) {
                addPath(path, append);
            }
        } else {
            tracker.paths.addAll(paths);
            log.trace("Adding all paths in list {} to {} (append={})", paths, this, append);
        }
    }

    public List<List<MicroElement>> getPaths() throws IllegalStateException {
        checkPathsEnabled();
        return tracker.paths;
    }

    public void getPaths(final FaunusElement element, final boolean append) {
        Preconditions.checkArgument(element instanceof FaunusPathElement);
        if (hasTrackPaths()) {
            addPaths(((FaunusPathElement) element).getPaths(), append);
        } else {
            pathCounter = pathCounter + ((FaunusPathElement) element).pathCount();
        }
    }

    public long incrPath(final long amount) throws IllegalStateException {
        if (hasTrackPaths())
            throw new IllegalStateException("Path calculations are enabled -- use addPath()");
        else
            pathCounter = pathCounter + amount;
        return pathCounter;
    }

    public boolean hasPaths() {
        if (hasTrackPaths())
            return !tracker.paths.isEmpty();
        else
            return pathCounter > 0;
    }

    public void clearPaths() {
        if (hasTrackPaths()) {
            tracker.paths.clear();
        } else
            this.pathCounter = 0;
        log.trace("Cleared paths on {}", this);
    }

    public long pathCount() {
        if (hasTrackPaths())
            return tracker.paths.size();
        else
            return this.pathCounter;
    }

    public void startPath() {
        if (hasTrackPaths()) {
            clearPaths();
            final List<MicroElement> startPath = new ArrayList<MicroElement>();
            startPath.add(tracker.microVersion);
            tracker.paths.add(startPath);
        } else {
            this.pathCounter = 1;
        }
    }


    public static abstract class MicroElement {

        protected final long id;

        public MicroElement(final long id) {
            this.id = id;
        }

        public long getId() {
            return this.id;
        }

        @Override
        public int hashCode() {
            return Long.valueOf(this.id).hashCode();
        }

        @Override
        public boolean equals(final Object object) {
            return (object.getClass().equals(this.getClass()) && this.id == ((MicroElement) object).getId());
        }
    }
}
