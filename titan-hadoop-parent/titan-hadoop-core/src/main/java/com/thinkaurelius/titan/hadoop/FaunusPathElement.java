package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class FaunusPathElement extends FaunusElement implements WritableComparable<FaunusElement>, Configurable {

    private static final Logger log =
            LoggerFactory.getLogger(FaunusPathElement.class);

    public static final Configuration EMPTY_CONFIG = EmptyConfiguration.immutable();
    private static final Tracker DEFAULT_TRACK = new Tracker();

    protected Tracker tracker = DEFAULT_TRACK;
    protected long pathCounter = 0;
    protected Configuration configuration = EMPTY_CONFIG;

    public FaunusPathElement(final long id) {
        super(id);
    }

    public FaunusPathElement(final Configuration configuration, final long id) {
        super(id);
        this.setConf(configuration);
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
        boolean trackPaths = (configuration.getBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, false));
        if (trackPaths) this.tracker = new Tracker((this instanceof HadoopVertex) ? new HadoopVertex.MicroVertex(this.id) : new StandardFaunusEdge.MicroEdge(this.id));
        log.debug("Set trackPaths=" + this.tracker.trackPaths + " from config option " + Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS);
    }

    public Configuration getConf() {
        return this.configuration;
    }

    @Override
    protected FaunusTypeManager getTypeManager() {
        return FaunusTypeManager.getTypeManager(configuration);
    }

    //##################################
    // Path Handling
    //##################################

    protected static class Tracker {

        private final boolean trackPaths;
        private final List<List<MicroElement>> paths;
        private final MicroElement microVersion;

        public Tracker(MicroElement microVersion) {
            this.trackPaths = true;
            this.microVersion = microVersion;
            this.paths = new ArrayList<List<MicroElement>>();
        }

        private Tracker() {
            this.trackPaths = false;
            this.paths = null;
            this.microVersion = null;
        }

    }

    private boolean hasTrackPaths() {
        return tracker.trackPaths;
    }

    private void checkPathsEnabled() {
        Preconditions.checkState(hasTrackPaths(),"Path calculations are not enabled");
    }

    public void addPath(final List<MicroElement> path, final boolean append) throws IllegalStateException {
        checkPathsEnabled();
        if (append) path.add(tracker.microVersion);
        tracker.paths.add(path);
    }

    public void addPaths(final List<List<MicroElement>> paths, final boolean append) throws IllegalStateException {
        checkPathsEnabled();
        if (append) {
            for (final List<MicroElement> path : paths) {
                addPath(path, append);
            }
        } else
            tracker.paths.addAll(paths);
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
