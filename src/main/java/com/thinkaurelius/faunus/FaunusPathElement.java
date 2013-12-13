package com.thinkaurelius.faunus;

import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class FaunusPathElement extends FaunusElement implements WritableComparable<FaunusElement>, Configurable {

    protected List<List<MicroElement>> paths = null;
    protected MicroElement microVersion = null;
    protected boolean trackPaths = false;
    protected long pathCounter = 0;
    protected Configuration configuration = new EmptyConfiguration();

    public FaunusPathElement(final long id) {
        super(id);
    }

    @Override
    protected <T> T getImplicitProperty(final FaunusType type) {
        if (type.equals(FaunusType.COUNT))
            return (T) Long.valueOf(this.pathCount());
        else return super.getImplicitProperty(type);
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
        this.enablePath(configuration.getBoolean(Tokens.FAUNUS_PIPELINE_TRACK_PATHS, false));
    }

    public Configuration getConf() {
        return this.configuration;
    }

    //##################################
    // Path Handling
    //##################################

    private void enablePath(final boolean enablePath) {
        this.trackPaths = enablePath;
        if (this.trackPaths) {
            if (null == this.microVersion)
                this.microVersion = (this instanceof FaunusVertex) ? new FaunusVertex.MicroVertex(this.id) : new FaunusEdge.MicroEdge(this.id);
            if (null == this.paths)
                this.paths = new ArrayList<List<MicroElement>>();
        }
        // TODO: else make pathCounter = paths.size()?
    }

    public void addPath(final List<MicroElement> path, final boolean append) throws IllegalStateException {
        if (this.trackPaths) {
            if (append) path.add(this.microVersion);
            this.paths.add(path);
        } else {
            throw new IllegalStateException("Path calculations are not enabled");
        }
    }

    public void addPaths(final List<List<MicroElement>> paths, final boolean append) throws IllegalStateException {
        if (this.trackPaths) {
            if (append) {
                for (final List<MicroElement> path : paths) {
                    this.addPath(path, append);
                }
            } else
                this.paths.addAll(paths);
        } else {
            throw new IllegalStateException("Path calculations are not enabled");
        }
    }

    public List<List<MicroElement>> getPaths() throws IllegalStateException {
        if (this.trackPaths)
            return this.paths;
        else
            throw new IllegalStateException("Path calculations are not enabled");
    }

    public void getPaths(final FaunusElement element, final boolean append) {
        Preconditions.checkArgument(element instanceof FaunusPathElement);
        if (this.trackPaths) {
            this.addPaths(((FaunusPathElement) element).getPaths(), append);
        } else {
            this.pathCounter = this.pathCounter + ((FaunusPathElement) element).pathCount();
        }
    }

    public long incrPath(final long amount) throws IllegalStateException {
        if (this.trackPaths)
            throw new IllegalStateException("Path calculations are enabled -- use addPath()");
        else
            this.pathCounter = this.pathCounter + amount;
        return this.pathCounter;
    }

    public boolean hasPaths() {
        if (this.trackPaths)
            return !this.paths.isEmpty();
        else
            return this.pathCounter > 0;
    }

    public void clearPaths() {
        if (this.trackPaths) {
            this.paths = new ArrayList<List<MicroElement>>();
            this.microVersion = (this instanceof FaunusVertex) ? new FaunusVertex.MicroVertex(this.id) : new FaunusEdge.MicroEdge(this.id);
        } else
            this.pathCounter = 0;
    }

    public long pathCount() {
        if (this.trackPaths)
            return this.paths.size();
        else
            return this.pathCounter;
    }

    public void startPath() {
        if (this.trackPaths) {
            this.clearPaths();
            final List<MicroElement> startPath = new ArrayList<MicroElement>();
            startPath.add(this.microVersion);
            this.paths.add(startPath);
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
