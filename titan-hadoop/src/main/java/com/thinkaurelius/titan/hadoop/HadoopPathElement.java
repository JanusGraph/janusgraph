package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class HadoopPathElement extends HadoopElement implements WritableComparable<HadoopElement>, Configurable {

    protected List<List<MicroElement>> paths = null;
    protected MicroElement microVersion = null;
    protected boolean trackPaths = false;
    protected long pathCounter = 0;
    protected Configuration configuration = EmptyConfiguration.immutable();

    public HadoopPathElement(final Configuration configuration, final long id) {
        super(id);
        this.setConf(configuration);
    }

    @Override
    protected <T> T getImplicitProperty(final HadoopType type) {
        if (type.equals(HadoopType.COUNT))
            return (T) Long.valueOf(this.pathCount());
        else return super.getImplicitProperty(type);
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
        this.trackPaths(configuration.getBoolean(Tokens.HADOOP_PIPELINE_TRACK_PATHS, false));
    }

    public Configuration getConf() {
        return this.configuration;
    }

    //##################################
    // Path Handling
    //##################################

    private void trackPaths(final boolean trackPaths) {
        this.trackPaths = trackPaths;
        if (this.trackPaths) {
            if (null == this.microVersion)
                this.microVersion = (this instanceof HadoopVertex) ? new HadoopVertex.MicroVertex(this.id) : new HadoopEdge.MicroEdge(this.id);
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

    public void getPaths(final HadoopElement element, final boolean append) {
        Preconditions.checkArgument(element instanceof HadoopPathElement);
        if (this.trackPaths) {
            this.addPaths(((HadoopPathElement) element).getPaths(), append);
        } else {
            this.pathCounter = this.pathCounter + ((HadoopPathElement) element).pathCount();
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
            this.microVersion = (this instanceof HadoopVertex) ? new HadoopVertex.MicroVertex(this.id) : new HadoopEdge.MicroEdge(this.id);
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
