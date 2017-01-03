package org.janusgraph.diskstorage.idmanagement;


/**
 * Represents ID allocation strategies for avoiding contention between
 * concurrent Janus instances using a shared storage backend. These strategies
 * are implemented in {@link ConsistentKeyIDAuthority}.
 */
public enum ConflictAvoidanceMode {

    /**
     * Disables ID allocation conflict avoidance. This setting does not
     * compromise correctness. However, in a clustered Janus deployment, this
     * may result in ID allocations frequently failing due to contention and
     * then retrying, slowing overall write throughput.
     */
    NONE,

    /**
     * <b>Expert feature: use with care.</b> The user manually assigns each
     * Janus instance a unique conflict avoidance tag in its local graph
     * configuration. The tag becomes part of globally-visible graph element IDs
     * allocated by that Janus instance. Each Janus instance assumes it has
     * exclusive control over its tag, and it uses datacenter-local-quorum-level
     * consistency (on storage backends that support this concept) when
     * allocating IDs with its tag.
     * <p>
     * This is useful for reducing write latency in Janus deployments atop a
     * multi-datacenter distributed store.
     * <p>
     * <b>When this mode is set, the user is responsible for setting a unique
     * tag in each Janus graph configuration. Setting the same tag on two
     * different Janus configurations can lead to silent graph corruption in
     * this mode! Each tag must be unique. If you're unsure about this or its
     * implications, then use one of the other modes instead.</b>
     */
    LOCAL_MANUAL,

    /**
     * The user assigns a tag to each Janus instance. The tags should be
     * globally unique for optimal performance, but duplicates will not
     * compromise correctness. The tag becomes part of globally-visible graph
     * element IDs allocated by the instance. If each instance has a unique tag,
     * then ID allocations will never conflict.
     * <p>
     * Unlike {@link #LOCAL_MANUAL}, setting the same tag on multiple instances
     * is safe in this mode. Janus uses global-quorum-level or greater on
     * storage backends that have a notion of eventual consistency, so Janus
     * will detect contention and avoid double allocation even when multiple
     * instances share a tag.
     */
    GLOBAL_MANUAL,

    /**
     * Janus randomly selects a tag from the space of all possible tags when
     * performing allocations. Like {@link #GLOBAL_MANUAL}, this uses at least
     * global-quorum-level consistency, so even if two instances happen to
     * select the same ID simultaneously, the conflict will still be detected.
     */
    GLOBAL_AUTO;

}
