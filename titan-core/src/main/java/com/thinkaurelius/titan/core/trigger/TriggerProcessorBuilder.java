package com.thinkaurelius.titan.core.trigger;

import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TriggerProcessorBuilder {

    public String getTriggerName();

    public TriggerProcessorBuilder setProcessorIdentifier(String name);

    public TriggerProcessorBuilder setStartTime(long time, TimeUnit unit);

    public TriggerProcessorBuilder setStartTimeNow();

    public TriggerProcessorBuilder addProcessor(ChangeProcessor processor);

}
