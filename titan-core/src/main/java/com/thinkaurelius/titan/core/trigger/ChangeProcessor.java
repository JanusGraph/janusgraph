package com.thinkaurelius.titan.core.trigger;

import com.thinkaurelius.titan.core.TitanTransaction;

/**
 *
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ChangeProcessor {

    public void process(TitanTransaction tx, ChangeState changeState);

}
