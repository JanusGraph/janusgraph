// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.JanusGraphException;

/**
 * This exception is thrown if a resource is being accessed that is unavailable.
 * The resource can be an external storage system, indexing system or other component.
 * <p>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ResourceUnavailableException extends JanusGraphException {

    private static final long serialVersionUID = 482890657293484420L;

    /**
     * @param msg Exception message
     */
    public ResourceUnavailableException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public ResourceUnavailableException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public ResourceUnavailableException(Throwable cause) {
        this("Attempting to access unavailable resource", cause);
    }

    public static void verifyOpen(boolean isOpen, String resourceName, String... resourceIdentifiers) {
        Preconditions.checkArgument(StringUtils.isNotBlank(resourceName));
        if (!isOpen) {
            StringBuilder msg = new StringBuilder();
            msg.append(resourceName).append(" ");
            if (resourceIdentifiers!=null && resourceIdentifiers.length>0) {
                msg.append("[");
                for (int i = 0; i < resourceIdentifiers.length; i++) {
                    if (i>0) msg.append(",");
                    msg.append(resourceIdentifiers[i]);
                }
                msg.append("] ");
            }
            msg.append("has been closed");
            throw new ResourceUnavailableException(msg.toString());
        }
    }


}
