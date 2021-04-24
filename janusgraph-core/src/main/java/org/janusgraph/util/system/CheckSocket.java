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

package org.janusgraph.util.system;

import java.net.InetAddress;
import java.net.Socket;

/*
 *  This doesn't really belong here.  It's only used in the zipfile
 *  distribution to check whether Gremlin Server or ES are listening on
 *  their respective TCP ports.    But it's so tiny that I don't want
 *  to reorganize the repo to accommodate it (yet).
 *
 *  Many widely available *NIX programs do this task better (e.g.
 *  netcat, telnet, nmap, socat, ... we could even use netstat since
 *  we're interested only in the status of local ports).  But we want
 *  to keep the JanusGraph distribution self-contained insofar as is
 *  reasonable.
 */
public class CheckSocket {

    public static final int E_USAGE  = 1;
    public static final int E_FAILED = 2;
    public static final String MSG_USAGE =
        "Usage: " + CheckSocket.class.getSimpleName() + " hostname port";

    public static void main(String[] args) {
        if (2 != args.length) {
            System.err.println(MSG_USAGE);
            System.exit(E_USAGE);
        }
        try {
            Socket s = new Socket(
                InetAddress.getByName(args[0]),
                Integer.parseInt(args[1]));
            s.close();
            System.exit(0);
        } catch (Throwable t) {
            System.err.println(t);
            System.exit(E_FAILED);
        }
    }
}
