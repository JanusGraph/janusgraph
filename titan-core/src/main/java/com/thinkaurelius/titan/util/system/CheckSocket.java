package com.thinkaurelius.titan.util.system;

import java.net.InetAddress;
import java.net.Socket;

/*
 *  This doesn't really belong here.  It's only used in the zipfile
 *  distribution to check whether Rexster or ES are listening on
 *  their respective TCP ports.    But it's so tiny that I don't want
 *  to reorganize the repo to accomodate it (yet).
 *
 *  Many widely available *NIX programs do this task better (e.g.
 *  netcat, telnet, nmap, socat, ... we could even use netstat since
 *  we're interested only in the status of local ports).  But we want
 *  to keep the Titan distribution self-contained insofar as is
 *  reasonable.
 */
public class CheckSocket {

    public static final int E_USAGE  = 1;
    public static final int E_FAILED = 2;
    public static final String MSG_USAGE =
        "Usage: " + CheckSocket.class.getSimpleName() + " hostname port";

    public static void main(String args[]) {
        if (2 != args.length) {
            System.err.println(MSG_USAGE);
            System.exit(E_USAGE);
        }
        try {
            Socket s = new Socket(
                InetAddress.getByName(args[0]),
                Integer.valueOf(args[1]).intValue());
            s.close();
            System.exit(0);
        } catch (Throwable t) {
            System.err.println(t.toString());
            System.exit(E_FAILED);
        }
    }
}
