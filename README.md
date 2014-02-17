akka-memcached
==============

This is not a wrapper around any existing memcached library, but a pure implementation of a memcached client in [http://scala-lang.org](Scala) with [http://akka.io](Akka).  It is intended to be fully asynchronous and non-blocking, using the existing Akka infrastructure and threading system, since I dislike competing for thread resources with other libraries, and most of my work is done with Akka these days.  It is also intended to be fully written using the binary memcached protocol, rather than the text version.

When I started writing this, I had a serious need for such a client, but that need disappeared.  As such, I've had to put the project aside and behind other more pressing priorities.

If you can make use of it, or make it better, please fork it and make it awesome.
