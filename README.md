Bolt UDP
==============

![logo](logo.png)

**Proposed Work**
- [ ] Single Sender/Receiver threads
- [ ] Review all thread sleeps in production code
- [ ] Consider 16/32 bit packet checksum (UDP checksum is only 16 bit - not safe enough)
- [ ] Optimize bandwidth limit pipe polling
- [ ] Compatibility version
- [ ] Rendezvous mode
- [ ] Discoverable MTU
- [ ] Retransmit strategy (aggressive, on NAK)
- [ ] Reduce 1+ second startup of local client/server
- [ ] Consider making a sequence number class to encapsulate overflow, comparison, etc logic
- [ ] Configurable timeout on no response
- [ ] Review Javadoc
- [ ] Separate to core and api packages
- [ ] IPv6 support
- [ ] Performance testing high-volume of single packets
- [ ] Performance testing, high-volume of large messages
- [ ] General performance testing
- [ ] Validate thread cleanup
- [ ] Improve statistics legibility
- [ ] Improve performance of sender onAcknowledgement
- [ ] Improve performance in high-packet loss scenarios
- [X] Reliable/Unreliable packets
- [X] Open-source library
- [X] In-order and out-of-order delivery
- [X] Improve computed NAK seq numbers memory consumption
- [X] Improve handshake completion time
- [X] Improve receiver performance
- [X] Message assembly/disassembly
- [X] Delivery strategy
- [X] Trim control and data packets
- [X] Simulated packet-loss
- [X] Simulated latency
- [X] Simulated jitter
- [X] Simulated bandwidth
- [X] Handle duplicate ConnectionReady events
- [X] Multi-client
- [X] Distinguish unit/integration tests and use both surefire/failsafe
- [X] Client disconnect (remove sessions, generate event)
- [X] Client disconnect/reconnect (bad network conditions)
- [X] Review thread sleep times for efficiency
- [X] Use log4j with performant string interpolation
- [X] Cleanup core classes (session, endpoint, sender, receiver, congestion-control)
- [X] Review level of each log statement
- [X] Remove BoltPacket forSender filtering functionality.
- [X] Test keep-alives and session expiry
- [X] Change Test classes names from (Test.*.java) to (.*Test.java)
- [X] Add group to base of package (before bolt.*)
- [X] Statistical breakdown of received data by class id.