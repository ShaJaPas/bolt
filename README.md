Bolt UDP
==============

**TODO**
- [X] Reliable/Unreliable packets
- [X] In-order and out-of-order delivery
- [ ] Consider 16/32 bit packet checksum (UDP checksum is only 16 bit - 65536 values - not safe enough)
- [X] Message assembly/disassembly
- [X] Delivery strategy
- [ ] Retransmit strategy (aggressive, on NACK)
- [X] Trim control and data packets
- [X] Simulated packet-loss
- [ ] Simulated latency
- [ ] Simulated jitter
- [ ] Multi-client
- [X] Distinguish unit/integration tests and use both surefire/failsafe
- [ ] IPv6 support
- [ ] Client disconnect (remove sessions, generate event)
- [ ] Client disconnect/reconnect (bad network conditions)
- [ ] Performance testing high-volume of single packets
- [ ] Performance testing, high-volume of large messages
- [ ] General performance testing
- [ ] Validate thread cleanup
- [ ] Improve statistics legibility
- [ ] Improve performance of sender onAcknowledgement
- [X] Use log4j with performant string interpolation
- [ ] Improve performance in high-packet loss scenarios
- [ ] Cleanup core classes (session, endpoint, sender, receiver, congestion-control)
- [ ] Review level of each log statement
- [ ] Review Javadoc
- [ ] Test keepalives and session expiry
- [X] Change Test classes names from (Test.*.java) to (.*Test.java)
- [X] Add group to base of package (before bolt.*)
- [ ] Separate to core and api packages
- [ ] Reduce 1+ second startup of local client/server
- [ ] Consider making a sequence number class to encapsulate overflow, comparison, etc logic
- [ ] Open-source the library
- [ ] Configurable timeout on no response