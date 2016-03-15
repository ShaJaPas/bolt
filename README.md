
**TODO**
- [ ] Reliable/Unreliable packets
- [X] Message assembly/disassembly
- [ ] Delivery strategy (aggressive, on NACK)
- [ ] Trim control and data packets
- [X] Simulated packet-loss
- [ ] Simulated latency
- [ ] Simulated jitter
- [ ] Client disconnect (remove sessions, generate event)
- [ ] Client disconnect/reconnect (bad network conditions)
- [ ] Performance testing high-volume of single packets
- [ ] Performance testing, high-volume of large messages
- [ ] General performance testing
- [X] Use log4j with performant string interpolation
- [ ] Improve performance in high-packet loss scenarios
- [ ] Cleanup core classes (session, endpoint, sender, receiver, congestion-control)
- [ ] Review level of each log statement
- [ ] Review Javadoc
- [ ] Add group to base of package (before bolt.*)
- [ ] Separate to core and api packages