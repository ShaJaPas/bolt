Bolt UDP
==============

![logo](logo256.png)

Bolt is a UDP-based networking library which that offers support for:
 - Optional reliability.
 - Optional ordering.
 - Automatic serialization of large messages into packets.
 - Session management.
 - High level Client and multi-tenanted Server endpoints.
 - Reactive API for easy handling of both messages and connection state events.
 - Custom codec registration for message types.
 - Session statistics.
 - Application level simulation of packet loss, latency and jitter.

**Proposed Work**
- [ ] Publish to maven central
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