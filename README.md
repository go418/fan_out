# fan_out

Go struct for passing values from a generator to multiple consumers (each consumer receives each message).
Implemented as efficiently as possible (single buffer that contains only the messages not read by one of the consumers).

Allows to block the sender if no reader has read enough messages (similar to size of a channel).
Also allows to block the sender if a reader is too far behind.

Should be a good solution for event systems with independent event consumers.
